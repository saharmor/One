from typing import Optional
from bs4 import BeautifulSoup

import requests
from db_utils import NO_CITATIONS_INDICATOR, connect_to_db, get_authors, get_paper_from_db, get_unprocessed_papers, insert_author_to_db, insert_paper_to_db, link_paper_author, update_failed_paper_citations_in_db, update_paper_citations_in_db

from load_aitidbits import load_ai_tidbits

import arxiv

from utils import extract_paper_id_from_url

class Paper:
    def __init__(self, citations_num: int, title: str, url: str, comment: str):
        self.citations_num = citations_num
        self.title = title
        self.url = url
        self.comment = comment
    
    def __str__(self):
        return f'{self.title} has {self.citations_num} citations and can be found at {self.url}'

class PaperAuthors:
    def __init__(self, paper: Paper, authors: list) -> None:
        self.paper = paper
        self.authors = authors

def fetch_paper_from_arxiv(paper_id):
    paper_id = paper_id.rstrip('.pdf')  # Remove '.pdf' suffix if it exists
    client = arxiv.Client()
    return next(client.results(arxiv.Search(id_list=[paper_id])))

def load_arxiv_data(paper_id: str):
    conn = connect_to_db()
    cursor = conn.cursor()

    # Check if paper data already exists in the database
    paper_row = get_paper_from_db(cursor, paper_id)
    if paper_row:
        # paper data exists
        conn.close()
        return

    # If paper data doesn't exist, fetch it from arXiv
    paper = fetch_paper_from_arxiv(paper_id)

    # Store paper data in the database
    insert_paper_to_db(cursor, paper, paper_id)

    # Store author data in the database and link with paper
    for author in paper.authors:
        author_id = insert_author_to_db(cursor, author.name)
        link_paper_author(cursor, paper_id, author_id)

    conn.commit()
    conn.close()

def get_paper_data_from_db(paper_id: str) -> Optional[PaperAuthors]:
    conn = connect_to_db()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM Papers WHERE paper_id = ?', (paper_id,))
    paper_row = cursor.fetchone()

    if paper_row:
        paper = Paper(paper_row[4], paper_row[2], paper_row[1], paper_row[3])  # citations, title, url, comment
        authors = get_authors(cursor, paper_id)
        paper_authors = PaperAuthors(paper, authors)

        conn.close()
        return paper_authors
    else:
        conn.close()
        return None  # Return None if paper not found

def get_paper_url_from_project_url(project_url: str) -> str:
    import requests
    from bs4 import BeautifulSoup
    import re

    # Send an HTTP request to the URL
    response = requests.get(project_url)
    
    arxiv_url = None

    # Check for a valid response
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Search for all links in the page
        for link in soup.find_all('a', href=True):
            # Check the descendants of each link for the text 'arXiv', case-insensitively
            if any(re.search('arxiv', str(descendant), re.IGNORECASE) for descendant in link.descendants):
                arxiv_url = link['href']
                break  # Exit the loop once the arXiv link is found
        
        if not arxiv_url:
            for link in soup.find_all('a', href=True):
                if any(re.search('Paper', str(descendant), re.IGNORECASE) for descendant in link.descendants):
                    arxiv_url = link['href']
                    break  # Exit the loop once the Paper link is found
        
        # Print the arXiv URL, or a message if it wasn't found
        if not arxiv_url:
            print(f'WARNING ***** No arXiv link found for URL {project_url}')
    else:
        print(f'Failed to retrieve the page: {response.status_code}')

    return arxiv_url
    
def get_paper_data(paper_url: str) -> Optional[PaperAuthors]:
    paper = None
    authors = []

    if 'github.io' in paper_url:
        paper_url = get_paper_url_from_project_url(paper_url)
        if not paper_url:
            return None

    if 'arxiv.org' not in paper_url.lower():
        print(f'WARNING ***** Not an arXiv URL, skipping {paper_url}')
        return None
    
    paper_id = extract_paper_id_from_url(paper_url)
    load_arxiv_data(paper_id)
    return get_paper_data_from_db(paper_id)

def print_authors_stats(authors, processed_papers_count):
    for author in authors:
        print(f"{author} has {len(authors[author])} papers")
    print(f"\n\nAuthors count {len(authors)} from {processed_papers_count} papers")

def update_paper_citations():
    conn = connect_to_db()
    cursor = conn.cursor()
    
    unprocessed_papers = get_unprocessed_papers(cursor)
    
    for paper_id in unprocessed_papers:
        try:
            citation_count = get_citations_for_each_paper(paper_id)
            update_paper_citations_in_db(cursor, paper_id, citation_count)
            conn.commit()
        except Exception as e:
            print(f"Failed to update citations for paper {paper_id}: {e}")
            update_failed_paper_citations_in_db(cursor, paper_id)

    conn.close()

def get_citations_for_each_paper(paper_id: str):
    url = f'https://scholar.google.com/scholar_lookup?arxiv_id={paper_id}'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f'Failed to retrieve page: {response.status_code}')
    
    soup = BeautifulSoup(response.text, 'html.parser')
    citation_element = soup.find('div', {'class': 'gs_ri'})
    
    if citation_element is None:
        raise Exception('Failed to find citation element')
    
    citation_text = citation_element.find('div', {'class': 'gs_fl'}).text
    citations = [int(s) for s in citation_text.split() if s.isdigit()]
    
    if not citations:
        return 0  # Assuming no citations if not found
    
    return citations[0]


def load_papers_data():
    papers = load_ai_tidbits()
    authors = {}

    paper_limit_debug_count = 0
    MAX_PAPER_DEBUG_COUNT = 999
    processed_papers_count = 0
    for tidbits_edition_papers in papers.values():
        for paper_url in tidbits_edition_papers:
            if paper_limit_debug_count == MAX_PAPER_DEBUG_COUNT:
                break

            paper_authors = get_paper_data(paper_url)
            if not paper_authors:
                continue

            # append this paper to each author in authors or create a new list with this paper only if author not in authors
            for author in paper_authors.authors:
                if author in authors:
                    authors[author].append(paper_url)
                else:
                    authors[author] = [paper_url]

            
            print(f"Finished processing {paper_url} with {len(paper_authors.authors)} authors")
            processed_papers_count += 1
            paper_limit_debug_count += 1
    
    return authors, processed_papers_count

# main
if __name__ == '__main__':
    # initialize_db()
    
    authors, processed_papers_count = load_papers_data()
    print('\n\n')
    print_authors_stats(authors, processed_papers_count)

    update_paper_citations()
    # print(get_citations_for_each_paper('2303.13375'))