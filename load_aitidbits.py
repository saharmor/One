from ast import Dict
import pandas as pd
import os
import pickle

from urllib.parse import urlparse


valid_domains = [
    'arxiv.org',
    'github.io',
    # 'ai.meta.com',
    # 'deepmind.com',
    # 'ai.googleblog.com'
]

def is_url(string):
    try:
        result = urlparse(string)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def is_research_paper_url(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return any(domain.endswith(valid_domain) for valid_domain in valid_domains)


def filter_research_paper_urls(url_dict):
    filtered_url_dict = {}
    for sheet_name, urls in url_dict.items():
        for url in urls:
            if not is_research_paper_url(url):
                print("Bad URL", url)

        research_paper_urls = [url for url in urls if is_research_paper_url(url)]
        if research_paper_urls:
            filtered_url_dict[sheet_name] = research_paper_urls
        else:
            print('No research paper URLs found in sheet "{}"'.format(sheet_name))
    return filtered_url_dict

def count_urls(url_dict):
    count = 0
    for urls in url_dict.values():
        count += len(urls)
    return count

def read_urls_from_excel(file_path):
    # Create an empty dictionary to store the URLs
    url_dict = {}

    # Load the Excel file
    xls = pd.ExcelFile(file_path)

    # Iterate through each sheet in the Excel file
    for sheet_name in xls.sheet_names:
        # Load the sheet into a pandas DataFrame
        df = xls.parse(sheet_name)

        # Find the column containing URLs
        url_column = None
        for col in df.columns:
            if 'url' in col.lower():
                url_column = col
                break

        if not url_column:
            continue

        # If a URL column was found, save the URLs to the dictionary
        urls = df[url_column].dropna().unique()
        valid_urls = [url for url in urls if is_url(url)]
        if valid_urls:
            url_dict[sheet_name] = valid_urls
        else:
            print('No valid URLs found in sheet "{}"'.format(sheet_name))

    return url_dict

def load_ai_tidbits() -> Dict:
    cache_file = 'ai_tidbits_cache.pkl'
    
    # Check if the cache file exists
    if os.path.exists(cache_file):
        # Load the data from the cache file
        with open(cache_file, 'rb') as f:
            filtered_url_dict = pickle.load(f)
        print('Loaded data from cache.')
    else:
        # Load and process the data from the Excel file
        file_path = 'AI Tidbits.xlsx'
        url_dict = read_urls_from_excel(file_path)
        filtered_url_dict = filter_research_paper_urls(url_dict)
        
        # Save the processed data to the cache file
        with open(cache_file, 'wb') as f:
            pickle.dump(filtered_url_dict, f)
        print('Saved data to cache.')
    
    url_count = count_urls(filtered_url_dict)
    print('Total number of URLs:', url_count)
    
    return filtered_url_dict
