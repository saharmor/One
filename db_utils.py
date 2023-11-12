import sqlite3

NO_CITATIONS_INDICATOR = -1
FAILED_FETCH_CITATIONS_INDICATOR = -2
DB_NAME = 'papers.db'

def initialize_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Create Papers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Papers (
            paper_id TEXT PRIMARY KEY,
            paper_url TEXT,
            title TEXT,
            comment TEXT,
            citations INTEGER
        )
    ''')

    # Create Authors table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Authors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fullname TEXT,
            affiliation TEXT
        )
    ''')

    # Create PaperAuthors table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PaperAuthors (
            paper_id TEXT,
            author_id INTEGER,
            FOREIGN KEY (paper_id) REFERENCES Papers(paper_id),
            FOREIGN KEY (author_id) REFERENCES Authors(id)
        )
    ''')

    conn.commit()
    conn.close()

def connect_to_db():
    return sqlite3.connect(DB_NAME)

def get_paper_from_db(cursor, paper_id):
    cursor.execute('SELECT * FROM Papers WHERE paper_id = ?', (paper_id,))
    return cursor.fetchone()

def insert_paper_to_db(cursor, paper, paper_id):
    cursor.execute('''
        INSERT INTO Papers (paper_id, paper_url, title, comment, citations)
        VALUES (?, ?, ?, ?, ?)
    ''', (paper_id, paper.pdf_url, paper.title, paper.comment, NO_CITATIONS_INDICATOR))  # Assuming 0 citations initially

def insert_author_to_db(cursor, author_name):
    cursor.execute('''
        INSERT INTO Authors (fullname)
        VALUES (?)
    ''', (author_name,))
    return cursor.lastrowid  # Return the auto-generated id of the last inserted row

def link_paper_author(cursor, paper_id, author_id):
    cursor.execute('''
        INSERT INTO PaperAuthors (paper_id, author_id)
        VALUES (?, ?)
    ''', (paper_id, author_id))

def get_authors(cursor, paper_id):
    cursor.execute('''
        SELECT Authors.fullname
        FROM Authors
        JOIN PaperAuthors ON Authors.id = PaperAuthors.author_id
        WHERE PaperAuthors.paper_id = ?
    ''', (paper_id,))
    return [row[0] for row in cursor.fetchall()]

def get_unprocessed_papers(cursor):
    cursor.execute("SELECT paper_id FROM Papers WHERE citations = -1")
    return [row[0] for row in cursor.fetchall()]


def update_paper_citations_in_db(cursor, paper_id, citation_count):
    cursor.execute(
        "UPDATE Papers SET citations = ? WHERE paper_id = ?",
        (citation_count, paper_id)
    )

def update_failed_paper_citations_in_db(cursor, paper_id):
    cursor.execute(
        "UPDATE Papers SET citations = ? WHERE paper_id = ?",
        (FAILED_FETCH_CITATIONS_INDICATOR, paper_id)
    )