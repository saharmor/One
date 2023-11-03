def extract_paper_id_from_url(paper_url: str) -> str:
   # Remove the query string (if any) from the URL
   paper_url = paper_url.split('?')[0]
   # Split the URL by the '/' character
   url_parts = paper_url.split('/')
   # Get the last part of the URL, which is the ID
   arxiv_id = url_parts[-1] if url_parts else None

   return arxiv_id