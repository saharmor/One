def get_numeric_prefix(s):
   result = []
   for char in s:
      if char.isdigit() or char == '.':
         result.append(char)
      else:
         break  # Stop iterating at the first non-numeric, non-dot character

   if result[-1] == '.':
      result.pop()

   return ''.join(result)

def extract_paper_id_from_url(paper_url: str) -> str:
   # Remove the query string (if any) from the URL
   paper_url = paper_url.split('?')[0]
   # Split the URL by the '/' character
   url_parts = paper_url.split('/')
   # Get the last part of the URL, which is the ID
   arxiv_id = url_parts[-1] if url_parts else None

   if not arxiv_id:
      return None
   
   arxiv_id = get_numeric_prefix(arxiv_id)
   if len(arxiv_id) == 0:
      return None
   
   # remove the ".pdf" suffix if exists
   return arxiv_id.rstrip('.pdf')