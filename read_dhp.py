from bs4 import BeautifulSoup
from pathlib import Path
from load_ati import extract_verses

html = Path("/Users/alee/Downloads/ati/tipitaka/kn/dhp/dhp.01.than.html").read_text()
soup = BeautifulSoup(html, "html.parser")
print(len(extract_verses(soup)))
