# parser.py

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime
from my_embedding import get_embedding
from db_process import insert_document

def infer_source_type(url: str) -> str:
    domain = urlparse(url).netloc.lower()
    if "naver.com" in domain or "daum.net" in domain:
        return "news"
    if "wikipedia.org" in domain:
        return "wiki"
    if "github.com" in domain:
        return "opensource"
    return "blog"

def parse_and_save(html: str, url: str, conn, save_dir=None):
    """
    HTML을 정제하고, 임베딩을 생성한 후
    insert_document를 통해 DB에 저장합니다.
    """
    # HTML 전처리
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title else "No Title"
    for tag in soup(["nav", "footer", "aside", "header", "script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    cleaned = "\n".join(line.strip() for line in text.splitlines() if line.strip())

    # 문서 메타/콘텐츠 구성
    now = datetime.now()
    doc = {
        "source_type": infer_source_type(url),
        "source_name": urlparse(url).netloc,
        "url": url,
        "title": title,
        "author": None,
        "content": cleaned,
        "raw_html": html,
        "crawl_date": now,
        "published_date": None,
        "language": "ko",
        "tags": ["크롤링"],
        "license": None,
        "checksum": str(hash(cleaned)),
        "embedding": get_embedding(cleaned),
        "created_at": now,
        "updated_at": now
    }

    insert_document(conn, doc)

    # 다음 링크 수집
    links = set()
    for a in soup.find_all("a", href=True):
        full = urljoin(url, a["href"])
        p = urlparse(full)
        if p.scheme in ("http", "https"):
            links.add(full)
    return links


def extract_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if parsed.scheme in ["http", "https"]:
            links.add(full_url)
    return links

def preprocess_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title = soup.title.string.strip() if soup.title else "No Title"
    for tag in soup(["nav", "footer", "aside", "header", "script", "style", "noscript", "iframe"]):
        tag.decompose()

    content = None
    for selector in ["article", "main", "div#content", "div.post", "section", "div#articleBody"]:
        content = soup.select_one(selector)
        if content:
            break
    if not content:
        content = soup.body

    for a in content.find_all("a"):
        a.unwrap()

    raw_text = content.get_text(separator="")
    cleaned_lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    cleaned_text = "\n".join(cleaned_lines)

    return {
        "title": title,
        "cleaned_text": cleaned_text,
        "raw_html": str(soup)
    }