from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote 
import os

def parse_and_save(html, url, save_dir):
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string.strip() if soup.title else "No Title"

    # 🔍 링크 추출 (href만)
    links = [a.get("href") for a in soup.find_all("a", href=True)]

    # 🔧 네비게이션 요소 제거
    for tag in soup(["nav", "footer", "aside", "header", "script", "style"]):
        tag.decompose()

    # 🔍 본문 추출 (우선순위: article > main > content div)
    content = None
    for selector in ["article", "main", "div#content", "div.post", "section"]:
        content = soup.select_one(selector)
        if content:
            break
    if not content:
        content = soup.body  # fallback

    # 🔧 링크 텍스트 제거
    for a in content.find_all("a"):
        a.unwrap()  # 링크 텍스트만 남기지 않고 제거

    # 텍스트 정리
    raw_text = content.get_text()
    cleaned_lines = [line for line in raw_text.splitlines() if line.strip()]
    cleaned_text = "\n".join(cleaned_lines)

    # 파일명 생성
    decoded_url = unquote(url)
    filename = decoded_url.replace("https://", "").replace("http://", "").replace("/", "_") + ".txt"
    filepath = os.path.join(save_dir, filename)

    # 저장
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"URL: {url}\n")
        f.write(f"Title: {title}\n\n")
        f.write(cleaned_text)

    print(f"[SAVED] {filepath}")
    return links  # 링크는 반환


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
