import os
import time
import requests
import urllib3
from queue import Queue
from parser import parse_and_save, extract_links
from db_process import get_db_connection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = "/home/woogaxon/WORK/DEV/RAG"
os.chdir(BASE_DIR)

TARGET_FILE = os.path.join(BASE_DIR, "DATA", "targets.txt")
SAVE_DIR = os.path.join(BASE_DIR, "DATA", "pages")
os.makedirs(SAVE_DIR, exist_ok=True)

MAX_DEPTH = 3

  # ✅ depth=0이면 초기 URL만, depth=1이면 링크 한 번 따라감
MAX_DOCS_PER_DEPTH = 100  # 모든 depth에 동일하게 적용

depth_counts = {}  # 각 depth별 문서 수 추적
url_queue = Queue()
visited = set()




def load_targets():
    with open(TARGET_FILE, "r") as f:
        for line in f:
            url = line.strip()
            # 🔍 빈 줄 또는 주석(#)으로 시작하는 줄은 스킵
            if not url or url.startswith("#"):
                continue
            url_queue.put((url, 0))  # 초기 depth는 0

def crawl_and_parse():
    conn = get_db_connection()
    while not url_queue.empty():
        url, depth = url_queue.get()

        if url in visited or depth > MAX_DEPTH:
            continue

        visited.add(url)

        try:
            res = requests.get(url, verify=False, timeout=10)
            if res.status_code == 200:
                print(f"[CRAWLED] {url} (depth={depth})")
                parse_and_save(res.text, url, conn, SAVE_DIR)

                # 다음 depth로 최대 10개 링크만 추출
                if depth < MAX_DEPTH:
                    links = extract_links(res.text, url)
                    limited_links = list(links)[:8]  # ✅ 여기서 리스트로 변환
                    for link in limited_links:
                        if link not in visited:
                            url_queue.put((link, depth + 1))
        except Exception as e:
            print(f"[ERROR] {url}: {e}")
    conn.close()



if __name__ == "__main__":
    load_targets()
    crawl_and_parse()
