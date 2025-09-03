import os
import bz2
import xml.etree.ElementTree as ET
from datetime import datetime

# ✅ 경로 설정
BASE_DIR = "/home/woogaxon/WORK/DEV/RAG/DATA"
RAW_PATH = os.path.join(BASE_DIR, "raw", "kowiki-20250820-pages-articles.xml.bz2")
OUTPUT_PATH = os.path.join(BASE_DIR, "processed", "kowiki_pages.txt")
LOG_PATH = os.path.join(BASE_DIR, "logs", "preprocess.log")

# ✅ 로그 기록 함수
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(msg)

# ✅ 네임스페이스 제거 함수
def strip_ns(tag):
    return tag.split("}")[-1] if "}" in tag else tag

# ✅ 제외할 제목 prefix
EXCLUDE_PREFIXES = ("파일:", "틀:", "분류:", "위키백과:", "포털:", "초안:")

# ✅ XML 덤프 전처리 함수
def extract_pages(xml_path, output_path):
    count = 0
    with bz2.open(xml_path, 'rt', encoding='utf-8') as f, open(output_path, 'w', encoding='utf-8') as out:
        context = ET.iterparse(f, events=('end',))

        for event, elem in context:
            if strip_ns(elem.tag) == "page":
                title = elem.findtext(".//{*}title")
                ns = elem.findtext(".//{*}ns")
                text_elem = elem.find(".//{*}text")
                text = text_elem.text if text_elem is not None else ""

                if ns == "0":
                    if title and title.startswith(EXCLUDE_PREFIXES):
                        log(f"[SKIP] {title}")
                    elif title and text and text.strip():
                        out.write(f"## {title}\n{text.strip()}\n\n")
                        log(f"[OK] {title} | text_len={len(text.strip())}")
                        count += 1
                    else:
                        log(f"[EMPTY] {title}")
                else:
                    log(f"[SKIP NS] {title}")

                elem.clear()

    log(f"[DONE] 총 처리 문서 수: {count}")

# ✅ 실행
if __name__ == "__main__":
    extract_pages(RAW_PATH, OUTPUT_PATH)
