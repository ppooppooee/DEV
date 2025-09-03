from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote
from datetime import datetime
from my_embedding import get_embedding  # 벡터 생성 함수


def infer_source_type(url: str) -> str:
    domain = urlparse(url).netloc.lower()

    if "naver.com" in domain or "daum.net" in domain or "news" in url:
        return "news"
    elif "wikipedia.org" in domain or "wiki" in url:
        return "wiki"
    elif "blog" in domain or "tistory.com" in domain or "medium.com" in domain:
        return "blog"
    elif "github.com" in domain or "gitlab.com" in domain:
        return "opensource"
    else:
        return "news"  # 기본값

def parse_and_save(html, url, conn, save_dir=None):  # ✅ conn 주입

    result = preprocess_html(html)
    title = result["title"]
    cleaned_text = result["cleaned_text"]
    raw_html = result["raw_html"]

    embedding = get_embedding(cleaned_text)
    source_type = infer_source_type(url)
    checksum = str(hash(cleaned_text))
    now = datetime.now()









#======================================================================================
#     New code (merge into)
#======================================================================================
    try:
        with conn.cursor() as cur:
            merge_query = """
                MERGE INTO documents AS d
                USING (
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                ) AS v(
                    source_type, source_name, url, title, author, content,
                    raw_html, crawl_date, published_date, language, tags,
                    license, checksum, embedding, created_at, updated_at
                )
                ON d.url = v.url
                WHEN MATCHED THEN
                    UPDATE SET
                        source_type = v.source_type,
                        source_name = v.source_name,
                        title = v.title,
                        author = v.author,
                        content = v.content,
                        raw_html = v.raw_html,
                        crawl_date = v.crawl_date,
                        published_date = v.published_date,
                        language = v.language,
                        tags = v.tags,
                        license = v.license,
                        checksum = v.checksum,
                        embedding = v.embedding,
                        updated_at = v.updated_at
                WHEN NOT MATCHED THEN
                    INSERT (
                        source_type, source_name, url, title, author, content,
                        raw_html, crawl_date, published_date, language, tags,
                        license, checksum, embedding, created_at, updated_at
                    )
                    VALUES (
                        v.source_type, v.source_name, v.url, v.title, v.author, v.content,
                        v.raw_html, v.crawl_date, v.published_date, v.language, v.tags,
                        v.license, v.checksum, v.embedding, v.created_at, v.updated_at
                    );
            """

            checksum = str(hash(cleaned_text))
            now = datetime.now()

            cur.execute(merge_query, (
                source_type,
                urlparse(url).netloc,
                url,
                title,
                None,
                cleaned_text,
                html,
                now,
                None,
                'ko',
                ['크롤링'],
                None,
                checksum,
                embedding,
                now,
                now
            ))

        conn.commit()
        print(f"[DB MERGED] {url}")

    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {url}: {e}")

#======================================================================================











#======================================================================================
#     old code (insert into)
#======================================================================================


    # ✅ DB 저장 (conn 주입받음)
    # try:
    #     with conn.cursor() as cur:
    #         insert_query = """
    #             INSERT INTO documents (
    #                 source_type, source_name, url, title, author, content,
    #                 raw_html, crawl_date, published_date, language, tags,
    #                 license, checksum, embedding, created_at, updated_at
    #             ) VALUES (
    #                 %s, %s, %s, %s, %s, %s,
    #                 %s, %s, %s, %s, %s,
    #                 %s, %s, %s, %s, %s
    #             )
    #         """
    #         checksum = str(hash(cleaned_text))
    #         now = datetime.now()

    #         cur.execute(insert_query, (
    #             source_type,
    #             urlparse(url).netloc,
    #             url,
    #             title,
    #             None,
    #             cleaned_text,
    #             html,
    #             now,
    #             None,
    #             'ko',
    #             ['크롤링'],
    #             None,
    #             checksum,
    #             embedding,
    #             now,
    #             now
    #         ))

    #     conn.commit()
    #     print(f"[DB SAVED] {url}")

    # except Exception as e:
    #     conn.rollback()  # ✅ 트랜잭션 복구
    #     print(f"[ERROR] DB 저장 실패: {e}")
#======================================================================================

    links = extract_links(html, url)
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

    # 제목 추출
    title = soup.title.string.strip() if soup.title else "No Title"

    # 불필요한 태그 제거
    for tag in soup(["nav", "footer", "aside", "header", "script", "style", "noscript", "iframe"]):
        tag.decompose()

    # 본문 추출
    content = None
    for selector in ["article", "main", "div#content", "div.post", "section", "div#articleBody"]:
        content = soup.select_one(selector)
        if content:
            break
    if not content:
        content = soup.body

    # 링크 제거
    for a in content.find_all("a"):
        a.unwrap()

    # 텍스트 정리
    raw_text = content.get_text(separator="\n")
    cleaned_lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    cleaned_text = "\n".join(cleaned_lines)

    return {
        "title": title,
        "cleaned_text": cleaned_text,
        "raw_html": str(soup)
    }
