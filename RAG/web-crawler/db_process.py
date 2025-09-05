# db_process.py
from utils import udprint
import psycopg2

DB_CONFIG = {
    'dbname': 'my_database',
    'user': 'postgres',
    'password': 'jypark11!!',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    """
    PostgreSQL 연결을 생성해서 반환합니다.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def insert_document(conn, doc: dict):
    """
    documents 테이블에 MERGE(UPSERT) 로직을 수행합니다.
    """
    merge_query = """
        MERGE INTO documents AS d
        USING (
            VALUES (
                %(source_type)s, %(source_name)s, %(url)s, %(title)s, %(author)s,
                %(content)s, %(raw_html)s, %(crawl_date)s, %(published_date)s::timestamp,
                %(language)s, %(tags)s, %(license)s, %(checksum)s, %(embedding)s,
                %(created_at)s, %(updated_at)s
            )
        ) AS v(
            source_type, source_name, url, title, author, content,
            raw_html, crawl_date, published_date, language, tags,
            license, checksum, embedding, created_at, updated_at
        )
        ON d.url = v.url
        WHEN MATCHED THEN
          UPDATE SET
            source_type   = v.source_type,
            source_name   = v.source_name,
            title         = v.title,
            author        = v.author,
            content       = v.content,
            raw_html      = v.raw_html,
            crawl_date    = v.crawl_date,
            published_date= v.published_date,
            language      = v.language,
            tags          = v.tags,
            license       = v.license,
            checksum      = v.checksum,
            embedding     = v.embedding,
            updated_at    = v.updated_at
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
    try:
        with conn.cursor() as cur:
            cur.execute(merge_query, doc)
        conn.commit()
        udprint(f"[DB MERGED] {doc['url']}")
    except Exception as e:
        conn.rollback()
        udprint(f"[ERROR] {doc['url']}: {e}")
