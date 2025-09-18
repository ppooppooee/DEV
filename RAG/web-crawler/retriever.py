import json
import numpy as np
from sentence_transformers import SentenceTransformer
from db_process import get_db_connection
from utils import udprint

model = SentenceTransformer("multi-qa-MiniLM-L6-cos-v1")

def get_query_embedding(query: str) -> list[float]:
    return model.encode(query, convert_to_numpy=True).tolist()

def cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

def retrieve_similar_documents(query: str, top_k=5):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT url, title, embedding FROM documents WHERE embedding IS NOT NULL")
    rows = cur.fetchall()
    conn.close()

    query_emb = get_query_embedding(query)
    scored = []
    for url, title, emb in rows:
        try:
            emb = json.loads(emb)
            score = cosine_similarity(query_emb, emb)
            scored.append((score, url, title))
        except Exception as e:
            udprint("[ERROR] 임베딩 파싱 실패:", url, e)

    scored.sort(reverse=True)
    return scored[:top_k]

if __name__ == "__main__":
    print("🔍 검색할 질문을 입력하세요 (예: '예루살렘의 역사적 배경은'):")
    query = input("> ")
    results = retrieve_similar_documents(query)
    for score, url, title in results:
        udprint(f"[SCORE: {score:.4f}] {title} → {url}")