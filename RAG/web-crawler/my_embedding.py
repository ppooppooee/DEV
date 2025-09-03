from sentence_transformers import SentenceTransformer
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.abspath(os.path.join(base_dir, '../ko-sroberta-multitask'))
model = SentenceTransformer(model_path)

def get_embedding(text: str) -> list[float]:
    embedding = model.encode(text, convert_to_numpy=True)
    return embedding.tolist()
