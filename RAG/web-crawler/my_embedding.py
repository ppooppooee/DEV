"""Lightweight helper for embeddings that lazily loads sentence-transformers.

This avoids raising ImportError at module import time in notebooks that haven't
installed the dependency yet. Callers should ensure `sentence-transformers` is
installed (e.g. via `pip install sentence-transformers`) or handle the
ImportError when calling `get_embedding`.
"""
import os
from functools import lru_cache
from typing import List, Optional

base_dir = os.path.dirname(os.path.abspath(__file__))
default_model_path = os.path.abspath(os.path.join(base_dir, '../ko-sroberta-multitask'))


@lru_cache(maxsize=1)
def _load_model(model_path: Optional[str] = None):
    """Lazily load and cache the SentenceTransformer model.

    Raises ImportError with a helpful message if sentence-transformers is not
    installed.
    """
    try:
        from sentence_transformers import SentenceTransformer
    except Exception as e:
        raise ImportError(
            "Missing dependency 'sentence-transformers'. Install it with: pip install sentence-transformers\nOriginal error: "
            + str(e)
        )

    path = model_path or default_model_path
    return SentenceTransformer(path)


def get_embedding(text: str, model_path: Optional[str] = None) -> List[float]:
    """Return embedding for `text` as a list[float].

    This will attempt to lazily load the model; if sentence-transformers is
    not installed, an informative ImportError is raised.
    """
    model = _load_model(model_path)
    emb = model.encode(text, convert_to_numpy=True)
    return emb.tolist()
