"""
LEGACY: tmp_lmstudio_test.py

This file is kept for history/backwards-compatibility only. The canonical
LMStudio adapter implementation has been moved to:

    /home/woogaxon/WORK/DEV/lmstudio_adapter.py

Please import LMStudioChat from that module for runtime usage:

    from lmstudio_adapter import LMStudioChat

To avoid accidental network calls or side-effects, the script's __main__ test
is disabled by default. If you really want to run the legacy script, edit the
bottom of this file and enable the test block.
"""

import os
os.environ["LM_STUDIO_API_URL"] = "http://10.81.94.125:1234"
os.environ["LM_STUDIO_MODEL_NAME"] = "exaone-3.5-2.4b-instruct"

import requests
from typing import List, Optional
import importlib.util
from pathlib import Path

class LMStudioChat:
    def __init__(self, model: Optional[str] = None, temperature: float = 0.1, api_url: Optional[str] = None, api_key: Optional[str] = None, timeout: int = 60):
        # Prefer explicit model id from env or provided value; fall back to the UI model id
        self.model = model or os.environ.get("LM_STUDIO_MODEL_NAME") or 'exaone-3.5-2.4b-instruct'
        self.api_url = api_url or os.environ.get("LM_STUDIO_API_URL")
        self.api_key = api_key or os.environ.get("LM_STUDIO_API_KEY")
        self.temperature = temperature
        self.timeout = timeout
        self._working_endpoint = None
        self._working_payload_shape = None
        # Prioritize OpenAI-compatible endpoints which LM Studio advertises as supported
        self._candidates = [
            ("/v1/chat/completions", lambda prompt: {"model": self.model, "messages": [{"role":"user","content": prompt}], "temperature": self.temperature, "max_new_tokens": 128, "stream": False}),
            ("/v1/completions", lambda prompt: {"model": self.model, "prompt": prompt, "temperature": self.temperature, "max_new_tokens": 128}),
            # embeddings endpoint (if server provides embeddings)
            ("/v1/embeddings", lambda prompt: {"model": self.model, "input": prompt}),
            # legacy/non-standard endpoints as last resorts
            ("/v1/generate", lambda prompt: {"model": self.model, "prompt": prompt, "temperature": self.temperature}),
            ("/api/predict", lambda prompt: {"model": self.model, "prompt": prompt}),
            ("/predict", lambda prompt: {"data": [{"text": prompt}]})
        ]

    def _headers(self):
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def _detect_and_send(self, prompt: str):
        if self._working_endpoint is not None:
            url = self.api_url.rstrip("/") + self._working_endpoint
            payload = self._working_payload_shape(prompt)
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=self.timeout)
            return resp
        for endpoint, payload_shape in self._candidates:
            url = self.api_url.rstrip("/") + endpoint
            try:
                payload = payload_shape(prompt)
                resp = requests.post(url, json=payload, headers=self._headers(), timeout=self.timeout)
                # Consider responses with HTTP < 300 AND without an error payload as success
                if resp.status_code < 300:
                    # try to detect error messages inside a 200-like response
                    try:
                        j = resp.json()
                        # if server returns an error dict, skip this endpoint
                        if isinstance(j, dict) and ("error" in j or "Unexpected endpoint" in str(j)):
                            # treat as non-working endpoint
                            continue
                    except Exception:
                        # not JSON or no parseable body; accept it
                        pass
                    self._working_endpoint = endpoint
                    self._working_payload_shape = payload_shape
                    return resp
            except Exception as e:
                # continue trying
                # print('candidate failed', endpoint, e)
                continue
        raise RuntimeError(f"No working LM Studio endpoint found at {self.api_url}")

    def _parse_response(self, resp: requests.Response) -> str:
        try:
            j = resp.json()
        except Exception:
            return resp.text
        if isinstance(j, dict):
            if "choices" in j:
                try:
                    choice = j["choices"][0]
                    if isinstance(choice.get("message"), dict):
                        return choice["message"].get("content", "")
                    return choice.get("text", "")
                except Exception:
                    pass
            if "outputs" in j and isinstance(j["outputs"], list) and len(j["outputs"])>0:
                first = j["outputs"][0]
                if isinstance(first, dict):
                    if "data" in first and isinstance(first["data"], list) and len(first["data"])>0:
                        d = first["data"][0]
                        return d.get("text") or d.get("content") or ""
                    return first.get("text") or first.get("content") or ""
            if "response" in j:
                return j.get("response", "")
        return str(j)

    def chat(self, prompt: str) -> str:
        resp = self._detect_and_send(prompt)
        return self._parse_response(resp)

# Embedding wrapper
my_embedding = None
try:
    # Look for my_embedding.py in several likely locations: current dir, parent dirs, and RAG/web-crawler
    cwd = Path.cwd().resolve()
    candidates = []
    candidates.append(cwd / 'RAG' / 'web-crawler' / 'my_embedding.py')
    for p in cwd.parents:
        candidates.append(p / 'RAG' / 'web-crawler' / 'my_embedding.py')
    # common workspace roots as extra candidates
    candidates.extend([
        Path('/home/woogaxon/WORK/DEV') / 'RAG' / 'web-crawler' / 'my_embedding.py',
        Path('/home/woogaxon/WORK') / 'RAG' / 'web-crawler' / 'my_embedding.py'
    ])
    found = None
    for c in candidates:
        if c.exists():
            found = c
            break
    # fallback: rglob for a matching filename (limit depth by checking first match)
    if found is None:
        for match in cwd.rglob('my_embedding.py'):
            # prefer ones inside a web-crawler folder
            if 'web-crawler' in str(match.parent):
                found = match
                break
        if found is None:
            # any match
            for match in cwd.rglob('my_embedding.py'):
                found = match
                break
    if found is not None:
        spec = importlib.util.spec_from_file_location("my_embedding_local", str(found))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        my_embedding = module
        print(f'Loaded my_embedding from {found}')
    else:
        # not fatal; we'll fallback to sentence-transformers if available
        print('my_embedding helper not found; will try sentence-transformers')
except Exception as e:
    print('my_embedding load error:', e)
    my_embedding = None

try:
    from sentence_transformers import SentenceTransformer
except Exception:
    SentenceTransformer = None

class LangChainLocalEmbeddings:
    def __init__(self, model_name: Optional[str] = None):
        self.model_name = model_name
        self._st_model = None
    def _ensure_model(self):
        if my_embedding is not None:
            return
        if SentenceTransformer is None:
            raise ImportError("sentence_transformers not available and no my_embedding helper found")
        if self._st_model is None:
            # Search for the local model directory in current dir, parents, and common workspace roots
            cwd = Path.cwd().resolve()
            candidates = []
            candidates.append(cwd / 'RAG' / 'ko-sroberta-multitask')
            for p in cwd.parents:
                candidates.append(p / 'RAG' / 'ko-sroberta-multitask')
            candidates.extend([
                Path('/home/woogaxon/WORK/DEV') / 'RAG' / 'ko-sroberta-multitask',
                Path('/home/woogaxon/WORK') / 'RAG' / 'ko-sroberta-multitask'
            ])
            found = None
            for c in candidates:
                if c.exists():
                    found = c
                    break
            if found is None:
                # fallback: rglob search for the folder name
                for match in cwd.rglob('ko-sroberta-multitask'):
                    if match.is_dir():
                        found = match
                        break
            if found is None:
                raise FileNotFoundError('ko-sroberta-multitask model directory not found in candidates')
            print(f'Loading SentenceTransformer from {found}')
            self._st_model = SentenceTransformer(self.model_name or str(found))
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        if my_embedding is not None:
            return [my_embedding.get_embedding(t) for t in texts]
        self._ensure_model()
        return self._st_model.encode(texts).tolist()
    def embed_query(self, text: str) -> List[float]:
        if my_embedding is not None:
            return my_embedding.get_embedding(text)
        self._ensure_model()
        return self._st_model.encode([text])[0].tolist()

class EmbeddingsWrapper:
    def __init__(self, impl: LangChainLocalEmbeddings):
        self.impl = impl
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return self.impl.embed_documents(texts)
    def embed_query(self, text: str) -> List[float]:
        return self.impl.embed_query(text)
    def __call__(self, texts: List[str]) -> List[List[float]]:
        return self.embed_documents(texts)

if __name__ == '__main__':
    print('This script is legacy. Prefer importing LMStudioChat from lmstudio_adapter.py')
    # Legacy execution disabled to avoid accidental network calls. To enable,
    # change the following block to `if True:` after reviewing the code.
    if False:
        print('LM_STUDIO_API_URL=', os.environ.get('LM_STUDIO_API_URL'))
        print('LM_STUDIO_MODEL_NAME=', os.environ.get('LM_STUDIO_MODEL_NAME'))
        llm = LMStudioChat()
        emb = EmbeddingsWrapper(LangChainLocalEmbeddings())
        # Try embeddings
        try:
            v = emb.embed_query('테스트 문장')
            print('embedding dim:', len(v))
        except Exception as e:
            print('embedding error:', e)
        # Try chat
        try:
            r = llm.chat('안녕하세요. 테스트 응답을 주세요.')
            print('lm response:', r[:400])
        except Exception as e:
            print('lm error:', e)
