"""LM Studio adapter module extracted from the notebook.
Provides:
- LMStudioChat: HTTP adapter with ChatOpenAI-like invoke API
- StructuredLLM: structured-output wrapper that parses LLM output into Pydantic models (Pydantic v2 aware)
- attach_runtime_patches: helper to patch in-kernel objects to ensure structured-output helpers are available

This module is defensive: imports langchain or Pydantic only when available and falls back gracefully.
"""
from __future__ import annotations
import os
import re
import json
from typing import Any, List, Dict, Optional

try:
    import requests
except Exception:
    requests = None

try:
    from langchain.schema import AIMessage
except Exception:
    AIMessage = None

# small helper to extract first JSON object/array from text
def _extract_first_json(text: str) -> Optional[Any]:
    if not isinstance(text, str):
        return None
    m = re.search(r"(?s)(\{.*\}|\[.*\])", text)
    if not m:
        return None
    s = m.group(0)
    try:
        return json.loads(s)
    except Exception:
        try:
            return json.loads(s.replace("'", '"'))
        except Exception:
            return None

class LMStudioChat:
    def __init__(self, model: Optional[str]=None, model_name: Optional[str]=None, temperature: float=0.0, api_url: Optional[str]=None, api_key: Optional[str]=None, timeout: int=60, **kwargs):
        chosen_model = model_name or model or os.getenv("LM_STUDIO_MODEL_NAME") or "exaone-3.5-2.4b-instruct"
        self.model = chosen_model
        self.api_url = (api_url or os.getenv("LM_STUDIO_API_URL") or "http://10.81.94.125:1234").rstrip("/")
        self.temperature = float(temperature)
        self.api_key = api_key or os.getenv("LM_STUDIO_API_KEY")
        self.timeout = timeout
        self.candidate_endpoints = [
            os.path.join(self.api_url, 'v1', 'chat', 'completions') if self.api_url else None,
            os.path.join(self.api_url, 'v1', 'completions') if self.api_url else None,
            os.path.join(self.api_url, 'v1', 'generate') if self.api_url else None,
            os.path.join(self.api_url, 'api', 'predict') if self.api_url else None,
            os.path.join(self.api_url, 'predict') if self.api_url else None,
            self.api_url,
        ]

    def _headers(self):
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def _parse_response(self, data: Any) -> str:
        try:
            if isinstance(data, dict) and "choices" in data:
                choice = data["choices"][0]
                if isinstance(choice, dict):
                    if "message" in choice and isinstance(choice["message"], dict) and "content" in choice["message"]:
                        return choice["message"]["content"]
                    if "text" in choice:
                        return choice["text"]
        except Exception:
            pass
        try:
            if isinstance(data, dict):
                if "result" in data and isinstance(data["result"], str):
                    return data["result"]
                if "text" in data and isinstance(data["text"], str):
                    return data["text"]
                if "outputs" in data and isinstance(data["outputs"], list):
                    for o in data["outputs"]:
                        if isinstance(o, dict):
                            for k in ("text", "content", "output"):
                                if k in o and isinstance(o[k], str):
                                    return o[k]
                            if "message" in o and isinstance(o["message"], dict) and "content" in o["message"]:
                                return o["message"]["content"]
        except Exception:
            pass
        return str(data)

    def _messages_to_prompt(self, messages: List[Dict[str,str]]) -> str:
        parts = []
        for m in messages:
            role = m.get('role', 'user')
            content = m.get('content', '')
            parts.append(f"[{role}] {content}")
        return "\n".join(parts)

    def _try_request(self, endpoint: str, payload: dict):
        if requests is None:
            raise RuntimeError('requests library not available in this environment')
        try:
            resp = requests.post(endpoint, json=payload, headers=self._headers(), timeout=self.timeout)
            return resp
        except Exception:
            return None

    def chat_messages(self, messages: List[Dict[str,str]], max_tokens: int = 512) -> str:
        prompt_text = self._messages_to_prompt(messages)
        for ep in [e for e in self.candidate_endpoints if e]:
            payloads = [
                {"model": self.model, "messages": messages, "temperature": float(self.temperature), "max_new_tokens": max_tokens, "stream": False},
                {"model": self.model, "prompt": prompt_text, "temperature": float(self.temperature), "max_new_tokens": max_tokens},
                {"model": self.model, "input": prompt_text, "temperature": float(self.temperature)},
                {"data": [prompt_text]},
            ]
            for payload in payloads:
                resp = self._try_request(ep, payload)
                if resp is None:
                    continue
                if resp.status_code in (200,201):
                    try:
                        data = resp.json()
                        return self._parse_response(data)
                    except Exception:
                        return resp.text
        raise RuntimeError('No endpoint responded')

    def chat(self, prompt: str, system_prompt: Optional[str]=None, max_tokens: int = 512) -> str:
        msgs = []
        if system_prompt:
            msgs.append({"role":"system","content":system_prompt})
        msgs.append({"role":"user","content":prompt})
        return self.chat_messages(msgs, max_tokens=max_tokens)

    def invoke(self, input, system_prompt: Optional[str]=None, max_tokens: int = 512):
        if isinstance(input, str):
            resp = self.chat(input, system_prompt=system_prompt, max_tokens=max_tokens)
            if AIMessage is not None:
                return AIMessage(content=resp)
            return resp
        try:
            msgs = list(input)
            normalized = []
            for m in msgs:
                if isinstance(m, dict):
                    role = m.get('role','user')
                    content = m.get('content','')
                    normalized.append({'role': role, 'content': content})
                else:
                    role = getattr(m, 'role', None) or getattr(m, 'type', 'user')
                    content = getattr(m, 'content', None)
                    if content is None:
                        content = str(m)
                    normalized.append({'role': role, 'content': content})
            resp = self.chat_messages(normalized, max_tokens=max_tokens)
            if AIMessage is not None:
                return AIMessage(content=resp)
            return resp
        except Exception:
            resp = self.chat(str(input), system_prompt=system_prompt, max_tokens=max_tokens)
            if AIMessage is not None:
                return AIMessage(content=resp)
            return resp

class StructuredLLM:
    """Wraps an LMStudioChat-like object and parses its output into a Pydantic model."""
    def __init__(self, parent, pydantic_model, parser=None):
        self._parent = parent
        self._model = pydantic_model
        self._parser = parser

    def invoke(self, input, system_prompt: Optional[str]=None, max_tokens: int = 512):
        resp = self._parent.invoke(input, system_prompt=system_prompt, max_tokens=max_tokens)
        text = getattr(resp, 'content', None) or str(resp)

        # 1) try parser
        if self._parser is not None:
            try:
                if hasattr(self._parser, 'parse'):
                    return self._parser.parse(text)
                if hasattr(self._parser, 'parse_text'):
                    return self._parser.parse_text(text)
            except Exception:
                pass

        # 2) prefer Pydantic v2 APIs
        try:
            if hasattr(self._model, 'model_validate_json'):
                try:
                    return self._model.model_validate_json(text)
                except Exception:
                    pass
            # legacy fallback
            if hasattr(self._model, 'parse_raw'):
                try:
                    return self._model.parse_raw(text)
                except Exception:
                    pass
        except Exception:
            pass

        # 3) extract JSON from text then validate
        j = _extract_first_json(text)
        if j is not None:
            try:
                if hasattr(self._model, 'model_validate'):
                    return self._model.model_validate(j)
                if hasattr(self._model, 'parse_obj'):
                    return self._model.parse_obj(j)
                return j
            except Exception:
                return j

        raise RuntimeError('Structured parsing failed. Output:\n' + text)

# helper to create PydanticOutputParser when langchain present
def _build_parser_for_model(pydantic_model):
    try:
        from langchain_core.output_parsers import PydanticOutputParser
        return PydanticOutputParser(pydantic_object=pydantic_model)
    except Exception:
        try:
            from langchain.output_parsers import PydanticOutputParser
            return PydanticOutputParser(pydantic_object=pydantic_model)
        except Exception:
            return None

# monkey-patch builder for class
def _with_structured_output(self, pydantic_model):
    parser = _build_parser_for_model(pydantic_model)
    return StructuredLLM(self, pydantic_model, parser)

# runtime helper: attach patches to in-kernel objects
def attach_runtime_patches(globals_dict: Optional[dict]=None):
    """If called from a notebook, patch the LMStudioChat and StructuredLLM names in that global namespace.
    If globals_dict is None, no in-place binding is attempted but module-level bindings remain available.
    """
    # ensure class attribute on module-level LMStudioChat if present in that namespace
    if globals_dict is not None:
        # bind LMStudioChat.with_structured_output if LMStudioChat exists in target globals
        try:
            if 'LMStudioChat' in globals_dict and hasattr(globals_dict['LMStudioChat'], '__dict__'):
                setattr(globals_dict['LMStudioChat'], 'with_structured_output', _with_structured_output)
        except Exception:
            pass
        # also bind StructuredLLM in target globals to this implementation
        try:
            globals_dict['StructuredLLM'] = StructuredLLM
        except Exception:
            pass
    # return the helper functions for callers
    return {
        'LMStudioChat': LMStudioChat,
        'StructuredLLM': StructuredLLM,
        'with_structured_output_func': _with_structured_output,
        'attach_runtime_patches': attach_runtime_patches,
    }

__all__ = ['LMStudioChat', 'StructuredLLM', 'attach_runtime_patches', '_extract_first_json']