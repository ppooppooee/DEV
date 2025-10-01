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
import logging

logger = logging.getLogger(__name__)

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
            # try to sanitize common JS-style comments and trailing commas then parse
            try:
                def _sanitize_json_like_text(t: str) -> str:
                    # remove // comments
                    t2 = re.sub(r"//.*?$", "", t, flags=re.MULTILINE)
                    # remove /* ... */ comments
                    t2 = re.sub(r"/\*.*?\*/", "", t2, flags=re.S)
                    # remove trailing commas before } or ]
                    t2 = re.sub(r",\s*([}\]])", r"\1", t2)
                    return t2

                s2 = _sanitize_json_like_text(s)
                try:
                    return json.loads(s2)
                except Exception:
                    try:
                        return json.loads(s2.replace("'", '"'))
                    except Exception:
                        return None
            except Exception:
                return None


def _heuristic_markdown_meeting_parser(text: str) -> Optional[Dict[str, Any]]:
    """Try to extract a simple meeting-summary structure from markdown-like text.
    Returns a dict with possible keys: summary, attendees, decisions, action_items.
    This is a best-effort heuristic parser (not exhaustive).
    """
    if not isinstance(text, str):
        return None
    out: Dict[str, Any] = {}
    # attendees: look for lines under headings like '참석자' or 'Attendees'
    attendees = []
    m = re.search(r"(?mi)^(?:\*\*|##\s*)?\s*(참석자|Attendees)\s*[:\n]\s*(.*)$", text)
    if m:
        # capture the following lines until a blank line or another heading
        start = m.start(2)
        tail = text[start:]
        lines = tail.splitlines()
        for ln in lines:
            ln = ln.strip()
            if ln == '':
                break
            # bullet items
            b = re.sub(r'^[-\*]\s*', '', ln)
            if b:
                attendees.append(b)
        if attendees:
            out['attendees'] = attendees

    # summary: take the first paragraph after a top heading or the first N chars
    # try heading '요약' or 'Summary'
    msum = re.search(r"(?mi)^(?:\#\#\s*)?\s*(요약|Summary|요약 및 다음 단계|Summary and next steps)\s*[:\n]\s*(.*)$", text)
    if msum:
        para = msum.group(2).strip()
        # take until blank line
        para_lines = para.splitlines()
        if para_lines:
            out['summary'] = para_lines[0].strip()
    else:
        # fallback: first non-heading paragraph
        paras = re.split(r"\n\s*\n", text.strip())
        if paras:
            first = paras[0]
            # drop markdown headings
            first = re.sub(r'^#+\s*', '', first)
            out['summary'] = first.strip()

    # decisions/action items: look for numbered lists under sections
    decisions = []
    action_items = []
    # simple pattern: lines starting with digits + '.' or '-' under '결정' or '결정 사항' or '다음 단계' sections
    sections = re.split(r"(?m)^#{1,3}\s*", text)
    for sec in sections:
        if re.search(r'(?i)결정|결정 사항|Decisions|Action Items|다음 단계', sec):
            for ln in sec.splitlines():
                ln2 = ln.strip()
                if re.match(r'^[0-9]+\.|^[-\*]\s', ln2):
                    item = re.sub(r'^[0-9]+\.|^[-\*]\s*', '', ln2).strip()
                    # heuristic: if contains date or 'by' consider as action item
                    if re.search(r'\bby\b|\b까지\b|\bdeadline\b|\b\d{1,2}/\d{1,2}\b', item, flags=re.I):
                        action_items.append({'item': item})
                    else:
                        decisions.append(item)
    if decisions:
        out['decisions'] = decisions
    if action_items:
        out['action_items'] = action_items

    # if we only have an empty summary, treat as no parse
    if not out:
        return None
    return out


def _coerce_simple_json(obj: Any) -> Any:
    """Recursively coerce simple JSON-like structures to make them more likely
    to validate against strict schemas. This is intentionally conservative:
    - replace None with 'unknown'
    - ensure that keys named 'assignee' or 'owner' are strings (use 'unknown')
    - trim string values
    """
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            if v is None:
                if k.lower() in ('assignee', 'owner'):
                    new[k] = 'unknown'
                else:
                    new[k] = ''
            else:
                new[k] = _coerce_simple_json(v)
        return new
    if isinstance(obj, list):
        return [_coerce_simple_json(v) for v in obj]
    if isinstance(obj, str):
        return obj.strip()
    # numbers, booleans pass through
    return obj


def _coerce_by_schema(obj: Any, schema: Any) -> Any:
    """Coerce values according to a JSON Schema-like dict.
    For enum fields, replace None or missing with the first enum value.
    For object properties, recurse. For arrays, apply to items.
    This is conservative and only touches None/missing values for enum fields.
    """
    try:
        if not isinstance(schema, dict):
            return obj
        # handle object
        stype = schema.get('type')
        if stype == 'object' or ('properties' in schema):
            props = schema.get('properties', {})
            if not isinstance(obj, dict):
                return obj
            new = dict(obj)
            required = schema.get('required', []) or []
            for k, subschema in props.items():
                missing = (k not in new) or (new.get(k) is None)
                enum = subschema.get('enum') if isinstance(subschema, dict) else None
                if missing:
                    # if enum present, use first enum
                    if enum and isinstance(enum, list) and len(enum) > 0:
                        new[k] = enum[0]
                        continue
                    # if required, fill conservative default based on type
                    if k in required:
                        st = None
                        if isinstance(subschema, dict):
                            st = subschema.get('type')
                        if st == 'string' or st is None:
                            new[k] = ''
                        elif st == 'array':
                            new[k] = []
                        elif st == 'object':
                            new[k] = {}
                        else:
                            new[k] = ''
                        continue
                # recurse if present
                if k in new and isinstance(subschema, dict):
                    new[k] = _coerce_by_schema(new[k], subschema)
            return new
        # handle array
        if stype == 'array' or 'items' in schema:
            itemschema = schema.get('items')
            if isinstance(obj, list) and itemschema:
                return [_coerce_by_schema(v, itemschema) for v in obj]
            return obj
        return obj
    except Exception:
        return obj


def _coerce_types_by_schema(obj: Any, schema: Any) -> Any:
    """Coerce values to expected primitive types according to a JSON-schema-like dict.
    Conservative transformations:
    - If schema expects a string but value is dict/list -> convert to a reasonable string (pick common fields or json-dump).
    - If schema expects an array but value is a string -> try to parse JSON array or split on newlines/commas.
    - If schema expects an object but value is a string -> try json.loads, else keep original.
    This attempts to fix common mismatches produced by converters or LLMs returning nested structures.
    """
    try:
        if schema is None or not isinstance(schema, dict):
            return obj

        stype = schema.get('type')

        # handle object expectation but got string
        if stype == 'object' or ('properties' in schema):
            if isinstance(obj, str):
                try:
                    loaded = json.loads(obj)
                    if isinstance(loaded, dict):
                        return _coerce_types_by_schema(loaded, schema)
                except Exception:
                    return obj
            if isinstance(obj, dict):
                # recurse into properties
                props = schema.get('properties', {})
                new = {}
                for k, v in obj.items():
                    if k in props:
                        new[k] = _coerce_types_by_schema(v, props[k])
                    else:
                        new[k] = v
                return new
            return obj

        # expect string but got dict/list
        if stype == 'string':
            if isinstance(obj, dict):
                # try common textual keys
                for key in ('text', 'reason', 'summary', 'content', 'description'):
                    if key in obj and isinstance(obj[key], (str, int, float)):
                        return str(obj[key]).strip()
                # fallback to json dump
                try:
                    return json.dumps(obj, ensure_ascii=False)
                except Exception:
                    return str(obj)
            if isinstance(obj, list):
                # join stringifiable elements
                try:
                    parts = []
                    for el in obj:
                        if isinstance(el, (str, int, float)):
                            parts.append(str(el))
                        else:
                            parts.append(json.dumps(el, ensure_ascii=False))
                    return '; '.join(parts)
                except Exception:
                    return str(obj)
            return obj

        # expect array but got string
        if stype == 'array' or 'items' in schema:
            if isinstance(obj, str):
                # try parse as json list
                s = obj.strip()
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        # coerce items according to items schema if possible
                        itemschema = schema.get('items')
                        if itemschema:
                            return [_coerce_types_by_schema(x, itemschema) for x in parsed]
                        return parsed
                except Exception:
                    # fallback: split on newlines or commas
                    if '\n' in s:
                        parts = [p.strip() for p in s.splitlines() if p.strip()]
                        return parts
                    if ',' in s:
                        parts = [p.strip() for p in s.split(',') if p.strip()]
                        return parts
                return [s]
            if isinstance(obj, list):
                itemschema = schema.get('items')
                if itemschema:
                    return [_coerce_types_by_schema(x, itemschema) for x in obj]
                return obj
            return obj

        return obj
    except Exception:
        return obj


def _normalize_converted_json(conv: Any, schema: Optional[dict]=None) -> Any:
    """Normalize common variations in converter output to expected schema field names.
    Examples:
    - sentiment_label -> sentiment
    - analysis.sentiment or analysis.sentiment_label -> sentiment
    - action_items[].assignee null -> keep for coercion
    This function mutates a shallow copy and returns it.
    """
    if not isinstance(conv, dict):
        return conv
    out = dict(conv)
    # sentiment mapping
    if 'sentiment' not in out:
        if 'sentiment_label' in out:
            out['sentiment'] = out.pop('sentiment_label')
        elif 'analysis' in out and isinstance(out['analysis'], dict):
            a = out['analysis']
            if 'sentiment' in a:
                val = a['sentiment']
                if isinstance(val, dict) and 'label' in val:
                    out['sentiment'] = val.get('label')
                elif isinstance(val, str):
                    out['sentiment'] = val
            elif 'sentiment_label' in a:
                out['sentiment'] = a.get('sentiment_label')

    # other heuristics can be added here: e.g., flatten action_items from nested 'analysis' keys
    if 'action_items' not in out and 'analysis' in out and isinstance(out['analysis'], dict):
        a = out['analysis']
        if 'action_items' in a:
            out['action_items'] = a['action_items']

    return out


def _coerce_field_by_path(obj: Any, path: str) -> Any:
    """Coerce the field at the given dotted path in obj to a string if it's a dict/list.
    Path example: 'reason' or 'action_items.0.assignee'. Returns modified obj (shallow copy when needed).
    """
    if not isinstance(obj, dict) or not path:
        return obj
    parts = path.split('.')
    cur = obj
    parents = []
    for i, p in enumerate(parts):
        parents.append((cur, p))
        # try numeric index
        if isinstance(cur, list):
            try:
                idx = int(p)
                if idx < 0 or idx >= len(cur):
                    return obj
                cur = cur[idx]
                continue
            except Exception:
                return obj
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
            continue
        # not found
        return obj

    # cur now points to target
    target = cur
    # if target is dict or list, coerce to string
    newval = target
    try:
        if isinstance(target, dict) or isinstance(target, list):
            newval = json.dumps(target, ensure_ascii=False)
        else:
            # leave primitives alone
            return obj
    except Exception:
        try:
            newval = str(target)
        except Exception:
            return obj

    # apply change to a copy
    newobj = dict(obj)
    cur2 = newobj
    for j, p in enumerate(parts):
        last = (j == len(parts) - 1)
        if isinstance(cur2, list):
            idx = int(p)
            if last:
                cur2[idx] = newval
            else:
                cur2 = cur2[idx]
            continue
        # ensure dict
        if last:
            cur2[p] = newval
        else:
            # clone nested dict to avoid mutating original
            nxt = dict(cur2.get(p, {})) if isinstance(cur2.get(p, {}), dict) else cur2.get(p)
            cur2[p] = nxt
            cur2 = nxt
    return newobj

class LMStudioChat:
    def __init__(self, model: Optional[str]=None, model_name: Optional[str]=None, temperature: float=0.0, api_url: Optional[str]=None, api_key: Optional[str]=None, timeout: int=60, **kwargs):
        chosen_model = model_name or model or os.getenv("LM_STUDIO_MODEL_NAME") or "exaone-3.5-2.4b-instruct"
        self.model = chosen_model
        self.api_url = (api_url or os.getenv("LM_STUDIO_API_URL") or "http://10.81.28.113:1234").rstrip("/")
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

        # 3b) heuristic markdown parser: try a rules-based extraction before asking the LM to convert
        try:
            heur = _heuristic_markdown_meeting_parser(text)
            if heur is not None:
                try:
                    if hasattr(self._model, 'model_validate'):
                        return self._model.model_validate(heur)
                    if hasattr(self._model, 'parse_obj'):
                        return self._model.parse_obj(heur)
                    return heur
                except Exception:
                    # if model validation fails, fall through to the LM-based converter
                    logger.debug('StructuredLLM: heuristic parser output failed validation')
                    pass
        except Exception:
            pass

        # 4) Fallback: ask the underlying LM to convert the human-readable output
        # into strict JSON that matches the model schema (if available). This helps
        # when the model returned a nicely formatted markdown/text summary instead
        # of raw JSON. We'll attempt a configurable number of conversion attempts
        # and log failures for debugging.
        retries = int(os.getenv('LM_STUDIO_STRUCTURED_RETRIES', '2'))
        logfile = os.getenv('LM_STUDIO_STRUCTURED_LOGFILE')
        last_conv_text = None
        last_error = None
        try:
            # try to obtain a JSON Schema from the pydantic model if possible
            schema = None
            try:
                if hasattr(self._model, 'model_json_schema'):
                    schema = self._model.model_json_schema()
                elif hasattr(self._model, 'schema'):
                    schema = self._model.schema()
            except Exception:
                schema = None

            for attempt in range(1, max(1, retries) + 1):
                # build system prompt for converter
                if schema:
                    system = (
                        "You are a converter that MUST output only valid JSON.\n"
                        "Convert the following assistant output into a JSON object that conforms to the provided JSON Schema.\n"
                        "Output MUST be parsable as JSON and contain only the JSON (no surrounding explanation).\n\n"
                        "Assistant output:\n" + text + "\n\nJSON Schema:\n" + json.dumps(schema, ensure_ascii=False)
                    )
                else:
                    system = (
                        "You are a converter that MUST output only valid JSON.\n"
                        "Convert the following assistant output into a JSON object.\n"
                        "Try to preserve semantic fields (e.g., attendees, decisions, action_items) if present.\n"
                        "Output MUST be parsable as JSON and contain only the JSON (no surrounding explanation).\n\n"
                        "Assistant output:\n" + text
                    )

                # if we attempted before and had a parsing/validation error, include it to help the converter
                if last_conv_text is not None or last_error is not None:
                    system += "\n\nPrevious conversion attempt result:\n" + (last_conv_text or "(none)")
                    if last_error is not None:
                        system += "\nPrevious validation error:\n" + str(last_error)

                try:
                    conv_resp = self._parent.invoke(text, system_prompt=system, max_tokens=max_tokens)
                    conv_text = getattr(conv_resp, 'content', None) or str(conv_resp)
                    last_conv_text = conv_text
                    conv_j = _extract_first_json(conv_text)
                    if conv_j is not None:
                        try:
                            # normalize common field name variations before validation
                            conv_j = _normalize_converted_json(conv_j)
                            if hasattr(self._model, 'model_validate'):
                                return self._model.model_validate(conv_j)
                            if hasattr(self._model, 'parse_obj'):
                                return self._model.parse_obj(conv_j)
                            return conv_j
                        except Exception as e:
                            last_error = e
                            # validation failed; try a conservative coercion and revalidate
                            logger.debug('StructuredLLM: validation failed on converted JSON: %s', e)
                            try:
                                coerced = _coerce_simple_json(conv_j)
                                if hasattr(self._model, 'model_validate'):
                                    return self._model.model_validate(coerced)
                                if hasattr(self._model, 'parse_obj'):
                                    return self._model.parse_obj(coerced)
                                return coerced
                            except Exception as e2:
                                # keep original error in last_error and continue
                                logger.debug('StructuredLLM: coercion attempt failed: %s', e2)
                                # try schema-aware coercion if available
                                try:
                                    schema = None
                                    if hasattr(self._model, 'model_json_schema'):
                                        schema = self._model.model_json_schema()
                                    elif hasattr(self._model, 'schema'):
                                        schema = self._model.schema()
                                    if schema:
                                                coerced2 = _coerce_by_schema(coerced, schema)
                                                # additionally try type-based coercion for dict/list vs string mismatches
                                                coerced3 = _coerce_types_by_schema(coerced2, schema)
                                                if hasattr(self._model, 'model_validate'):
                                                    return self._model.model_validate(coerced3)
                                                if hasattr(self._model, 'parse_obj'):
                                                    return self._model.parse_obj(coerced3)
                                                return coerced3
                                except Exception:
                                    pass
                            if logfile:
                                try:
                                    with open(logfile, 'a', encoding='utf-8') as f:
                                        f.write('Validation failed on converted JSON:\n')
                                        f.write(str(e) + '\n')
                                        f.write('Converted text:\n' + conv_text + '\n---\n')
                                except Exception:
                                    pass
                            # continue to next attempt
                            continue
                    else:
                        # conversion produced no parseable JSON
                        last_error = 'No JSON extracted from converter output'
                        logger.debug('StructuredLLM: conversion produced no JSON on attempt %d', attempt)
                        if logfile:
                            try:
                                with open(logfile, 'a', encoding='utf-8') as f:
                                    f.write('Conversion produced no JSON on attempt %d\n' % attempt)
                                    f.write('Converter output:\n' + conv_text + '\n---\n')
                            except Exception:
                                pass
                        continue
                except Exception as e:
                    last_error = e
                    logger.debug('StructuredLLM: converter invoke failed on attempt %d: %s', attempt, e)
                    if logfile:
                        try:
                            with open(logfile, 'a', encoding='utf-8') as f:
                                f.write('Converter invoke failed on attempt %d:\n' % attempt)
                                f.write(str(e) + '\n')
                        except Exception:
                            pass
                    continue

        except Exception:
            pass
        # Attempt targeted field coercion based on last_error message (e.g., pydantic reports that 'reason' should be a string)
        try:
            if last_error is not None and isinstance(last_conv_text, str):
                # try to extract a field name from common pydantic error text patterns
                import re as _re
                m = _re.search(r"for (\S+)\\n.*?Input should be a valid string|\n(\w+)\n  Input should be a valid string", str(last_error))
                field_candidates = set()
                if m:
                    for g in m.groups():
                        if g:
                            # strip punctuation
                            field_candidates.add(g.strip().strip('"\'`'))
                # fallback: look for 'reason' specifically since it's common
                if not field_candidates:
                    if 'reason' in (last_conv_text or ''):
                        field_candidates.add('reason')

                # parse last_conv_text as JSON to modify
                try:
                    conv_j = _extract_first_json(last_conv_text)
                except Exception:
                    conv_j = None
                if conv_j and isinstance(conv_j, dict) and field_candidates:
                    for fld in field_candidates:
                        try:
                            coerced_field = _coerce_field_by_path(conv_j, fld)
                            if coerced_field is not conv_j:
                                # revalidate
                                try:
                                    if hasattr(self._model, 'model_validate'):
                                        return self._model.model_validate(coerced_field)
                                    if hasattr(self._model, 'parse_obj'):
                                        return self._model.parse_obj(coerced_field)
                                    return coerced_field
                                except Exception:
                                    # continue trying other candidates
                                    pass
                        except Exception:
                            pass
        except Exception:
            pass

        # If we get here, all attempts failed
        # attach a helpful debug snippet to the runtime error
        debug_msg = '\n\n[converter_attempts_failed] last_converter_output:\n' + (last_conv_text or '') + '\nlast_error:\n' + (str(last_error) if last_error is not None else '')
        raise RuntimeError('Structured parsing failed. Output:\n' + text + debug_msg)

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

# Ensure the LMStudioChat class has the with_structured_output method at module load time
try:
    setattr(LMStudioChat, 'with_structured_output', _with_structured_output)
except Exception:
    pass

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