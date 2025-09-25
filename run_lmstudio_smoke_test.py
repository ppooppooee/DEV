#!/usr/bin/env python3
import sys, traceback, json
workspace = '/home/woogaxon/WORK/DEV'
if workspace not in sys.path:
    sys.path.insert(0, workspace)

API_URL = "http://10.81.28.113:1234"
print('Smoke test script starting. Using API_URL=', API_URL)

try:
    import lmstudio_adapter
    from lmstudio_adapter import LMStudioChat
except Exception as e:
    print('Failed to import lmstudio_adapter:', e)
    traceback.print_exc()
    raise SystemExit(2)

llm = None
# Try a few constructor signatures to be resilient to different module versions
ctor_attempts = [
    {},
    {'api_url': API_URL},
    {'model_name': 'gpt-4', 'temperature': 0.0, 'api_url': API_URL, 'api_key': None},
    {'api_url': API_URL, 'api_key': None},
]

for kw in ctor_attempts:
    try:
        llm = LMStudioChat(**kw)
        print('Instantiated LMStudioChat with kwargs:', kw)
        break
    except TypeError as e:
        # maybe LMStudioChat expects no kwargs
        try:
            llm = LMStudioChat()
            print('Instantiated LMStudioChat with no args (fallback)')
            break
        except Exception:
            pass
    except Exception as e:
        print('Ctor attempt failed for', kw, 'error:', repr(e))

if llm is None:
    print('Failed to create LMStudioChat instance after attempts. Aborting.')
    raise SystemExit(3)

print('LLM instance created:', type(llm))
print('Public attributes (first 40):', [a for a in dir(llm) if not a.startswith('_')][:40])

# 1) Basic invoke requesting JSON
try:
    prompt = "안녕하세요. 짧게 'SMOKE' 태그와 함께 한 문장으로 인사하고 JSON 형식으로 {\"message\": \"...\"}만 반환하세요."
    print('\nInvoking llm.invoke with JSON-only instruction...')
    res = llm.invoke(prompt, system_prompt='Return valid JSON only.', max_tokens=256)
    print('Invocation returned type:', type(res))
    content = getattr(res, 'content', None) or str(res)
    print('Raw content:\n', content)
    import re
    m = re.search(r'(?s)(\{.*\}|\[.*\])', content)
    if m:
        try:
            parsed = json.loads(m.group(0))
            print('\nExtracted JSON:\n', json.dumps(parsed, ensure_ascii=False, indent=2))
        except Exception as e:
            print('Failed to parse extracted JSON:', e)
    else:
        print('No JSON blob detected in response content.')
except Exception as e:
    print('Invocation failed:', repr(e))
    traceback.print_exc()

# 2) If with_structured_output exists, try a tiny Pydantic model
try:
    if hasattr(llm, 'with_structured_output'):
        print('\nLLM supports with_structured_output — attempting structured parse with a tiny Pydantic model.')
        SmokeModel = None
        try:
            from pydantic import BaseModel
            class SmokeModel(BaseModel):
                message: str
        except Exception as e:
            print('Pydantic BaseModel unavailable or failed to import:', e)
            SmokeModel = None

        if SmokeModel is not None:
            try:
                wrapper = llm.with_structured_output(SmokeModel)
                print('Created wrapper:', type(wrapper))
                resp2 = wrapper.invoke('Return JSON only: {"message":"hello from smoke"}', system_prompt='Return JSON only.', max_tokens=128)
                print('Wrapper returned type:', type(resp2))
                if hasattr(resp2, 'model_dump_json'):
                    print('Parsed via pydantic v2:\n', resp2.model_dump_json())
                elif hasattr(resp2, 'json'):
                    print('Parsed via pydantic v1:\n', resp2.json())
                else:
                    print('Wrapper raw result:', resp2)
            except Exception as e:
                print('Structured wrapper invocation failed:', repr(e))
                traceback.print_exc()
        else:
            print('Skipping structured test because Pydantic BaseModel is unavailable')
    else:
        print('\nLLM does not support with_structured_output; skipping structured test.')
except Exception as e:
    print('Structured test general failure:', repr(e))
    traceback.print_exc()

print('\nSmoke test finished.')
