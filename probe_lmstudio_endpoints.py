#!/usr/bin/env python3
import sys, json, traceback
workspace = '/home/woogaxon/WORK/DEV'
if workspace not in sys.path:
    sys.path.insert(0, workspace)

API_URL = 'http://10.81.28.113:1234'
print('Probe script using API_URL=', API_URL)

try:
    import lmstudio_adapter
    from lmstudio_adapter import LMStudioChat
except Exception as e:
    print('Failed to import lmstudio_adapter:', e)
    traceback.print_exc()
    raise SystemExit(2)

chat = LMStudioChat(api_url=API_URL)
print('Constructed LMStudioChat; model=', chat.model)
print('Candidate endpoints:')
for ep in chat.candidate_endpoints:
    print(' -', ep)

payloads = [
    {"model": chat.model, "messages": [{"role":"user","content":"Hello"}]},
    {"model": chat.model, "prompt": "Hello", "max_new_tokens": 10},
    {"input": "Hello"},
    {"data": ["Hello"]},
]

import requests
for ep in chat.candidate_endpoints:
    if not ep:
        continue
    print('\nTesting endpoint:', ep)
    for i,p in enumerate(payloads, start=1):
        try:
            r = requests.post(ep, json=p, timeout=5)
            print(f' payload[{i}] status={r.status_code} len={len(r.text)}')
            h = r.headers.get('Content-Type','')
            print('  content-type=', h)
            # print a short snippet
            print('  snippet:', r.text[:400].replace('\n','\\n'))
            if r.status_code in (200,201):
                try:
                    d = r.json()
                    print('  json keys:', list(d.keys())[:10])
                except Exception:
                    pass
        except Exception as e:
            print(' payload', i, 'failed:', repr(e))

print('\nProbe finished.')
