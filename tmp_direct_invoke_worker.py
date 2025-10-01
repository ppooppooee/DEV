#!/usr/bin/env python3
import json, re, sys, traceback, time
from pathlib import Path

OUT_FILE = Path('./tmp_direct_invoke_output.json')
LOG_FILE = Path('./tmp_direct_invoke_log.txt')

def log(msg):
    with LOG_FILE.open('a', encoding='utf-8') as f:
        f.write(msg+"\n")
    print(msg)

try:
    nb_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.ipynb')
    nb = json.load(nb_path.open('r', encoding='utf-8'))
    # extract sample_transcript
    sample = None
    for cell in nb.get('cells', []):
        src = cell.get('source', '')
        if isinstance(src, list):
            joined = '\n'.join(src)
        else:
            joined = str(src)
        if 'sample_transcript' in joined and '"""' in joined:
            m = re.search(r'sample_transcript\s*=\s*"""(.*?)"""', joined, re.S)
            if m:
                sample = m.group(1).strip()
                break
    if not sample:
        log('ERROR: Could not extract sample_transcript')
        sys.exit(2)
    log('Extracted sample_transcript (first 200 chars):')
    log(sample[:200])

    # invoke LM
    try:
        import lmstudio_adapter
        LMStudioChat = getattr(lmstudio_adapter, 'LMStudioChat', None)
        if LMStudioChat is None:
            log('ERROR: LMStudioChat class not found in lmstudio_adapter')
            sys.exit(3)
        client = LMStudioChat()
        log('Instantiated LMStudioChat, model=' + str(getattr(client, 'model', None)))
        # first try a direct HTTP probe to candidate endpoints (helps debug network/endpoint issues)
        try:
            import requests
        except Exception:
            requests = None

        probe_result = None
        if requests is not None and hasattr(client, 'candidate_endpoints'):
            log('Starting HTTP probe of candidate endpoints')
            prompt_text = sample
            payloads = [
                {"model": client.model, "messages": [{"role":"user","content": prompt_text}], "temperature": float(client.temperature), "max_new_tokens": 1024, "stream": False},
                {"model": client.model, "prompt": prompt_text, "temperature": float(client.temperature), "max_new_tokens": 1024},
                {"model": client.model, "input": prompt_text, "temperature": float(client.temperature)},
                {"data": [prompt_text]},
            ]
            for ep in [e for e in client.candidate_endpoints if e]:
                for payload in payloads:
                    try:
                        t0 = time.time()
                        r = requests.post(ep, json=payload, headers=client._headers(), timeout=client.timeout)
                        elapsed = time.time() - t0
                        status = getattr(r, 'status_code', None)
                        log(f'HTTP probe {ep} status={status} elapsed={elapsed:.2f}s')
                        preview = (r.text or '')[:4000]
                        log('Response preview:\n' + preview)
                        if status in (200, 201):
                            try:
                                data = r.json()
                            except Exception:
                                data = r.text
                            probe_result = {'endpoint': ep, 'status': status, 'elapsed': elapsed, 'data': data}
                            break
                    except Exception as e:
                        log('HTTP probe exception for ' + str(ep) + ': ' + repr(e))
                if probe_result is not None:
                    break

        if probe_result is not None:
            result = {'stage': 'http_probe', 'probe': probe_result}
            OUT_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
            log('Wrote HTTP probe result to ' + str(OUT_FILE))
            sys.exit(0)

        # perform invoke (fallback)
        log('Performing LMStudioChat.invoke (fallback) at ' + time.strftime('%Y-%m-%d %H:%M:%S'))
        t0 = time.time()
        resp = client.invoke(sample, system_prompt='간결한 한국어로, JSON만 반환하세요.', max_tokens=1024)
        elapsed = time.time() - t0
        raw_text = getattr(resp, 'content', None) or str(resp)
        log('Invoke returned, elapsed=' + ('%.2f' % elapsed) + 's, length=' + str(len(raw_text)))

        result = {'raw_text': raw_text}
        # attempt to extract JSON block
        m = re.search(r'(?s)(\{.*\}|\[.*\])', raw_text)
        if m:
            jtext = m.group(0)
            result['json_block'] = jtext
            try:
                parsed = json.loads(jtext)
                result['parsed_json'] = parsed
            except Exception as e:
                result['json_parse_error'] = repr(e)

        OUT_FILE.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        log('Wrote result to ' + str(OUT_FILE))
        sys.exit(0)
    except Exception:
        tb = traceback.format_exc()
        log('Invoke failed with exception:\n' + tb)
        try:
            OUT_FILE.write_text(json.dumps({'error': tb}, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception:
            pass
        sys.exit(4)
except Exception:
    tb = traceback.format_exc()
    log('Worker failed pre-invoke: ' + tb)
    try:
        OUT_FILE.write_text(json.dumps({'error': tb}, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception:
        pass
    sys.exit(5)
