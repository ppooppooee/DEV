import json, re, sys, time
from pathlib import Path
out_file = Path('/home/woogaxon/WORK/DEV/tmp_direct_invoke_output.json')
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

res = {'stage':'start', 'time': time.time()}
out_file.write_text(json.dumps(res, ensure_ascii=False))

if not sample:
    res = {'error': 'Could not extract sample_transcript'}
    out_file.write_text(json.dumps(res, ensure_ascii=False))
    sys.exit(1)

res = {'stage':'extracted_sample', 'sample_snippet': sample[:300]}
out_file.write_text(json.dumps(res, ensure_ascii=False))

try:
    import lmstudio_adapter
    LMStudioChat = getattr(lmstudio_adapter, 'LMStudioChat', None)
    if LMStudioChat is None:
        out_file.write_text(json.dumps({'error':'LMStudioChat not found'}, ensure_ascii=False))
        sys.exit(1)
    client = LMStudioChat()
    try:
        client.timeout = 30
    except Exception:
        pass
    out_file.write_text(json.dumps({'stage':'instantiated','model':getattr(client,'model',None)}, ensure_ascii=False))
    start = time.time()
    # invoke and capture
    resp = client.invoke(sample, system_prompt='간결한 한국어로, JSON만 반환하세요.', max_tokens=1024)
    elapsed = time.time()-start
    raw_text = getattr(resp, 'content', None) or str(resp)
    out = {'stage':'invoked','elapsed':elapsed,'raw_preview': raw_text[:4000], 'raw_full': raw_text}
    out_file.write_text(json.dumps(out, ensure_ascii=False))
except Exception as e:
    import traceback
    out_file.write_text(json.dumps({'exception': traceback.format_exc()}, ensure_ascii=False))
    sys.exit(1)

print('Wrote output to', out_file)
