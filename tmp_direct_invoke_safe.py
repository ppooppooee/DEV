import json, re, sys, time
from pathlib import Path
from multiprocessing import Process, Queue

nb_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.ipynb')
nb = json.load(nb_path.open('r', encoding='utf-8'))
# extract sample_transcript from cells
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
    print('Could not extract sample_transcript from notebook')
    sys.exit(1)

print('Extracted sample_transcript (first 300 chars):')
print(sample[:300])

# worker function to call LMStudioChat and send results via queue
def worker(q):
    try:
        import lmstudio_adapter
        LMStudioChat = getattr(lmstudio_adapter, 'LMStudioChat', None)
        if LMStudioChat is None:
            q.put({'error':'LMStudioChat not found in lmstudio_adapter'})
            return
        client = LMStudioChat()
        # enforce shorter timeouts if the implementation supports it
        try:
            client.timeout = 30
        except Exception:
            pass
        q.put({'info': f'Instantiated LMStudioChat, model={getattr(client, "model", None)}'})
        start = time.time()
        # call invoke and capture response
        resp = client.invoke(sample, system_prompt='간결한 한국어로, JSON만 반환하세요.', max_tokens=1024)
        elapsed = time.time() - start
        raw_text = getattr(resp, 'content', None) or str(resp)
        q.put({'elapsed': elapsed, 'raw': raw_text})
    except Exception as e:
        import traceback
        q.put({'exception': traceback.format_exc()})

# run worker in separate process and wait with timeout
q = Queue()
p = Process(target=worker, args=(q,))
print('Starting worker process to invoke LM...')
p.start()
# wait for results with a generous timeout
timeout = 120
try:
    waited = 0
    while waited < timeout:
        if not q.empty():
            res = q.get()
            if 'info' in res:
                print(res['info'])
            if 'elapsed' in res:
                print(f"Invocation finished (elapsed {res['elapsed']:.2f}s)")
                print('\n--- RAW TEXT (first 4000 chars) ---')
                print(res['raw'][:4000])
                print('\n--- END RAW TEXT ---')
            if 'raw' in res and 'elapsed' not in res:
                print('\n--- RAW TEXT ---')
                print(res['raw'][:4000])
                print('\n--- END RAW TEXT ---')
            if 'exception' in res:
                print('Worker exception:\n', res['exception'])
            if 'error' in res:
                print('Worker error:', res['error'])
            break
        else:
            time.sleep(0.5)
            waited += 0.5
    else:
        print(f'No response from worker after {timeout}s; terminating worker')
        p.terminate()
        p.join(1)
        print('Worker terminated')
finally:
    if p.is_alive():
        p.terminate()
        p.join()

print('Done')
