import json, re, sys
from pathlib import Path
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

# Now invoke LMStudioChat
try:
    import lmstudio_adapter
    LMStudioChat = getattr(lmstudio_adapter, 'LMStudioChat', None)
    if LMStudioChat is None:
        print('LMStudioChat not found in lmstudio_adapter')
        sys.exit(1)
    client = LMStudioChat()
    print('Instantiated LMStudioChat, model=', getattr(client, 'model', None))
    resp = client.invoke(sample, system_prompt='간결한 한국어로, JSON만 반환하세요.', max_tokens=1024)
    raw_text = getattr(resp, 'content', None) or str(resp)
    print('\n--- RAW TEXT ---')
    print(raw_text[:4000])
    print('\n--- END RAW TEXT ---')
    m = re.search(r'(?s)(\{.*\}|\[.*\])', raw_text)
    if m:
        jtext = m.group(0)
        print('\nFound JSON-like block:')
        print(jtext[:2000])
        try:
            parsed = json.loads(jtext)
            print('\nParsed JSON:')
            print(json.dumps(parsed, ensure_ascii=False, indent=2))
        except Exception as e:
            print('Failed to parse JSON block:', e)
    else:
        print('No JSON-like block found')
except Exception as e:
    import traceback
    print('Direct invoke failed:')
    traceback.print_exc()
    sys.exit(1)
