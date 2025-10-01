import json
from pathlib import Path
p = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.ipynb')
nb = json.load(p.open('r', encoding='utf-8'))
found = []
for idx, cell in enumerate(nb.get('cells', []), start=1):
    src = cell.get('source', '')
    if isinstance(src, list):
        joined = '\n'.join(src)
    else:
        joined = str(src)
    if 'print(llm.invoke(' in joined or 'print(llm.invoke' in joined:
        print('Found candidate cell', idx)
        print('--- source ---')
        print(joined)
        found.append(idx)

if not found:
    print('No candidate cells found')
else:
    print('Candidates:', found)
