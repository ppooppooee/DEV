import json
from pathlib import Path
nb = json.load(open('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.executed.ipynb','r',encoding='utf-8'))
errors = []
for idx, cell in enumerate(nb.get('cells', []), start=1):
    outs = cell.get('outputs', [])
    for out in outs:
        if out.get('output_type') == 'error':
            errors.append((idx, out.get('ename'), out.get('evalue'), out.get('traceback')))

if not errors:
    print('No error outputs found in executed notebook')
else:
    for idx, ename, evalue, tb in errors:
        print('Cell', idx, 'Error:', ename)
        print('Value:', evalue)
        print('\n'.join(tb))
        print('---')
