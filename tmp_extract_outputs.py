import json
from pathlib import Path
nb_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.executed.ipynb')
nb = json.load(nb_path.open('r', encoding='utf-8'))
# find the cell that contains 'Invoking Summary_llm...' in its source
for idx, cell in enumerate(nb.get('cells', []), start=1):
    src = cell.get('source', '')
    joined = ''.join(src) if isinstance(src, list) else str(src)
    if 'Invoking Summary_llm' in joined:
        print('Found cell', idx)
        outputs = cell.get('outputs', [])
        for out in outputs:
            print('--- output type:', out.get('output_type'))
            if out.get('output_type') == 'stream':
                print(out.get('text'))
            elif out.get('output_type') == 'error':
                print('Error:', '\n'.join(out.get('traceback', [])))
            elif out.get('output_type') == 'execute_result' or out.get('output_type') == 'display_data':
                print(json.dumps(out.get('data', {}), ensure_ascii=False, indent=2))
        break
else:
    print('Invoking Summary_llm cell not found')
