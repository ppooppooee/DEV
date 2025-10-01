import json
nb = json.load(open('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.executed.ipynb','r',encoding='utf-8'))
for idx,cell in enumerate(nb.get('cells',[]), start=1):
    src = ''.join(cell.get('source','')) if isinstance(cell.get('source',''), list) else str(cell.get('source',''))
    if 'Running diagnostic to capture raw LLM output' in src:
        print('Found diagnostic cell at', idx)
        outs = cell.get('outputs', [])
        for out in outs:
            if out.get('output_type') == 'stream':
                print('--- STREAM ---')
                print(''.join(out.get('text',[])))
            elif out.get('output_type') == 'error':
                print('--- ERROR ---')
                print('\n'.join(out.get('traceback',[])))
        break
else:
    print('Diagnostic cell not found')
