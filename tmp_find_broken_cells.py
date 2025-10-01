import json
from pathlib import Path
nb = json.load(open('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.ipynb','r',encoding='utf-8'))
for i,cell in enumerate(nb.get('cells',[]), start=1):
    src = cell.get('source','')
    joined = ''.join(src) if isinstance(src,list) else str(src)
    if 'HumanMessage(content="내 이름이 뭐게?' in joined or 'print(llm.invoke(' in joined or 'HumanMessage(content="안녕, 내 이름은 하만득' in joined:
        print('Cell', i)
        print('----')
        print(joined)
        print('----')
