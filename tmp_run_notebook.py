import nbformat
from nbclient import NotebookClient
from nbformat import read, write
from pathlib import Path

nb_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.ipynb')
out_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.executed.ipynb')

nb = nbformat.read(nb_path.open('r', encoding='utf-8'), as_version=4)
client = NotebookClient(nb, timeout=600, kernel_name='python3')

print('Starting execution...')
client.execute()
print('Execution finished; writing output file')
nbformat.write(nb, out_path.open('w', encoding='utf-8'))
print('Wrote', out_path)
