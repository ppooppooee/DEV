import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError
from nbformat import write
from pathlib import Path
import sys
import traceback


nb_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS2.ipynb')
out_path = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS2.executed.ipynb')

nb = nbformat.read(nb_path.open('r', encoding='utf-8'), as_version=4)
# Preprocess the notebook: create a cleaned copy where install/magic lines are stripped
clean_nb = nbformat.from_dict(nb)
for cell in clean_nb.cells:
	if cell.get('cell_type') != 'code':
		continue
	src = cell.get('source', [])
	if isinstance(src, list):
		lines = src
	else:
		lines = str(src).splitlines()
	cleaned_lines = []
	skip_keywords = ('pip install', '!pip', '!pip3', '!apt', '%%capture', '%pip', '!curl', '!wget')
	for ln in lines:
		s = ln.strip()
		if not s:
			cleaned_lines.append(ln)
			continue
		if s.startswith('!') or s.startswith('%%') or any(k in s for k in skip_keywords):
			# drop install/magic lines only
			continue
		cleaned_lines.append(ln)
	# if all lines were install/magic, replace with a harmless pass comment
	if not any(str(l).strip() for l in cleaned_lines):
		cell['source'] = '# skipped install/magic lines'
	else:
		# nbformat expects cell.source to be a single string
		cell['source'] = '\n'.join(cleaned_lines)

client = NotebookClient(clean_nb, timeout=600, kernel_name='python3')
print('Starting execution of cleaned notebook', nb_path)
try:
	client.execute()
	write(clean_nb, out_path.open('w', encoding='utf-8'))
	print('Execution finished; wrote', out_path)
except CellExecutionError as e:
	print('Cell execution error during cleaned notebook execution:', e)
	traceback.print_exception(type(e), e, e.__traceback__)
	try:
		write(clean_nb, out_path.open('w', encoding='utf-8'))
		print('Wrote partial output to', out_path)
	except Exception as write_err:
		print('Failed to write partial notebook:', write_err)
	sys.exit(1)
except Exception as e:
	print('Unexpected error during cleaned notebook execution:', e)
	traceback.print_exception(type(e), e, e.__traceback__)
	try:
		write(clean_nb, out_path.open('w', encoding='utf-8'))
		print('Wrote partial output to', out_path)
	except Exception as write_err:
		print('Failed to write partial notebook:', write_err)
	sys.exit(1)
