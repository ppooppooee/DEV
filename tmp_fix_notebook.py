import json
from pathlib import Path
p = Path('/home/woogaxon/WORK/DEV/colabNotebooks/1.LangChainRAG4LMS.ipynb')
nb = json.load(p.open('r', encoding='utf-8'))
changed = False
for i,cell in enumerate(nb.get('cells',[])):
    src = cell.get('source','')
    joined = ''.join(src) if isinstance(src,list) else str(src)
    if 'print(llm.invoke(' in joined and 'code\npython' in joined:
        print('Fixing cell index', i+1)
        # Replace source with diagnostic-only cell (as list of lines)
        new_src = [
            "# Diagnostic helper: call the underlying LLM directly and print raw output + extracted JSON\n",
            "import re, json, traceback\n",
            "print('Running diagnostic to capture raw LLM output...')\n",
            "try:\n",
            "    if 'Summary_llm' not in globals():\n",
            "        print('Summary_llm not defined; cannot run diagnostic')\n",
            "    else:\n",
            "        parent = getattr(Summary_llm, '_parent', None) or Summary_llm\n",
            "        print('Using parent for raw invoke:', type(parent))\n",
            "        resp = parent.invoke(sample_transcript, system_prompt='간결한 한국어로, JSON만 반환하세요.', max_tokens=1024)\n",
            "        raw_text = getattr(resp, 'content', None) or str(resp)\n",
            "        print('\\n--- RAW TEXT (first 4000 chars) ---')\n",
            "        print(raw_text[:4000])\n",
            "        print('\\n--- END RAW TEXT ---')\n",
            "        # try to find first JSON-like block\n",
            "        m = re.search(r'(?s)(\\{.*\\}|\\[.*\\])', raw_text)\n",
            "        if m:\n",
            "            jtext = m.group(0)\n",
            "            print('\\nFound JSON-like block (first 2000 chars):')\n",
            "            print(jtext[:2000])\n",
            "            try:\n",
            "                parsed = json.loads(jtext)\n",
            "                print('\\nSuccessfully parsed JSON block:')\n",
            "                print(json.dumps(parsed, ensure_ascii=False, indent=2))\n",
            "            except Exception as e:\n",
            "                print('Failed to json.loads extracted block:', e)\n",
            "                traceback.print_exc()\n",
            "        else:\n",
            "            print('No JSON-like block found in raw text')\n",
            "except Exception as e:\n",
            "    print('Diagnostic invocation failed:')\n",
            "    traceback.print_exc()\n"
        ]
        cell['source'] = new_src
        changed = True

if changed:
    json.dump(nb, p.open('w', encoding='utf-8'), ensure_ascii=False, indent=2)
    print('Notebook fixed')
else:
    print('No changes made')
