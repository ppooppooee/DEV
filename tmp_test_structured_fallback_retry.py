# Test retry and logging behavior for StructuredLLM fallback
import os
from lmstudio_adapter import StructuredLLM

# Fake parent that returns markdown first, then invalid JSON, then valid JSON
class FlakyParent:
    def __init__(self):
        self.calls = 0
    def invoke(self, input, system_prompt=None, max_tokens=512):
        self.calls += 1
        if self.calls == 1:
            return '### 회의 요약\n- 참가자: A, B\n- 결정: Do X by May 1'
        if self.calls == 2:
            return "I think this should be JSON: {'summary': 'partial'}"
        return '{"summary": "final"}'

class SimpleModel:
    @staticmethod
    def model_json_schema():
        return {"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"]}
    @staticmethod
    def model_validate(obj):
        if isinstance(obj, dict) and 'summary' in obj:
            return obj
        raise ValueError('invalid')

# configure retries and logfile
os.environ['LM_STUDIO_STRUCTURED_RETRIES'] = '3'
os.environ['LM_STUDIO_STRUCTURED_LOGFILE'] = '/tmp/structured_fallback_test.log'

parent = FlakyParent()
llm = StructuredLLM(parent, SimpleModel)
res = llm.invoke('irrelevant')
print('result:', res)
print('parent calls:', parent.calls)
print('log contents preview:')
try:
    with open('/tmp/structured_fallback_test.log','r',encoding='utf-8') as f:
        print('\n'.join(f.read().splitlines()[-20:]))
except Exception as e:
    print('no log or read failed:', e)
