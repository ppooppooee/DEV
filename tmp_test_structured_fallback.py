# Quick test for StructuredLLM fallback behavior
from lmstudio_adapter import LMStudioChat, StructuredLLM

# Fake parent with invoke returning a human-readable markdown summary first
class FakeParent:
    def __init__(self, first_text, converted_json_text=None):
        self.first = first_text
        self.converted = converted_json_text or '{"summary":"converted"}'
        self.calls = 0
    def invoke(self, input, system_prompt=None, max_tokens=512):
        # First call returns human text, second call (converter invocation) returns JSON
        self.calls += 1
        if self.calls == 1:
            return self.first
        return self.converted

# Simple model-like class with model_validate and model_json_schema
class SimpleModel:
    @staticmethod
    def model_json_schema():
        return {"type":"object","properties":{"summary":{"type":"string"}},"required":["summary"]}
    @staticmethod
    def model_validate(obj):
        # pretend pydantic: accept dict or raise
        if isinstance(obj, dict) and "summary" in obj:
            return obj
        raise ValueError("invalid")

fake_text = '''### 팀 회의 요약\n\n**참석자:**\n- A\n- B\n\n**결정:**\n- Do X by May 1\n'''
parent = FakeParent(fake_text, '{"summary":"팀 회의이 요약: Do X by May 1"}')
wrapped = StructuredLLM(parent, SimpleModel)
res = wrapped.invoke('irrelevant')
print('result:', res)
