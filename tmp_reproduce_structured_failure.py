from lmstudio_adapter import StructuredLLM

# Use the user's example meeting summary as raw LLM output
meeting_summary = '''### 팀 회의 요약 (2025-04-29)

**참석자:**
- 김민준
- 이지혜
- 박서준

**주요 내용 및 결정 사항:**

1. **사용자 피드백 분석 결과 공유:**
   - **김민준**은 지난 주 사용자 피드백 분석 결과를 발표했습니다. 주요 문제점으로는 **로그인 절차의 복잡성**이 지적되었으며, 이를 개선하기 위한 작업이 필요하다고 강조했습니다.

2. **UI/UX 개선 프로젝트 제안:**
   - **이지혜**는 로그인 인터페이스와 사용자 경험(UX) 개선에 대한 초안 제작을 요청했습니다.
     - **박서준**은 이 요청에 동의하고, 다음 주 **5월 6일까지** 초안 디자인을 완료하겠다고 약속했습니다.

3. **경쟁사 분석 프로젝트:**
   - **김민준**은 이지혜에게 경쟁사 앱의 로그인 방식에 대한 리서치를 요청했습니다.
     - **이지혜**는 이번 주 금요일인 **5월 2일까지** 리서치 결과를 공유하겠다고 확인했습니다.

4. **다음 회의 준비:**
   - 모든 참석자들은 각자 맡은 프로젝트를 다음 주 회의 전까지 완료하기로 합의했습니다.
   - 오늘의 회의는 이와 같은 결정 사항을 바탕으로 마무리되었습니다.

### 요약 및 다음 단계 계획:
- **박서준**: 로그인 UI/UX 개선 초안 디자인 완료 (5월 6일)
- **이지혜**: 경쟁사 앱 로그인 방식 리서치 완료 및 결과 공유 (5월 2일)
- **팀 전체**: 각 프로젝트의 진행 상황을 다음 주 회의에서 점검하고 필요한 조정 사항 논의 예정.
'''

# Fake parent that always returns the meeting summary (so fallback will get same input)
class AlwaysSameParent:
    def invoke(self, input, system_prompt=None, max_tokens=512):
        return meeting_summary

# Simple model that requires a 'summary' key
class SimpleModel:
    @staticmethod
    def model_validate(obj):
        if isinstance(obj, dict) and 'summary' in obj:
            return obj
        raise ValueError('missing summary')

parent = AlwaysSameParent()
llm = StructuredLLM(parent, SimpleModel)
try:
    res = llm.invoke('irrelevant')
    print('RES:', res)
except Exception as e:
    print('EXC TYPE:', type(e))
    print('EXC MSG:\n', str(e))
