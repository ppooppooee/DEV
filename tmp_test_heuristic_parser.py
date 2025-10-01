from lmstudio_adapter import _heuristic_markdown_meeting_parser

text = '''### 팀 회의 요약 (2025-04-29)

**참석자:**
- 김민준
- 이지혜
- 박서준

**주요 내용 및 결정 사항:**

1. **사용자 피드백 분석 결과 공유:**
   - 김민준은 ...

4. **다음 회의 준비:**
- 모든 참석자들은 각자 맡은 프로젝트를 다음 주 회의 전까지 완료하기로 합의했습니다.

### 요약 및 다음 단계 계획:
- 박서준: 로그인 UI/UX 개선 초안 디자인 완료 (5월 6일)
- 이지혜: 경쟁사 앱 로그인 방식 리서치 완료 및 결과 공유 (5월 2일)
'''

print(_heuristic_markdown_meeting_parser(text))
