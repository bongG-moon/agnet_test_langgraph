# Compact Manufacturing Chat Service (LangGraph)

제조 데이터 조회와 `current_data` 기반 pandas 후속 분석에 집중한 경량 서비스입니다.
이 버전은 내부 실행 흐름을 `LangGraph StateGraph` 기반으로 구성했습니다.

## 실행

```bash
pip install -r requirements.txt
copy .env.example .env
streamlit run app.py
```

## 현재 포함된 조회 데이터셋

- 생산
- 목표
- 불량
- 설비
- WIP
- 수율
- 홀드
- 스크랩
- 레시피 조건
- LOT 이력

## 루트에 남겨둔 실행 파일

- `app.py`
- `ui_renderer.py`
- `core/`
- `.env.example`
- `requirements.txt`

## 참고 자료 위치

- `reference_materials/docs/`
  - 초보자용 시작 문서
  - 코드 상세 해설
  - 질문 사용법
  - 도메인 가이드
  - 추가/확장 가이드
  - 테스트 질문 모음

## 초보자용 읽기 순서

1. `reference_materials/docs/START_HERE.md`
2. `reference_materials/docs/CODE_WALKTHROUGH.md`
3. `reference_materials/docs/QUESTION_GUIDE.md`
4. `reference_materials/docs/DOMAIN_GUIDE.md`
5. `reference_materials/docs/LANGGRAPH_DESIGN.md`
6. `reference_materials/docs/BEGINNER_ADD_GUIDE.md`
7. `reference_materials/docs/TEST_QUESTIONS.md`
8. `app.py`
9. `core/agent.py`

## 역할별 파일

- `app.py`: Streamlit 채팅 UI 시작점
- `ui_renderer.py`: 결과 표, 조건, 분석 요약 렌더링
- `core/agent.py`: LangGraph 상태/노드 정의와 실행 라우터
- `core/parameter_resolver.py`: 질문 조건 추출과 para 승계
- `core/domain_knowledge.py`: 제조 도메인 기준 사전
- `core/data_tools.py`: 조회 함수와 dataset registry
- `core/data_analysis_engine.py`: LLM 기반 pandas 코드 생성과 실행
- `core/number_format.py`: 수량 컬럼 단위 표시 포맷
