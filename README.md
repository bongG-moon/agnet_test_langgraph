# Compact Manufacturing Chat Service (LangGraph)

제조 데이터 조회와 `current_data` 기반 pandas 후속 분석에 집중한 경량 서비스입니다.
이 버전은 기존 Python 구현의 동작을 유지하면서, 내부 실행 흐름만 `LangGraph StateGraph` 기반으로 옮긴 버전입니다.

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

## 프로젝트 구조

- `app.py`
  - Streamlit 채팅 앱 시작점
- `ui_renderer.py`
  - 결과 표와 요약 영역 렌더링
- `core/agent.py`
  - LangGraph 상태, 노드, 라우팅, 최종 실행 진입점
- `core/data_tools.py`
  - 데이터셋별 조회 함수와 registry
- `core/data_analysis_engine.py`
  - 후속 pandas 분석 실행
- `core/domain_knowledge.py`
  - 제조 도메인 기준 사전

## LangGraph 실행 흐름

`run_agent()`가 호출되면 내부에서 아래 순서로 그래프가 실행됩니다.

```text
app.py
  -> run_agent()
     -> StateGraph
        -> resolve_request
        -> followup_analysis 또는 plan_retrieval
        -> single_retrieval 또는 multi_retrieval
        -> finish
  -> ui_renderer.py
```

핵심 아이디어는 단순합니다.

- 새 조회 질문이면 조회 노드로 보냅니다.
- 이미 조회된 표가 있고 후속 질문이면 분석 노드로 보냅니다.
- 여러 날짜 또는 여러 데이터셋이 필요하면 multi retrieval 경로로 보냅니다.

## 참고 문서

모든 가이드는 `reference_materials/docs/` 아래에 있습니다.

추천 읽기 순서는 아래와 같습니다.

1. `reference_materials/docs/START_HERE.md`
2. `reference_materials/docs/CODE_WALKTHROUGH.md`
3. `reference_materials/docs/QUESTION_GUIDE.md`
4. `reference_materials/docs/LANGGRAPH_DESIGN.md`
5. `reference_materials/docs/DOMAIN_GUIDE.md`
6. `reference_materials/docs/RUN_CHECKLIST.md`
7. `reference_materials/docs/BEGINNER_ADD_GUIDE.md`
8. `reference_materials/docs/TEST_QUESTIONS.md`

## 초보자에게 먼저 추천하는 파일

- `app.py`
  - 화면에서 질문을 받고 `run_agent()`를 호출하는 흐름이 가장 짧게 보입니다.
- `core/agent.py`
  - LangGraph 노드가 어떤 순서로 실행되는지 한눈에 볼 수 있습니다.
- `core/data_tools.py`
  - 실제 조회 데이터가 어떻게 만들어지는지 볼 수 있습니다.
- `core/data_analysis_engine.py`
  - 후속 pandas 분석이 어떻게 실행되는지 볼 수 있습니다.

## 개발 메모

- 결과 payload에는 `execution_engine="langgraph"` 메타데이터가 들어갑니다.
- 다중 조회 비교와 초기 질문 후처리도 LangGraph 노드 흐름 안에서 처리합니다.
- 초보자 가독성을 위해 문서와 핵심 실행 파일에 설명을 자세히 남겨두었습니다.
