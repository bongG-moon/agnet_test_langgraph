# 시작하기

이 문서는 처음 이 프로젝트를 여는 사람이 가장 먼저 읽는 문서입니다.

## 이 서비스가 하는 일

이 서비스는 제조 데이터를 채팅으로 조회하고, 바로 직전 결과를 다시 pandas로 분석할 수 있는 경량 앱입니다.

사용자는 보통 이렇게 씁니다.

1. `오늘 생산량 보여줘`
2. `상위 5개만 보여줘`
3. `공정군별로 그룹화해줘`

즉 첫 질문은 조회, 그다음 질문은 후속 분석입니다.

## 실행 방법

```bash
pip install -r requirements.txt
copy .env.example .env
streamlit run app.py
```

## 지금 들어 있는 데이터셋

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

## 가장 먼저 봐야 할 파일

- `app.py`
  - 채팅 화면 시작점
- `core/agent.py`
  - LangGraph 상태와 실행 흐름
- `core/data_tools.py`
  - 조회 데이터 생성
- `core/domain_knowledge.py`
  - 제조 용어 사전

## 이 프로젝트는 LangGraph 구조인가?

네. 이 버전은 기존 Python 로직의 동작은 최대한 유지하면서, 내부 실행 순서를 `LangGraph`의 `StateGraph`로 옮긴 버전입니다.

즉 현재 실행 흐름은 대략 아래와 같습니다.

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

초보자에게는 이 구조를 "큰 함수 하나 안에서 순서대로 하던 일을, 이름 붙은 단계로 나눈 것"이라고 생각하면 가장 이해하기 쉽습니다.

## 코드 흐름을 함수 기준으로 보기

### 1. 화면 시작

- `app.py`
  - `main()`
    - Streamlit 화면 시작
  - `_run_chat_turn()`
    - 사용자 질문을 받아 `run_agent()` 호출

### 2. 메인 실행 그래프

- `core/agent.py`
  - `run_agent()`
    - 그래프 실행 시작점
  - `_get_agent_graph()`
    - LangGraph `StateGraph` 생성
  - `AgentGraphState`
    - 노드 사이에 넘기는 상태 정의

### 3. 주요 노드

- `resolve_request`
  - 질문에서 날짜, 공정, 제품, MODE 같은 조건 추출
  - 새 조회인지 후속 분석인지 판단

- `plan_retrieval`
  - 어떤 데이터셋을 조회할지 결정
  - 날짜 필수 여부 확인
  - 어제/오늘 비교처럼 여러 조회 job 생성

- `single_retrieval`
  - 조회 1건 실행
  - 필요하면 바로 pandas 후처리 수행

- `multi_retrieval`
  - 여러 데이터셋 또는 여러 날짜 비교 조회 실행
  - 필요하면 공통 비교용 테이블 생성

- `followup_analysis`
  - 현재 화면의 `current_data`를 다시 분석

- `finish`
  - 최종 응답 형식 정리

### 4. 조회 데이터 생성

- `core/data_tools.py`
  - `pick_retrieval_tools()`
    - 어떤 데이터셋이 필요한지 결정
  - `execute_retrieval_tools()`
    - 선택된 조회 함수 실행
  - `build_current_datasets()`
    - 여러 데이터셋 결과를 묶어 저장
  - `get_production_data()`, `get_target_data()`, `get_defect_rate()` 등
    - 실제 mock 데이터 생성

### 5. 후속 pandas 분석

- `core/data_analysis_engine.py`
  - `execute_analysis_query()`
    - 후속 분석의 시작점
  - `_find_semantic_retry_reason()`
    - 질문 의도를 놓친 경우 한 번 더 수정 요청
  - `_execute_with_retry()`
    - 코드 실행 실패 시 재시도

- `core/analysis_llm.py`
  - `build_llm_prompt()`
    - LLM에게 보낼 프롬프트 구성
  - `build_llm_plan()`
    - LLM 응답을 계획/코드 형태로 정리

- `core/analysis_helpers.py`
  - `find_missing_dimensions()`
    - 없는 컬럼 요청인지 확인
  - `validate_plan_columns()`
    - 생성 코드가 실제 컬럼을 쓰는지 확인

- `core/safe_code_executor.py`
  - `execute_safe_dataframe_code()`
    - 생성된 pandas 코드 안전 실행

### 6. 화면 렌더링

- `ui_renderer.py`
  - `render_tool_results()`
    - 결과 표 출력
  - `render_analysis_summary()`
    - 이번 분석 요약 출력
  - `sync_context()`
    - 다음 질문용 context 저장

## 초보자용 읽기 순서

1. `START_HERE.md`
2. `QUESTION_GUIDE.md`
3. `LANGGRAPH_DESIGN.md`
4. `DOMAIN_GUIDE.md`
5. `BEGINNER_ADD_GUIDE.md`

## 정말 중요한 개념 2개

### 1. 새 조회

예:

- `오늘 생산량 보여줘`
- `오늘 TEST 공정 불량 보여줘`

이런 질문은 원본 데이터를 새로 가져옵니다.

### 2. 후속 분석

예:

- `상위 5개만 보여줘`
- `공정군별로 그룹화해줘`
- `목표 대비 달성율 계산해줘`

이런 질문은 방금 본 결과를 다시 가공합니다.

## 막히면 어디부터 볼까

- 실행이 안 되면: `README.md`, `.env`, `requirements.txt`
- 질문 조건이 이상하면: `core/parameter_resolver.py`
- 조회 데이터가 이상하면: `core/data_tools.py`
- 그래프 흐름이 이상하면: `core/agent.py`
- 후속 pandas 분석이 이상하면: `core/data_analysis_engine.py`, `core/analysis_llm.py`
