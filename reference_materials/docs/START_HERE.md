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
  - 질문이 새 조회인지 후속 분석인지 결정
- `core/data_tools.py`
  - 조회 데이터 생성
- `core/domain_knowledge.py`
  - 제조 용어 사전

## 이 프로젝트는 LangGraph 구조인가?

아니요. 이 compact 버전은 LangGraph 노드 그래프를 쓰지 않습니다.  
대신 `run_agent()` 하나를 중심으로 여러 함수를 순서대로 호출하는 단순 구조입니다.

즉 현재 실행 흐름은 대략 아래와 같습니다.

```text
app.py
  -> run_agent()
     -> resolve_required_params()
     -> _choose_query_mode()
     -> _run_retrieval() 또는 _run_followup_analysis()
        -> execute_retrieval_tools() 또는 execute_analysis_query()
  -> ui_renderer.py
```

초보자에게는 이 구조가 더 읽기 쉽습니다.  
노드/엣지 개념보다 “어떤 함수가 다음 함수를 부르는가”만 보면 되기 때문입니다.

## 코드 흐름을 함수 기준으로 보기

### 1. 화면 시작

- `app.py`
  - `main()`
    - Streamlit 화면 시작
  - `_run_chat_turn()`
    - 사용자 질문을 받아 `run_agent()` 호출

### 2. 메인 라우터

- `core/agent.py`
  - `run_agent()`
    - 전체 흐름의 시작점
  - `_choose_query_mode()`
    - 새 조회인지 후속 분석인지 결정
  - `_run_retrieval()`
    - 생산/목표/불량/설비/WIP 같은 새 조회 실행
  - `_run_followup_analysis()`
    - `current_data` 기준 pandas 분석 실행
  - `_run_multi_retrieval()`
    - 여러 데이터셋을 같이 조회하는 질문 처리

### 3. 질문 조건 추출

- `core/parameter_resolver.py`
  - `resolve_required_params()`
    - 날짜, 공정, 제품, MODE 같은 조건 추출
  - `_inherit_from_context()`
    - 이전 질문 조건 승계

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
3. `DOMAIN_GUIDE.md`
4. `BEGINNER_ADD_GUIDE.md`
5. `TEST_QUESTIONS.md`

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
- 후속 pandas 분석이 이상하면: `core/data_analysis_engine.py`, `core/analysis_llm.py`
