# 코드 상세 해설

이 문서는 초보자가 실제 코드를 보면서 따라갈 수 있도록 만든 문서입니다.

목표는 아래 3가지입니다.

1. 어떤 함수가 언제 호출되는지 이해하기
2. 함수 입력과 출력 형태를 이해하기
3. 기능을 수정할 때 어디를 건드려야 하는지 감 잡기

이 문서는 현재 LangGraph 버전 기준으로 설명합니다.

---

## 1. 전체 실행 흐름

```text
app.py
  -> main()
  -> _run_chat_turn()
  -> core/agent.py::run_agent()
     -> _get_agent_graph()
     -> resolve_request
     -> followup_analysis 또는 plan_retrieval
     -> single_retrieval 또는 multi_retrieval
     -> finish
  -> ui_renderer.py
```

### 한 줄 요약

- `app.py`: 화면 시작
- `run_agent()`: 그래프 실행 시작
- `resolve_request`: 질문 조건 추출 + 라우팅 판단
- `single_retrieval` / `multi_retrieval`: 새 데이터 조회
- `followup_analysis`: 현재 데이터 후속 pandas 분석

---

## 2. `app.py`의 `main()`

이 함수는 Streamlit 앱의 시작점입니다.

### 이 코드가 하는 일

- 세션 상태 초기화
- 제목/설명 표시
- 이전 대화 표시
- 새 질문 입력 받기
- 질문이 들어오면 `_run_chat_turn()` 호출
- 받은 결과를 다시 화면에 그림

### 입력 예시

```python
user_input = "오늘 생산량 보여줘"
```

### 초보자 수정 포인트

- 화면 문구를 바꾸고 싶다
  - `main()`
- 입력창 예시를 바꾸고 싶다
  - `st.chat_input(...)`

---

## 3. `app.py`의 `_run_chat_turn()`

이 함수는 화면과 백엔드를 연결하는 다리입니다.

### 이 코드가 하는 일

- `run_agent()` 호출
- 이번 턴에서 뽑은 조건을 `context`에 저장
- 현재 결과 표를 `current_data`에 저장

### 입력 예시

```python
user_input = "상위 5개만 보여줘"
```

### 출력 예시

```python
{
    "response": "현재 결과 기준 상위 5개만 정리했습니다.",
    "tool_results": [...],
    "current_data": {...},
    "extracted_params": {...},
}
```

---

## 4. `core/agent.py`의 `run_agent()`

이 함수가 이 프로젝트의 메인 진입점입니다.

### 이 코드가 하는 일

1. 입력값을 LangGraph 상태로 정리
2. `_get_agent_graph()`로 그래프를 불러옴
3. 그래프를 실행
4. 최종 상태에서 `result`를 꺼내 반환

### 초보자 수정 포인트

- 실행 단계가 헷갈린다
  - `_get_agent_graph()`
- 최종 응답 형식이 이상하다
  - `_finish_node()`

---

## 5. `AgentGraphState`

이 타입은 노드 사이에 주고받는 공용 상태입니다.

대표 필드:

- `user_input`
- `chat_history`
- `context`
- `current_data`
- `extracted_params`
- `query_mode`
- `retrieval_keys`
- `retrieval_jobs`
- `result`

초보자 기준으로는 "함수 인자를 여기저기 따로 넘기지 않고, 한 묶음 딕셔너리로 계속 들고 다니는 방식"이라고 이해하면 됩니다.

---

## 6. `resolve_request` 노드

이 노드는 그래프의 첫 단계입니다.

### 이 코드가 하는 일

- `resolve_required_params()`로 질문 조건 추출
- 현재 질문이 새 조회인지 후속 분석인지 결정
- 다음에 어느 노드로 갈지 판단할 재료를 상태에 저장

### 입력 예시

```python
{
    "user_input": "오늘 DA 공정 생산량 보여줘",
    "current_data": None,
}
```

### 출력 예시

```python
{
    "extracted_params": {
        "date": "20260330",
        "process_name": ["DA"],
    },
    "query_mode": "retrieval",
}
```

### 초보자 수정 포인트

- 질문 조건 추출이 이상하다
  - `resolve_required_params()`
- 새 조회/후속 분석 판단이 이상하다
  - `_choose_query_mode()`

---

## 7. `plan_retrieval` 노드

이 노드는 "무엇을 어떻게 조회할지"를 정리합니다.

### 이 코드가 하는 일

- `pick_retrieval_tools()`로 데이터셋 선택
- 날짜 필수 여부 확인
- 어제/오늘 비교 같은 질문이면 `retrieval_jobs`를 여러 개 생성

### 예시

질문:

```text
어제 DA공정에서 생산량과 오늘 DA공정에서 생산량을 세부 공정별로 비교해줘
```

이 노드는 내부적으로 아래와 비슷한 job을 만듭니다.

```python
[
    {"dataset_key": "production", "date": "20260329", "label": "어제"},
    {"dataset_key": "production", "date": "20260330", "label": "오늘"},
]
```

### 초보자 수정 포인트

- 어떤 데이터셋이 잡히는지 이상하다
  - `pick_retrieval_tools()`
- 날짜 필수 조건이 이상하다
  - `dataset_requires_date()`
- 여러 번 조회해야 하는 질문을 못 나눈다
  - `_extract_date_slices()`

---

## 8. `single_retrieval` 노드

조회 1건만 필요한 경우 실행됩니다.

### 이 코드가 하는 일

- `execute_retrieval_tools()` 호출
- 결과를 `current_data`로 저장
- 질문에 그룹화, 비교, 정렬 의도가 있으면 바로 pandas 후처리까지 수행

### 출력 예시

```python
{
    "result": {
        "response": "오늘 생산량을 조회했습니다.",
        "tool_results": [...],
        "current_data": {...},
    }
}
```

---

## 9. `multi_retrieval` 노드

여러 데이터셋 또는 여러 날짜를 함께 조회할 때 실행됩니다.

### 이 코드가 하는 일

- 여러 retrieval job 반복 실행
- 결과를 하나의 응답으로 묶음
- 필요하면 비교용 테이블 생성
- 질문에 계산 의도가 있으면 후처리까지 이어감

### 대표 질문 예시

- `오늘 생산과 목표를 같이 보여줘`
- `어제 DA공정 생산량과 오늘 DA공정 생산량을 비교해줘`

---

## 10. `followup_analysis` 노드

이 노드는 이미 화면에 있는 `current_data`를 다시 가공합니다.

### 이 코드가 하는 일

- 현재 표가 있는지 확인
- `execute_analysis_query()` 호출
- 생성된 pandas 코드로 후속 분석 실행

### 질문 예시

- `상위 5개만 보여줘`
- `MODE별로 정리해줘`
- `재공은 없는데 생산량은 있는 제품만 보여줘`

---

## 11. `finish` 노드

이 노드는 최종 응답 형식을 정리합니다.

### 이 코드가 하는 일

- `result`가 항상 일정한 형태를 갖도록 마무리
- `execution_engine="langgraph"` 같은 메타데이터 부착

이 단계가 있기 때문에 화면 쪽에서는 "결과 형식이 항상 비슷하다"고 가정하고 처리할 수 있습니다.

---

## 12. `core/data_tools.py`

이 파일은 실제 조회 함수를 모아둔 곳입니다.

대표 함수:

- `pick_retrieval_tools()`
- `execute_retrieval_tools()`
- `build_current_datasets()`
- `get_production_data()`
- `get_target_data()`
- `get_defect_rate()`

초보자 기준으로는 "질문에서 생산/목표/WIP를 골라서, 그에 맞는 mock 데이터를 만드는 곳"이라고 생각하면 됩니다.

---

## 13. `core/data_analysis_engine.py`

이 파일은 후속 pandas 분석의 시작점입니다.

### 이 코드가 하는 일

1. 현재 표가 있는지 확인
2. 없는 컬럼 요청인지 확인
3. LLM에게 pandas 코드 생성 요청
4. 안전 검사
5. 실행
6. 실패 시 재시도

### 초보자 수정 포인트

- 후속 분석이 자주 실패한다
- 파생 컬럼이 자꾸 없는 컬럼처럼 막힌다
- 특정 질문 패턴에서 LLM이 엉뚱한 코드를 만든다

---

## 14. 디버깅 순서

### 질문 조건이 잘못 뽑힌다

1. `core/parameter_resolver.py`
2. `core/domain_knowledge.py`

### 조회 데이터가 이상하다

1. `core/data_tools.py`
2. `_apply_common_filters()`

### 그래프 흐름이 이상하다

1. `core/agent.py`
2. `_route_after_resolve()`
3. `_route_after_retrieval_plan()`

### 후속 pandas 분석이 이상하다

1. `core/data_analysis_engine.py`
2. `core/analysis_llm.py`
3. `core/analysis_helpers.py`

### 화면 표시가 이상하다

1. `ui_renderer.py`
2. `core/number_format.py`
