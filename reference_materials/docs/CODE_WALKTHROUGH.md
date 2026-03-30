# 코드 상세 해설

이 문서는 초보자가 실제 코드를 보면서 따라갈 수 있도록 만든 문서입니다.

목표는 아래 3가지입니다.

1. 어떤 함수가 언제 호출되는지 이해하기
2. 함수 입력과 출력 형태를 이해하기
3. 기능을 수정할 때 어디를 건드려야 하는지 감 잡기

이 문서는 현재 코드 기준으로 설명합니다.  
즉 이 프로젝트는 `LangGraph` 노드 그래프가 아니라, 여러 함수를 순서대로 호출하는 **함수 파이프라인 구조**입니다.

---

## 1. 전체 실행 흐름

```text
app.py
  -> main()
  -> _run_chat_turn()
  -> core/agent.py::run_agent()
     -> resolve_required_params()
     -> _choose_query_mode()
     -> _run_retrieval() 또는 _run_followup_analysis()
        -> execute_retrieval_tools() 또는 execute_analysis_query()
  -> ui_renderer.py
```

### 한 줄 요약

- `app.py`: 화면 시작
- `run_agent()`: 메인 라우터
- `resolve_required_params()`: 질문 조건 추출
- `execute_retrieval_tools()`: 새 데이터 조회
- `execute_analysis_query()`: 후속 pandas 분석

---

## 2. `app.py`의 `main()`

이 함수는 Streamlit 앱의 시작점입니다.

### 실제 코드

```python
def main() -> None:
    init_session_state()

    st.title("제조 데이터 채팅 분석")
    st.caption("생산, 목표, 불량, 설비, WIP 조회와 현재 결과 기반 후속 분석을 지원합니다.")
    render_context()
    _render_saved_chat_history()

    user_input = st.chat_input("예: 오늘 생산량 보여줘 / 그 결과를 MODE별로 상위 3개만 정리해줘")
    if not user_input:
        return

    st.session_state.messages.append({"role": "user", "content": user_input})

    result = _run_chat_turn(user_input)
    response = result.get("response", "응답을 생성하지 못했습니다.")
    tool_results = result.get("tool_results", [])
```

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

### 출력 예시

`main()`은 값을 return하지 않습니다.  
대신 화면에 아래를 그립니다.

- 사용자 질문
- AI 응답 텍스트
- 결과 표

### 초보자 수정 포인트

- 화면 문구를 바꾸고 싶다
  - `main()`
- 입력창 예시를 바꾸고 싶다
  - `st.chat_input(...)`

---

## 3. `app.py`의 `_run_chat_turn()`

이 함수는 화면과 백엔드를 연결하는 다리입니다.

### 실제 코드

```python
def _run_chat_turn(user_input: str):
    result = run_agent(
        user_input=user_input,
        chat_history=_get_saved_chat_history(),
        context=st.session_state.context,
        current_data=st.session_state.current_data,
    )

    tool_results = result.get("tool_results", [])
    extracted_params = result.get("extracted_params", {})
    if tool_results:
        sync_context(extracted_params)

    st.session_state.current_data = result.get("current_data")
    return result
```

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

### 초보자 수정 포인트

- 후속 질문이 왜 이상하게 해석되는지 보고 싶다
  - `current_data`, `context` 값이 어떻게 저장되는지 확인

---

## 4. `core/agent.py`의 `run_agent()`

이 함수가 이 프로젝트의 **메인 라우터**입니다.

### 실제 코드

```python
def run_agent(
    user_input: str,
    chat_history: List[Dict[str, str]] | None = None,
    context: Dict[str, Any] | None = None,
    current_data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    chat_history = chat_history or []
    context = context or {}
    current_data = current_data if isinstance(current_data, dict) else None

    extracted_params = resolve_required_params(
        user_input=user_input,
        chat_history_text=_build_recent_chat_text(chat_history),
        current_data_columns=_get_current_table_columns(current_data),
        context=context,
    )

    mode = _choose_query_mode(user_input, current_data)
    if mode == "followup_transform" and isinstance(current_data, dict):
        return _run_followup_analysis(user_input, chat_history, current_data, extracted_params)

    return _run_retrieval(user_input, chat_history, current_data, extracted_params)
```

### 이 코드가 하는 일

1. 질문에서 조건 추출
2. 이 질문이 새 조회인지 후속 분석인지 판단
3. 맞는 경로로 보냄

### 입력 예시

```python
run_agent(
    user_input="오늘 생산량 보여줘",
    chat_history=[],
    context={},
    current_data=None,
)
```

### 출력 예시

```python
{
    "response": "오늘 생산량을 조회했습니다.",
    "tool_results": [
        {
            "tool_name": "get_production_data",
            "data": [...],
            "summary": "총 48건, 총 생산량 126.50K"
        }
    ],
    "current_data": {...},
    "extracted_params": {
        "date": "20260325",
        "process_name": None,
        "product_name": None
    }
}
```

### 초보자 수정 포인트

- 새 조회/후속 분석 분기가 이상하다
  - `_choose_query_mode()`
- 질문 조건 추출이 이상하다
  - `resolve_required_params()`

---

## 5. `core/agent.py`의 `_choose_query_mode()`

이 함수는 “지금 질문이 새 조회인가, 후속 분석인가?”를 판단합니다.

### 실제 코드

```python
def _choose_query_mode(user_input: str, current_data: Dict[str, Any] | None) -> QueryMode:
    if _has_current_data(current_data) and not _looks_like_new_data_request(user_input):
        return "followup_transform"
    return "retrieval"
```

### 이 코드가 하는 일

- `current_data`가 있고
- 질문이 새 조회처럼 안 보이면
- 후속 분석으로 보냄

### 입력 예시 1

```python
user_input = "상위 5개만 보여줘"
current_data = {"data": [...]} 
```

### 출력 예시 1

```python
"followup_transform"
```

### 입력 예시 2

```python
user_input = "오늘 불량 보여줘"
current_data = {"data": [...]} 
```

### 출력 예시 2

```python
"retrieval"
```

### 초보자 수정 포인트

- 후속 분석이어야 하는데 새 조회로 간다
- 새 조회여야 하는데 후속 분석으로 간다

이럴 때 가장 먼저 보는 함수입니다.

---

## 6. `core/parameter_resolver.py`의 `resolve_required_params()`

이 함수는 질문에서 날짜, 공정, 제품 같은 조건을 뽑습니다.

### 실제 코드 일부

```python
def resolve_required_params(
    user_input: str,
    chat_history_text: str,
    current_data_columns: List[str],
    context: Dict[str, Any] | None = None,
) -> RequiredParams:
    today = datetime.now().strftime("%Y%m%d")
    domain_prompt = build_domain_knowledge_prompt()
    prompt = f"""You are extracting retrieval parameters ...
    """

    parsed: Dict[str, Any] = {}
    try:
        llm = get_llm()
        response = llm.invoke([...])
        parsed = _parse_json_block(_extract_text_from_response(response.content))
    except Exception:
        parsed = {}

    extracted_params: RequiredParams = {
        "date": parsed.get("date") or _fallback_date(user_input),
        "process_name": parsed.get("process"),
        "product_name": parsed.get("product_name"),
        ...
    }
    return _inherit_from_context(extracted_params, context)
```

### 이 코드가 하는 일

- LLM에게 질문 조건을 JSON으로 뽑게 함
- 실패해도 `오늘`, `어제` 정도는 규칙으로 보완
- 이전 질문 조건도 이어받음

### 입력 예시

```python
user_input = "오늘 TEST 공정 WIP 보여줘"
context = {"product_name": "LPDDR5X_8533"}
```

### 출력 예시

```python
{
    "date": "20260325",
    "process_name": ["TEST"],
    "product_name": "LPDDR5X_8533",
    "line_name": None,
    "mode": None,
    "den": None,
    "tech": None,
    "lead": None,
    "mcp_no": None,
    "group_by": None,
}
```

### 초보자 수정 포인트

- 질문에서 날짜를 자주 못 잡는다
- 공정군/제품명이 잘못 뽑힌다
- 이전 조건 승계가 이상하다

---

## 7. `core/data_tools.py`의 `pick_retrieval_tools()`

이 함수는 질문 안에서 어떤 데이터셋을 조회해야 하는지 고릅니다.

### 실제 코드 개념

이 함수는 `DATASET_REGISTRY`의 `keywords`를 보고 질문과 비교합니다.

예를 들어:

- `생산` -> `production`
- `목표` -> `target`
- `불량` -> `defect`
- `WIP` -> `wip`
- `홀드` -> `hold`

### 입력 예시

```python
"오늘 생산과 목표를 같이 보여줘"
```

### 출력 예시

```python
["production", "target"]
```

### 초보자 수정 포인트

- 어떤 질문에서 특정 데이터셋이 안 잡힌다
- 새 데이터셋 키워드를 추가하고 싶다

이 경우:
- `core/domain_knowledge.py`의 `DATASET_METADATA`
- `core/data_tools.py`의 registry

를 같이 봅니다.

---

## 8. `core/data_tools.py`의 `get_production_data()`

이 함수는 생산 데이터를 만드는 조회 함수입니다.

### 실제 코드 일부

```python
def get_production_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        base = 3000 if spec["family"] in {"ASSY_PREP", "DIE_ATTACH"} else 2200
        variation = random.uniform(0.55, 1.15)
        qty = int(base * variation)
        rows.append(
            {
                "날짜": date,
                "공정": spec["공정"],
                "공정군": spec["family"],
                "MODE": product["MODE"],
                ...
                "production": qty,
            }
        )
    rows = _apply_common_filters(rows, params)
    return {
        "success": True,
        "tool_name": "get_production_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 총 생산량 ..."
    }
```

### 이 코드가 하는 일

- 날짜를 seed로 해서 재현 가능한 mock 데이터 생성
- 공정/제품 조합으로 생산 행 생성
- 마지막에 공통 필터 적용

### 입력 예시

```python
{"date": "20260325", "process_name": ["TEST"], "product_name": None}
```

### 출력 예시

```python
{
    "success": True,
    "tool_name": "get_production_data",
    "data": [
        {
            "날짜": "20260325",
            "공정": "final_test",
            "공정군": "TEST",
            "MODE": "DDR5_6400",
            "production": 2450
        }
    ],
    "summary": "총 6건, 총 생산량 12.45K"
}
```

### 초보자 수정 포인트

- 생산 데이터 컬럼을 늘리고 싶다
- 기본 생산량 범위를 바꾸고 싶다
- 필터가 안 먹는다

---

## 9. `core/data_analysis_engine.py`의 `execute_analysis_query()`

이 함수는 후속 pandas 분석의 시작점입니다.

### 실제 코드 일부

```python
def execute_analysis_query(query_text: str, data: List[Dict[str, Any]], source_tool_name: str = "") -> Dict[str, Any]:
    if not data:
        return {"success": False, "error_message": "분석할 현재 데이터가 없습니다.", "data": []}

    columns = extract_columns(data)
    missing_dimensions = find_missing_dimensions(query_text, columns)
    if missing_dimensions:
        return _error_result(...)

    plan, analysis_logic = build_llm_plan(query_text, data)
    if plan is None:
        plan = minimal_fallback_plan(query_text, data)
        analysis_logic = "minimal_fallback"

    plan_missing_columns = validate_plan_columns(plan, columns)
    if plan_missing_columns:
        return _error_result(...)

    executed, final_plan, final_logic = _execute_with_retry(query_text, data, plan, analysis_logic)
    ...
    return _success_result(...)
```

### 이 코드가 하는 일

1. 현재 표가 있는지 확인
2. 없는 컬럼 요청인지 확인
3. LLM에게 pandas 코드 생성 요청
4. 안전 검사
5. 실행
6. 실패 시 재시도
7. 성공/실패 payload 반환

### 입력 예시

```python
query_text = "공정군별로 그룹화해서 평균 불량률을 보여줘"
data = [
    {"공정군": "TEST", "defect_rate": 1.2, "주요불량유형": "timing_fail"},
    {"공정군": "TEST", "defect_rate": 1.8, "주요불량유형": "leakage_fail"},
]
```

### 출력 예시

```python
{
    "success": True,
    "tool_name": "analyze_current_data",
    "data": [
        {"공정군": "TEST", "평균_불량율": 1.5}
    ],
    "analysis_logic": "llm_primary",
    "generated_code": "result = df.groupby(...)",
}
```

### 초보자 수정 포인트

- 후속 분석이 자주 실패한다
- 파생 컬럼이 자꾸 없는 컬럼처럼 막힌다
- 특정 질문 패턴에서 LLM이 엉뚱한 코드를 만든다

---

## 10. `core/analysis_llm.py`의 `build_llm_prompt()`

이 함수는 LLM에게 보낼 프롬프트를 만듭니다.

### 실제 코드 일부

```python
def build_llm_prompt(
    query_text: str,
    data: List[Dict[str, Any]],
    retry_error: str = "",
    previous_code: str = "",
) -> str:
    profile = dataset_profile(data)
    dataset_hints = build_dataset_specific_hints(data, query_text)
    ...
    return f"""You generate pandas code ...
    Dataset profile:
    {json.dumps(profile, ensure_ascii=False)}

    Dataset-specific column hints:
    {dataset_hints or "- No extra dataset-specific hints."}
    ...
    """
```

### 이 코드가 하는 일

- 현재 컬럼 목록
- 샘플 행
- 도메인 지식
- 데이터셋별 힌트

를 합쳐서 LLM이 더 정확한 pandas 코드를 쓰도록 돕습니다.

### 입력 예시

```python
query_text = "목표 대비 생산 달성율을 알려줘"
data = [
    {"공정": "burn_in", "production": 100, "target": 120}
]
```

### 출력 예시

문자열 프롬프트 하나를 반환합니다.

```python
"You generate pandas code for follow-up analysis ..."
```

### 초보자 수정 포인트

- 특정 질문 패턴에서 자꾸 엉뚱한 해석이 나온다
- 특정 데이터셋의 대표 컬럼을 더 강하게 알려주고 싶다

이럴 때 `build_dataset_specific_hints()`를 먼저 보는 편이 가장 쉽습니다.

---

## 11. `core/safe_code_executor.py`의 역할

이 파일은 생성된 pandas 코드를 그냥 실행하지 않고, 안전하게 실행하기 위한 파일입니다.

초보자 입장에서는 이렇게만 이해하면 됩니다.

- `LLM이 짠 코드`를 바로 돌리면 위험할 수 있음
- 그래서 허용된 코드만 실행
- 최종 결과는 꼭 `result` 변수에 담기게 강제

즉 이 파일은 “보안 + 안정성” 담당입니다.

---

## 12. 자주 보는 입력/출력 형태 요약

### `resolve_required_params()` 출력 형태

```python
{
    "date": "20260325",
    "process_name": ["TEST"],
    "product_name": "LPDDR5X_8533",
    "line_name": None,
    "mode": None,
    "den": None,
    "tech": None,
    "lead": None,
    "mcp_no": None,
    "group_by": None,
}
```

### 조회 함수 출력 형태

```python
{
    "success": True,
    "tool_name": "get_production_data",
    "data": [...],
    "summary": "총 48건, 총 생산량 126.50K"
}
```

### 후속 분석 출력 형태

```python
{
    "success": True,
    "tool_name": "analyze_current_data",
    "data": [...],
    "summary": "현재 데이터 분석 결과: 8행",
    "analysis_plan": {...},
    "analysis_logic": "llm_primary",
    "generated_code": "result = ...",
}
```

---

## 13. 초보자에게 추천하는 디버깅 순서

### 질문 조건이 잘못 뽑힌다

1. `core/parameter_resolver.py`
2. `core/domain_knowledge.py`

### 조회 데이터가 이상하다

1. `core/data_tools.py`
2. `_apply_common_filters()`

### 후속 pandas 분석이 이상하다

1. `core/data_analysis_engine.py`
2. `core/analysis_llm.py`
3. `core/analysis_helpers.py`

### 화면 표시가 이상하다

1. `ui_renderer.py`
2. `core/number_format.py`

---

## 14. 이 문서를 읽고 나서 추천 순서

1. `START_HERE.md`
2. 이 문서
3. `QUESTION_GUIDE.md`
4. `BEGINNER_ADD_GUIDE.md`
5. `TEST_QUESTIONS.md`

이 정도 순서면 초보자도 “운영”, “수정”, “확장” 흐름을 꽤 잘 따라갈 수 있습니다.
