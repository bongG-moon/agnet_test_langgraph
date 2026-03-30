# 추가와 확장 가이드

이 문서는 초보자가 새 데이터셋이나 새 도메인 지식을 추가할 때 따라할 수 있도록 만든 문서입니다.

## 1. 새 데이터셋 추가 방법

보통 아래 2곳만 수정하면 됩니다.

1. `core/data_tools.py`
2. `core/domain_knowledge.py`

### 순서

1. `core/data_tools.py`에 조회 함수 추가
2. `core/data_tools.py`의 `DATASET_TOOL_FUNCTIONS`에 등록
3. `core/domain_knowledge.py`의 `DATASET_METADATA`에 이름/키워드 추가
4. 테스트 질문 2~3개 추가

### 예시

`alarm` 데이터셋을 추가한다고 가정하면:

1. `core/data_tools.py`에 `get_alarm_data()` 추가
2. 아래처럼 registry에 연결

```python
"alarm": get_alarm_data,
```

3. `core/domain_knowledge.py`에 아래 추가

```python
"alarm": {
    "label": "알람",
    "keywords": ["알람", "alarm", "경보"],
}
```

## 2. 도메인 지식 추가 방법

도메인 용어는 먼저 `core/domain_knowledge.py`에 넣습니다.

주로 수정하는 항목:

- `PROCESS_SPECS`
- `PROCESS_GROUP_SYNONYMS`
- `PRODUCTS`
- `PRODUCT_TECH_FAMILY`
- `MODE_GROUPS`
- `DEN_GROUPS`
- `TECH_GROUPS`
- `DATASET_METADATA`

## 3. 왜 이렇게 나눠두었나

- `domain_knowledge.py`
  - 질문을 이해하기 위한 기준 사전
- `data_tools.py`
  - 실제 데이터를 반환하는 조회기

즉:
- 용어를 늘릴 때는 `domain_knowledge.py`
- 데이터를 늘릴 때는 `data_tools.py`

이렇게 생각하면 됩니다.

## 4. 함수 기준으로 어디를 고치면 되나

### 새 조회 함수를 추가할 때

- 파일: `core/data_tools.py`
- 주로 보는 부분:
  - `get_production_data()` 같은 기존 함수
  - `DATASET_TOOL_FUNCTIONS`
  - `execute_retrieval_tools()`

### 질문 키워드를 추가할 때

- 파일: `core/domain_knowledge.py`
- 주로 보는 부분:
  - `DATASET_METADATA`

### 질문 조건 추출이 이상할 때

- 파일: `core/parameter_resolver.py`
- 주로 보는 함수:
  - `resolve_required_params()`
  - `_inherit_from_context()`

### 후속 분석이 이상할 때

- 파일: `core/data_analysis_engine.py`
- 주로 보는 함수:
  - `execute_analysis_query()`
  - `_find_semantic_retry_reason()`

- 파일: `core/analysis_llm.py`
- 주로 보는 함수:
  - `build_llm_prompt()`
  - `build_dataset_specific_hints()`

## 5. 새 기능을 넣을 때 체크리스트

- 이 질문은 새 조회인가, 후속 분석인가
- 어떤 데이터셋이 필요한가
- 대표 컬럼이 무엇인가
- 테스트 질문 2~3개를 바로 만들 수 있는가

## 6. 수정 후 빠른 확인 방법

### 문법 확인

```bash
python -m py_compile app.py ui_renderer.py core\agent.py core\domain_knowledge.py core\data_tools.py
```

### 질문 확인

- `오늘 생산량 보여줘`
- `오늘 생산과 목표를 같이 보여줘`
- `공정별로 목표 대비 생산 달성율을 알려줘`
- `오늘 TEST 공정 WIP 보여줘`

## 7. 초보자에게 가장 중요한 팁

코드를 한 번에 많이 바꾸지 마세요.

가장 안전한 순서는 아래입니다.

1. 도메인 지식 추가
2. 데이터셋 함수 추가
3. registry 연결
4. 테스트 질문 실행
5. 필요하면 후속 분석 힌트 보강
