# LangGraph 구조 안내

이 프로젝트는 기존 Python 함수형 흐름을 유지하면서, 내부 실행 순서만 `LangGraph`의 `StateGraph`로 옮긴 버전입니다.

## 왜 이렇게 바꿨나

- 기존 동작을 크게 깨지 않고 구조를 단계별로 분리하기 쉽습니다.
- 조회, 후속 분석, 다중 조회 비교 같은 흐름을 노드 단위로 나눠 확장할 수 있습니다.
- 나중에 사람 검토 노드, DB 조회 노드, 검증 노드를 더 붙이기 편합니다.

## 현재 그래프 흐름

`run_agent()`가 호출되면 내부에서 아래 순서로 그래프가 실행됩니다.

1. `resolve_request`
2. `followup_analysis` 또는 `plan_retrieval`
3. `single_retrieval` 또는 `multi_retrieval`
4. `finish`

## 노드별 역할

- `resolve_request`
  - 질문에서 날짜, 공정, MODE 같은 조건을 추출합니다.
  - 현재 질문이 새 데이터 조회인지, 현재 표에 대한 후속 분석인지 판단합니다.

- `plan_retrieval`
  - 어떤 데이터셋을 조회할지 고릅니다.
  - 날짜가 꼭 필요한 데이터인지 확인합니다.
  - 여러 날짜를 비교해야 하면 조회 job을 여러 개로 쪼갭니다.

- `single_retrieval`
  - 조회 1건을 실행합니다.
  - 질문에 그룹화/비교/정렬 의도가 있으면 바로 pandas 후처리까지 이어갑니다.

- `multi_retrieval`
  - 생산/목표 같이 여러 데이터셋을 함께 조회하거나,
    어제/오늘처럼 같은 데이터셋을 여러 번 조회하는 흐름을 처리합니다.
  - 필요하면 공통 분석용 테이블을 만들고 후처리까지 실행합니다.

- `followup_analysis`
  - 이미 화면에 있는 `current_data`를 다시 가공합니다.
  - 예: 상위 5개, MODE별 집계, 없는 제품 목록

- `finish`
  - 최종 응답 형식을 정리합니다.
  - 현재는 결과에 `execution_engine="langgraph"` 표시도 넣습니다.

## 초보자 기준으로 보면 좋은 파일

- `app.py`
  - Streamlit 화면 시작점

- `core/agent.py`
  - LangGraph 상태, 노드, 라우팅, 최종 실행 진입점

- `core/data_tools.py`
  - 실제 조회 함수 모음

- `core/data_analysis_engine.py`
  - 후속 pandas 코드 실행기

## 확장 포인트

- 실제 Oracle 조회 노드 분리
- 검증 전용 노드 추가
- 응답 생성 전 품질 점검 노드 추가
- 사람 승인 노드 추가
