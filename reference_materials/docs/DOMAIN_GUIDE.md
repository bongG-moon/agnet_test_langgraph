# 도메인 가이드

이 문서는 이 서비스가 이해하는 제조 도메인 용어를 정리한 문서입니다.

## 1. 공정군

현재 주요 공정군은 아래와 같습니다.

- `ASSY_PREP`
- `DIE_ATTACH`
- `WIRE_BOND`
- `FLIP_CHIP`
- `MOLD`
- `SINGULATION`
- `TEST`
- `PACK_OUT`

질문에서 공정군을 말하면, 시스템은 이 값을 기준으로 데이터를 좁힙니다.

## 2. 제품/기술 관련 키

### 자주 나오는 제품 예시

- `DDR5_6400`
- `DDR5_5600`
- `LPDDR5X_8533`
- `UFS3.1`
- `PMIC`
- `CIS`

### 자주 나오는 제품 속성

- `MODE`
- `DEN`
- `TECH`
- `LEAD`
- `MCP_NO`

이 값들은 질문 조건 추출과 후속 분석에서 모두 중요합니다.

## 3. 데이터셋별 대표 컬럼

### 생산

- `production`

### 목표

- `target`

### 불량

- `inspection_qty`
- `불량수량`
- `defect_rate`
- `주요불량유형`

### 설비

- `planned_hours`
- `actual_hours`
- `가동률`

### WIP

- `재공수량`
- `avg_wait_minutes`
- `상태`

### 수율

- `tested_qty`
- `pass_qty`
- `yield_rate`
- `dominant_fail_bin`

### 홀드

- `lot_id`
- `hold_qty`
- `hold_reason`
- `hold_owner`
- `hold_hours`

### 스크랩

- `scrap_qty`
- `scrap_rate`
- `loss_cost_usd`

### 레시피

- `recipe_id`
- `temp_c`
- `pressure_kpa`
- `process_time_sec`

### LOT 이력

- `lot_id`
- `route_step`
- `current_status`
- `elapsed_hours`

## 4. 컬럼명을 볼 때 기억할 점

이 프로젝트는 한글 컬럼과 영문 컬럼이 같이 있습니다.

- 식별용 정보는 한글이 많습니다.
  - `공정`, `공정군`, `라인`, `상태`
- 계산용 정보는 영문이 많습니다.
  - `production`, `target`, `defect_rate`, `yield_rate`

이 구조는 pandas 코드 작성과 화면 가독성을 둘 다 맞추기 위한 것입니다.

## 5. 초보자에게 중요한 원칙

- 새 용어를 추가할 때는 먼저 `core/domain_knowledge.py`를 봅니다.
- 새 데이터셋을 추가할 때는 “이 데이터셋이 어떤 질문에 쓰이는지”를 먼저 생각합니다.
- 질문과 컬럼이 연결되지 않으면 LLM도 잘못 해석할 수 있습니다.

예:

- `대표 hold 사유` -> `hold_reason`
- `평균 대기시간` -> `avg_wait_minutes`
- `목표 대비 달성율` -> `production / target`

## 6. 코드에서 도메인 지식이 실제로 쓰이는 위치

### `core/domain_knowledge.py`

이 파일이 기준 사전입니다.

중요한 항목:

- `PROCESS_SPECS`
  - 공정과 공정군 정보
- `PRODUCTS`
  - 제품 후보
- `PRODUCT_TECH_FAMILY`
  - 제품과 공정군의 연결
- `DATASET_METADATA`
  - 데이터셋 이름, 라벨, 키워드

### `core/data_tools.py`

도메인 사전을 읽어서 실제 mock 데이터를 만듭니다.

중요한 부분:

- `_iter_valid_process_product_pairs()`
  - 가능한 공정/제품 조합 생성
- `_apply_common_filters()`
  - 공정, 제품, MODE, DEN, TECH 조건 공통 적용
- `DATASET_TOOL_FUNCTIONS`
  - 데이터셋별 조회 함수 등록

### `core/parameter_resolver.py`

도메인 사전을 프롬프트에 넣고 질문 조건을 추출합니다.

중요한 함수:

- `resolve_required_params()`
- `_inherit_from_context()`

즉, 도메인 지식은 단순 설명용이 아니라

1. 질문 해석
2. 데이터 생성
3. 필터 적용

이 세 곳에서 실제로 사용됩니다.
