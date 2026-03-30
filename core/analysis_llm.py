import json
from typing import Any, Dict, List, Tuple

from langchain_core.messages import HumanMessage, SystemMessage

from .analysis_contracts import PreprocessPlan
from .analysis_helpers import dataset_profile
from .config import get_llm
from .domain_knowledge import build_domain_knowledge_prompt


def build_dataset_specific_hints(data: List[Dict[str, Any]], query_text: str) -> str:
    # 같은 "평균", "최빈", "대표"라는 표현이어도
    # 데이터셋마다 우선 써야 하는 컬럼이 다르므로
    # 현재 표에 실제로 있는 컬럼을 기준으로 짧은 힌트를 붙입니다.
    if not data:
        return ""

    columns = {str(key) for key in data[0].keys()}
    query = str(query_text or "")
    hints: List[str] = []

    if "yield_rate" in columns:
        hints.append("- `수율`, `평균 수율`, `최저 수율`, `최고 수율` 요청은 `yield_rate`를 우선 사용하세요.")
        hints.append("- `수율` 질문에서는 `tested_qty`, `pass_qty`를 주 metric으로 사용하지 마세요. 사용자가 직접 요청했을 때만 보조로 쓰세요.")
    if "dominant_fail_bin" in columns:
        hints.append("- `대표 불량`, `최빈 불량`, `주요 fail bin` 요청은 `dominant_fail_bin`을 우선 사용하세요.")
    if "hold_reason" in columns:
        hints.append("- `대표 hold 사유`, `최빈 hold 사유` 요청은 `hold_reason`을 우선 사용하세요.")
        hints.append("- `사유`를 요청받았을 때는 `hold_hours` 같은 시간 컬럼으로 대체하지 마세요.")
    if "lot_id" in columns:
        hints.append("- `lot 수`, `lot 개수`, `lot 건수` 요청은 `lot_id` count로 계산하세요.")
    if "hold_qty" in columns:
        hints.append("- `hold 수량` 요청은 `hold_qty` sum을 우선 사용하세요.")
    if "hold_hours" in columns:
        hints.append("- `평균 hold 시간`, `hold 시간 평균` 요청은 `hold_hours` mean을 우선 사용하세요.")
    if "avg_wait_minutes" in columns:
        hints.append("- `평균 대기시간` 요청은 `avg_wait_minutes` mean을 우선 사용하세요.")
    if "상태" in columns:
        hints.append("- `최빈 상태`, `대표 상태` 요청은 `상태`의 최빈값을 사용하세요.")
        if "lot" in query.lower() or "건수" in query or "개수" in query or "수" in query:
            hints.append("- `hold lot 수` 요청이고 `lot_id`가 없으면 `상태`가 HOLD인 행 수를 세는 방식으로 계산할 수 있습니다.")
    if "defect_rate" in columns:
        hints.append("- `불량율`, `불량률` 요청은 `defect_rate`를 우선 사용하세요.")
    if "주요불량유형" in columns:
        hints.append("- `최빈 불량 case`, `대표 불량유형` 요청은 `주요불량유형`의 최빈값을 사용하세요.")
    if "production" in columns and "target" in columns:
        hints.append("- `달성율`, `목표 대비 생산`, `achievement rate` 요청은 `production / target` 비율로 계산하세요.")
        hints.append("- `달성율`은 원본 컬럼이 아니라 파생 컬럼이므로 코드에서 직접 만들어 결과 표에 넣으세요.")

    if "yield_rate" in columns and "공정군" in columns and "OPER_NAME" in columns and ("최저 수율 공정" in query or "가장 낮은 수율 공정" in query):
        hints.append(
            "- `최저 수율 공정` 요청은 공정군별 평균 수율만 구하지 말고, 각 공정군 안에서 `yield_rate`가 가장 낮은 공정을 함께 구하세요."
        )
    if "yield_rate" in columns and "MODE" in columns and "평균 수율" in query:
        hints.append(
            "- `MODE별 평균 수율` 예시: result = df.groupby('MODE', as_index=False).agg(평균_수율=('yield_rate', 'mean')).sort_values('평균_수율', ascending=False)"
        )
    if "hold_reason" in columns and ("대표 hold 사유" in query or "최빈 hold 사유" in query):
        hints.append(
            "- `대표 hold 사유` 요청은 groupby 후 `hold_reason`의 최빈값을 별도 계산해 결과 컬럼으로 합치세요."
        )
        hints.append(
            "- `MODE별 hold lot 수와 대표 hold 사유` 예시: lot_id count와 hold_reason 최빈값을 같은 결과 표에 합치세요."
        )
    if "상태" in columns and ("최빈 상태" in query or "대표 상태" in query):
        hints.append(
            "- `최빈 상태` 요청은 groupby 후 `상태`의 최빈값을 별도 계산해 결과 컬럼으로 합치세요."
        )
    if "avg_wait_minutes" in columns and "상태" in columns and "평균 대기시간" in query and "hold lot 수" in query:
        hints.append(
            "- `평균 대기시간과 hold lot 수` 요청은 `avg_wait_minutes` mean과 `상태 == 'HOLD'` 행 수를 같은 groupby 결과에 함께 넣으세요."
        )
    if "production" in columns and "target" in columns and ("달성율" in query or "달성률" in query or "목표 대비" in query):
        hints.append(
            "- `목표 대비 생산 달성율` 예시: grouped = df.groupby('OPER_NAME', as_index=False).agg(production=('production', 'sum'), target=('target', 'sum')); grouped['달성율'] = grouped['production'] / grouped['target']; result = grouped"
        )

    return "\n".join(hints)


def extract_text_from_response(content: Any) -> str:
    # 모델 SDK가 문자열 또는 파트 리스트를 반환할 수 있으므로
    # 이후 로직이 항상 문자열만 다루도록 여기서 통일합니다.
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict) and "text" in item:
                parts.append(str(item["text"]))
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts)
    return str(content)


def extract_json_payload(text: str) -> Dict[str, Any]:
    # 모델이 JSON 앞뒤로 설명이나 코드펜스를 붙여도
    # 실제 JSON 블록만 잘라서 파싱하기 위한 함수입니다.
    cleaned = str(text or "").strip()
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0]
    elif "```" in cleaned:
        cleaned = cleaned.split("```", 1)[1].split("```", 1)[0]

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}

    try:
        return json.loads(cleaned[start : end + 1])
    except Exception:
        return {}


def build_llm_prompt(
    query_text: str,
    data: List[Dict[str, Any]],
    retry_error: str = "",
    previous_code: str = "",
) -> str:
    # 후속 분석의 핵심 프롬프트입니다.
    # 현재 컬럼, 샘플 행, 도메인 지식을 함께 주고
    # LLM이 직접 pandas 코드를 작성하도록 유도합니다.
    profile = dataset_profile(data)
    dataset_hints = build_dataset_specific_hints(data, query_text)
    retry_section = ""
    if retry_error:
        retry_section = f"""
Previous generated code failed.
Failure reason:
{retry_error}

Previous code:
{previous_code}

Write a corrected pandas transformation.
"""

    return f"""You generate pandas code for follow-up analysis on an already retrieved manufacturing dataframe.
Return JSON only.

Authority rules:
- The generated pandas code is the primary execution path.
- Do not rely on predefined templates.
- Work only on dataframe `df`.
- Always assign the final DataFrame to `result`.
- Do not import anything.
- Do not use files, network, shell, eval, exec, OS APIs, plotting, or database access.
- Use only pandas operations on existing columns.
- Never invent missing columns.
- If the user requests a column that does not exist, leave code empty and add a warning.
- Prefer concise, readable code.
- If grouping, sorting, filtering, ranking, top-N-per-group, or derived columns are needed, express them directly in code.
- If the user asks for average/mean, most common/mode, max, min, sum, count, or ratio by a group,
  use groupby + agg and create derived result columns directly in code.
- Treat Korean synonyms like "불량율" and "불량률" as the same concept.
- If a column named `defect_rate` exists and the user asks for "불량율" or "불량률",
  prefer aggregating `defect_rate` rather than `불량수량`.
- Treat expressions like "최빈 불량case", "최빈 불량 case", "대표 불량" as a request to summarize
  the most common value in column `주요불량유형` when that column exists.

Manufacturing domain hints:
{build_domain_knowledge_prompt()}

Dataset profile:
{json.dumps(profile, ensure_ascii=False)}

Dataset-specific column hints:
{dataset_hints or "- No extra dataset-specific hints."}

User question:
{query_text}
{retry_section}

Good aggregation example:
- If columns include `공정군`, `defect_rate`, `주요불량유형` and the user asks
  "공정군별로 그룹화해서 불량율이랑 최빈 불량case를 정리해줘"
  then a good code shape is:
  result = df.groupby('공정군', as_index=False).agg(
      평균_불량율=('defect_rate', 'mean'),
      최빈_불량유형=('주요불량유형', lambda s: s.mode().iloc[0] if not s.mode().empty else None)
  )

Good comparison example:
- If columns include `production` and `target` and the user asks for achievement rate by process,
  a good code shape is:
  grouped = df.groupby('OPER_NAME', as_index=False).agg(production=('production', 'sum'), target=('target', 'sum'))
  grouped['achievement_rate'] = grouped['production'] / grouped['target']
  result = grouped

Return this schema:
{{
  "intent": "short summary",
  "operations": ["groupby", "sort_values"],
  "output_columns": ["MODE", "production"],
  "group_by_columns": ["MODE"],
  "partition_by_columns": [],
  "filters": [],
  "sort_by": "production",
  "sort_order": "desc",
  "top_n": 5,
  "top_n_per_group": 3,
  "metric_column": "production",
  "warnings": [],
  "code": "result = df.copy()"
}}
"""


def build_llm_plan(
    query_text: str,
    data: List[Dict[str, Any]],
    retry_error: str = "",
    previous_code: str = "",
) -> Tuple[PreprocessPlan | None, str]:
    # LLM 응답을 "실행기와 UI가 공통으로 이해할 수 있는 계획 객체"로 변환합니다.
    try:
        llm = get_llm()
        prompt = build_llm_prompt(query_text, data, retry_error=retry_error, previous_code=previous_code)
        response = llm.invoke(
            [
                SystemMessage(content="Generate safe pandas dataframe transformation code."),
                HumanMessage(content=prompt),
            ]
        )
        parsed = extract_json_payload(extract_text_from_response(response.content))
        code = str(parsed.get("code", "") or "").strip()

        plan: PreprocessPlan = {
            "intent": str(parsed.get("intent", "current data preprocessing")).strip() or "current data preprocessing",
            "operations": [str(item).strip() for item in (parsed.get("operations") or []) if str(item).strip()],
            "output_columns": [str(item).strip() for item in (parsed.get("output_columns") or []) if str(item).strip()],
            "group_by_columns": [str(item).strip() for item in (parsed.get("group_by_columns") or []) if str(item).strip()],
            "partition_by_columns": [str(item).strip() for item in (parsed.get("partition_by_columns") or []) if str(item).strip()],
            "filters": parsed.get("filters") or [],
            "sort_by": str(parsed.get("sort_by", "")).strip(),
            "sort_order": str(parsed.get("sort_order", "")).strip() or "desc",
            "metric_column": str(parsed.get("metric_column", "")).strip(),
            "warnings": [str(item).strip() for item in (parsed.get("warnings") or []) if str(item).strip()],
            "code": code,
            "source": "llm_primary" if not retry_error else "llm_retry",
        }
        if isinstance(parsed.get("top_n"), int):
            plan["top_n"] = parsed["top_n"]
        if isinstance(parsed.get("top_n_per_group"), int):
            plan["top_n_per_group"] = parsed["top_n_per_group"]

        if not code:
            return None, "llm_empty_code"
        return plan, "llm_primary" if not retry_error else "llm_retry"
    except Exception:
        return None, "llm_failed"
