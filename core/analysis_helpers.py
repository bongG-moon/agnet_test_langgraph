import re
from typing import Any, Dict, List

from .analysis_contracts import DatasetProfile, PreprocessPlan
from .filter_utils import normalize_text


DIMENSION_ALIAS_MAP = {
    "WORK_DT": {"WORK_DT", "일자", "날짜", "date"},
    "OPER_NAME": {"OPER_NAME", "공정", "process"},
    "공정군": {"공정군", "process family", "family"},
    "라인": {"라인", "line"},
    "MODE": {"mode", "모드", "제품"},
    "DEN": {"den", "density", "용량"},
    "TECH": {"tech", "기술"},
    "LEAD": {"lead"},
    "MCP_NO": {"mcp", "mcp_no"},
    "OPER_NUM": {"oper_num", "oper", "공정번호", "operation"},
    "PKG_TYPE1": {"pkg type1", "pkg_type1", "pkg1"},
    "PKG_TYPE2": {"pkg type2", "pkg_type2", "pkg2", "stack"},
    "PKG1": {"pkg1"},
    "PKG2": {"pkg2"},
    "TSV_DIE_TYP": {"tsv_die_typ", "tsv", "hbm", "3ds"},
    "FACTORY": {"factory"},
    "FAMILY": {"family"},
    "ORG": {"org"},
    "상태": {"상태", "status"},
    "주요불량유형": {"주요불량유형", "불량유형", "defect type"},
}

IGNORED_DIMENSION_TOKENS = {
    "오늘",
    "어제",
    "기준",
    "비교",
    "결과",
    "데이터",
    "생산량",
    "재공",
    "세부",
    "목록",
    "list",
    "show",
}


def extract_columns(data: List[Dict[str, Any]]) -> List[str]:
    columns: List[str] = []
    for row in data:
        for key in row.keys():
            name = str(key)
            if name not in columns:
                columns.append(name)
    return columns


def dataset_profile(data: List[Dict[str, Any]]) -> DatasetProfile:
    return {
        "columns": extract_columns(data),
        "row_count": len(data),
        "sample_rows": list(data[:3]),
    }


def find_metric_column(columns: List[str], query_text: str) -> str:
    normalized = normalize_text(query_text)
    candidates = ["production", "target", "defect_rate", "불량수량", "가동률", "재공수량"]

    for candidate in candidates:
        if normalize_text(candidate) in normalized and candidate in columns:
            return candidate

    for candidate in candidates:
        if candidate in columns:
            return candidate

    return columns[-1]


def _resolve_requested_column(token: str, columns: List[str]) -> str | None:
    normalized_token = normalize_text(token)
    if not normalized_token or normalized_token in {normalize_text(item) for item in IGNORED_DIMENSION_TOKENS}:
        return None

    for canonical_name, aliases in DIMENSION_ALIAS_MAP.items():
        alias_candidates = {canonical_name, *aliases}
        if any(normalized_token == normalize_text(alias) for alias in alias_candidates):
            return canonical_name

    for column in columns:
        if normalized_token == normalize_text(column):
            return column

    return None


def find_requested_dimensions(query_text: str, columns: List[str]) -> List[str]:
    normalized_query = normalize_text(query_text)
    requested: List[str] = []

    for canonical_name, aliases in DIMENSION_ALIAS_MAP.items():
        if any(normalize_text(alias) in normalized_query for alias in {canonical_name, *aliases}):
            requested.append(canonical_name)

    for pattern in [r"([A-Za-z가-힣0-9_/\-]+)\s*별로", r"([A-Za-z가-힣0-9_/\-]+)\s*기준"]:
        for raw_token in re.findall(pattern, query_text):
            token = str(raw_token).strip()
            resolved_name = _resolve_requested_column(token, columns)
            if resolved_name:
                requested.append(resolved_name)

    return list(dict.fromkeys(requested))


def find_missing_dimensions(query_text: str, columns: List[str]) -> List[str]:
    available = set(columns)
    requested = find_requested_dimensions(query_text, columns)
    return [column for column in requested if column not in available]


def format_missing_column_message(missing_columns: List[str], columns: List[str]) -> str:
    clean_columns = [column for column in columns if not (column.endswith("_x") or column.endswith("_y"))]
    preview_columns = clean_columns[:12] if clean_columns else columns[:12]
    available_preview = ", ".join(preview_columns)
    missing_preview = ", ".join(missing_columns)

    suffix_note = ""
    if any(column.endswith("_x") or column.endswith("_y") for column in columns):
        suffix_note = " `_x`/`_y`는 병합 중 같은 이름 컬럼이 겹칠 때 임시로 붙는 접미사입니다."

    return (
        f"요청하신 컬럼(조건) `{missing_preview}`은 현재 결과 테이블에 없습니다. "
        f"현재 사용할 수 있는 주요 컬럼은 `{available_preview}` 입니다.{suffix_note}"
    )


def parse_top_n(text: str, default: int = 5) -> int:
    match = re.search(r"(\d+)", str(text or ""))
    if match:
        return max(1, min(50, int(match.group(1))))
    return default


def minimal_fallback_plan(query_text: str, data: List[Dict[str, Any]]) -> PreprocessPlan:
    columns = extract_columns(data)
    metric_column = find_metric_column(columns, query_text)
    sort_order = "asc" if any(token in normalize_text(query_text) for token in ["하위", "최소", "낮은"]) else "desc"
    top_n = parse_top_n(query_text, default=5)
    return {
        "intent": "기본 정렬 fallback",
        "operations": ["sort_values", "head"],
        "output_columns": columns,
        "sort_by": metric_column,
        "sort_order": sort_order,
        "top_n": top_n,
        "metric_column": metric_column,
        "warnings": ["LLM 코드 생성에 실패해 최소 fallback 로직을 사용했습니다."],
        "source": "fallback",
        "code": (
            f"result = df.sort_values(by={metric_column!r}, "
            f"ascending={str(sort_order == 'asc')}).head({top_n})"
        ),
    }


def extract_derived_columns_from_code(code: str) -> List[str]:
    derived_columns: List[str] = []
    patterns = [
        r"result\[['\"]([^'\"]+)['\"]\]\s*=",
        r"df\[['\"]([^'\"]+)['\"]\]\s*=",
        r"[A-Za-z_][A-Za-z0-9_]*\[['\"]([^'\"]+)['\"]\]\s*=",
        r"([A-Za-z가-힣0-9_]+)\s*=\s*\(\s*['\"][^'\"]+['\"]\s*,",
        r"rename\s*\(\s*columns\s*=\s*\{[^}]*['\"][^'\"]+['\"]\s*:\s*['\"]([^'\"]+)['\"]",
    ]

    for pattern in patterns:
        for match in re.findall(pattern, str(code or "")):
            column_name = str(match).strip()
            if column_name and column_name not in derived_columns:
                derived_columns.append(column_name)

    return derived_columns


def validate_plan_columns(plan: PreprocessPlan, columns: List[str]) -> List[str]:
    derived_columns = extract_derived_columns_from_code(str(plan.get("code", "")))
    allowed_columns = set(columns) | set(derived_columns)
    required_columns: List[str] = []

    for field_name in ["group_by_columns", "partition_by_columns"]:
        for column in plan.get(field_name, []) or []:
            if column is None:
                continue
            column_name = str(column).strip()
            if column_name and column_name.lower() != "none":
                required_columns.append(column_name)

    for field_name in ["sort_by", "metric_column"]:
        raw_value = plan.get(field_name, "")
        if raw_value is None:
            continue
        column_name = str(raw_value).strip()
        if column_name and column_name.lower() != "none" and column_name not in allowed_columns:
            required_columns.append(column_name)

    unique_required = list(dict.fromkeys(required_columns))
    return [column for column in unique_required if column not in allowed_columns]


def build_transformation_summary(
    plan: PreprocessPlan,
    input_rows: int,
    output_rows: int,
    analysis_logic: str,
) -> Dict[str, Any]:
    return {
        "analysis_logic": analysis_logic,
        "input_row_count": input_rows,
        "output_row_count": output_rows,
        "group_by_columns": plan.get("group_by_columns", []),
        "partition_by_columns": plan.get("partition_by_columns", []),
        "metric_column": plan.get("metric_column", ""),
        "sort_by": plan.get("sort_by", ""),
        "sort_order": plan.get("sort_order", ""),
        "top_n": plan.get("top_n"),
        "top_n_per_group": plan.get("top_n_per_group"),
        "output_columns": plan.get("output_columns", []),
        "warnings": plan.get("warnings", []),
    }
