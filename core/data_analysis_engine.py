from typing import Any, Dict, List

from .analysis_llm import build_llm_plan
from .analysis_helpers import (
    build_transformation_summary,
    extract_columns,
    find_missing_dimensions,
    format_missing_column_message,
    minimal_fallback_plan,
    validate_plan_columns,
)
from .safe_code_executor import execute_safe_dataframe_code


def _find_semantic_retry_reason(query_text: str, columns: List[str], code: str) -> str:
    query = str(query_text or "")
    code_text = str(code or "")
    available = set(columns)
    lower_query = query.lower()

    if "hold_reason" in available and ("대표 hold 사유" in query or "최다 hold 사유" in query):
        if "hold_reason" not in code_text:
            return "The previous code did not use `hold_reason` even though the user explicitly asked for representative hold reason."

    if "avg_wait_minutes" in available and "상태" in available and "평균 대기시간" in query and "hold lot 수" in query:
        has_wait_metric = "avg_wait_minutes" in code_text
        has_hold_count = "HOLD" in code_text or "hold_lot" in code_text or "상태" in code_text
        if not (has_wait_metric and has_hold_count):
            return "The previous code did not include both average wait time and hold lot count in the same grouped result."

    if "production" in available and "target" in available and ("달성율" in query or "달성률" in query or "목표 대비" in query):
        has_production = "production" in code_text
        has_target = "target" in code_text
        has_ratio = "/" in code_text or "달성율" in code_text or "달성률" in code_text or "achievement" in code_text.lower()
        if not (has_production and has_target and has_ratio):
            return "The previous code did not calculate achievement rate from both `production` and `target`."

    if ("없는" in query or "목록" in query or "list" in lower_query) and not any(
        token in code_text for token in ["isna(", "isnull(", "notna(", "notnull("]
    ):
        return "The previous code did not apply missing-value filtering even though the user asked for rows that exist in one dataset but not another."

    return ""


def _execute_plan(plan: Dict[str, Any], data: List[Dict[str, Any]]) -> Dict[str, Any]:
    return execute_safe_dataframe_code(str(plan.get("code", "")).strip(), data)


def _success_result(
    plan: Dict[str, Any],
    analysis_logic: str,
    result_rows: List[Dict[str, Any]],
    source_tool_name: str,
    input_rows: int,
) -> Dict[str, Any]:
    return {
        "success": True,
        "tool_name": "analyze_current_data",
        "data": result_rows,
        "summary": f"현재 데이터 분석 결과: {len(result_rows)}건",
        "analysis_plan": plan,
        "analysis_logic": analysis_logic,
        "generated_code": plan.get("code", ""),
        "source_tool_name": source_tool_name,
        "transformation_summary": build_transformation_summary(
            plan,
            input_rows=input_rows,
            output_rows=len(result_rows),
            analysis_logic=analysis_logic,
        ),
    }


def _error_result(
    error_message: str,
    columns: List[str],
    plan: Dict[str, Any] | None = None,
    analysis_logic: str | None = None,
    missing_columns: List[str] | None = None,
) -> Dict[str, Any]:
    return {
        "success": False,
        "tool_name": "analyze_current_data",
        "error_message": error_message,
        "data": [],
        "analysis_plan": plan,
        "analysis_logic": analysis_logic,
        "generated_code": (plan or {}).get("code", ""),
        "missing_columns": missing_columns or [],
        "available_columns": columns,
    }


def _execute_with_retry(query_text: str, data: List[Dict[str, Any]], plan: Dict[str, Any], analysis_logic: str):
    executed = _execute_plan(plan, data)
    if executed.get("success") or str(plan.get("source")) != "llm_primary":
        return executed, plan, analysis_logic

    retry_plan, retry_logic = build_llm_plan(
        query_text,
        data,
        retry_error=str(executed.get("error_message", "")),
        previous_code=str(plan.get("code", "")),
    )
    if retry_plan is None:
        return executed, plan, analysis_logic

    retry_executed = _execute_plan(retry_plan, data)
    return retry_executed, retry_plan, retry_logic


def execute_analysis_query(query_text: str, data: List[Dict[str, Any]], source_tool_name: str = "") -> Dict[str, Any]:
    if not data:
        return {
            "success": False,
            "tool_name": "analyze_current_data",
            "error_message": "분석할 현재 데이터가 없습니다.",
            "data": [],
        }

    columns = extract_columns(data)
    missing_dimensions = find_missing_dimensions(query_text, columns)
    if missing_dimensions:
        return _error_result(
            format_missing_column_message(missing_dimensions, columns),
            columns,
            missing_columns=missing_dimensions,
        )

    plan, analysis_logic = build_llm_plan(query_text, data)
    if plan is None:
        plan = minimal_fallback_plan(query_text, data)
        analysis_logic = "minimal_fallback"
    else:
        semantic_retry_reason = _find_semantic_retry_reason(query_text, columns, str(plan.get("code", "")))
        if semantic_retry_reason:
            retry_plan, retry_logic = build_llm_plan(
                query_text,
                data,
                retry_error=semantic_retry_reason,
                previous_code=str(plan.get("code", "")),
            )
            if retry_plan is not None:
                plan = retry_plan
                analysis_logic = retry_logic

    plan_missing_columns = validate_plan_columns(plan, columns)
    if plan_missing_columns:
        return _error_result(
            format_missing_column_message(plan_missing_columns, columns),
            columns,
            plan=plan,
            analysis_logic=analysis_logic,
            missing_columns=plan_missing_columns,
        )

    executed, final_plan, final_logic = _execute_with_retry(query_text, data, plan, analysis_logic)
    if not executed.get("success"):
        error_message = str(executed.get("error_message", "분석 코드 실행에 실패했습니다."))
        if "KeyError" in error_message:
            missing_from_error = plan_missing_columns or missing_dimensions or ["요청 컬럼"]
            error_message = format_missing_column_message(missing_from_error, columns)

        return _error_result(
            error_message,
            columns,
            plan=final_plan,
            analysis_logic=final_logic,
            missing_columns=plan_missing_columns,
        )

    result_rows = executed.get("data", [])
    return _success_result(final_plan, final_logic, result_rows, source_tool_name, len(data))
