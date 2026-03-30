from typing import Any, Dict, List, Set

import pandas as pd
import streamlit as st

from core.number_format import format_rows_for_display


def empty_context() -> Dict[str, Any]:
    return {
        "date": None,
        "process_name": None,
        "oper_num": None,
        "pkg_type1": None,
        "pkg_type2": None,
        "product_name": None,
        "line_name": None,
        "mode": None,
        "den": None,
        "tech": None,
        "lead": None,
        "mcp_no": None,
    }


def init_session_state() -> None:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "current_data" not in st.session_state:
        st.session_state.current_data = None
    if "context" not in st.session_state:
        st.session_state.context = empty_context()
    if "engineer_mode" not in st.session_state:
        st.session_state.engineer_mode = False


def format_display_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    formatted_rows, _ = format_rows_for_display(rows)
    return pd.DataFrame(formatted_rows)


def render_applied_params(applied_params: Dict[str, Any]) -> None:
    label_map = {
        "date": "날짜",
        "process_name": "공정",
        "oper_num": "공정번호",
        "pkg_type1": "PKG TYPE1",
        "pkg_type2": "PKG TYPE2",
        "product_name": "제품",
        "line_name": "라인",
        "mode": "MODE",
        "den": "DEN",
        "tech": "TECH",
        "lead": "LEAD",
        "mcp_no": "MCP",
        "group_by": "그룹 기준",
    }
    for key, value in applied_params.items():
        if value in (None, "", []):
            continue
        rendered = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value)
        st.markdown(f"- **{label_map.get(key, key)}**: {rendered}")


def render_context() -> None:
    context = st.session_state.get("context", {})
    active = []
    label_map = [
        ("date", "날짜"),
        ("process_name", "공정"),
        ("oper_num", "공정번호"),
        ("pkg_type1", "PKG TYPE1"),
        ("pkg_type2", "PKG TYPE2"),
        ("product_name", "제품"),
        ("line_name", "라인"),
        ("mode", "MODE"),
        ("den", "DEN"),
        ("tech", "TECH"),
        ("lead", "LEAD"),
        ("mcp_no", "MCP"),
    ]
    for field, label in label_map:
        value = context.get(field)
        if value:
            rendered = ", ".join(str(item) for item in value) if isinstance(value, list) else str(value)
            active.append(f"{label}: {rendered}")
    if active:
        st.info("현재 조회 조건 | " + " / ".join(active))


def render_analysis_summary(result: Dict[str, Any], row_count: int) -> None:
    analysis_plan = result.get("analysis_plan", {})
    transformation_summary = result.get("transformation_summary", {})
    source_label = {
        "llm_primary": "LLM이 pandas 코드를 직접 생성했습니다.",
        "llm_retry": "LLM이 오류를 반영해 pandas 코드를 다시 생성했습니다.",
        "minimal_fallback": "최소 fallback 로직으로 처리했습니다.",
    }.get(str(result.get("analysis_logic", "")).lower(), "")

    st.markdown("**분석 처리 요약**")
    if source_label:
        st.markdown(f"- **코드 생성 방식**: {source_label}")
    if analysis_plan.get("intent"):
        st.markdown(f"- **분석 의도**: {analysis_plan.get('intent')}")
    if transformation_summary.get("group_by_columns"):
        st.markdown(f"- **그룹 기준**: {', '.join(transformation_summary.get('group_by_columns', []))}")
    if transformation_summary.get("metric_column"):
        st.markdown(f"- **핵심 지표**: {transformation_summary.get('metric_column')}")
    if transformation_summary.get("sort_by"):
        st.markdown(
            f"- **정렬**: {transformation_summary.get('sort_by')} ({transformation_summary.get('sort_order', 'desc')})"
        )
    if transformation_summary.get("top_n"):
        st.markdown(f"- **상위 N**: {transformation_summary.get('top_n')}")
    if transformation_summary.get("top_n_per_group"):
        st.markdown(f"- **그룹별 상위 N**: {transformation_summary.get('top_n_per_group')}")
    if transformation_summary.get("input_row_count") is not None:
        st.markdown(
            f"- **행 변화**: {transformation_summary.get('input_row_count')}건 -> {transformation_summary.get('output_row_count', row_count)}건"
        )
    st.markdown(f"- **결과 행수**: {row_count}건")

    analysis_base_info = result.get("analysis_base_info", {})
    if analysis_base_info.get("source_tool_names"):
        st.markdown(f"- **결합 원본**: {', '.join(analysis_base_info.get('source_tool_names', []))}")
    if analysis_base_info.get("join_columns"):
        st.markdown(f"- **결합 키**: {', '.join(analysis_base_info.get('join_columns', []))}")


def _get_expanded_indexes(tool_results: List[Dict[str, Any]]) -> Set[int]:
    expanded_indexes = {
        index
        for index, result in enumerate(tool_results)
        if result.get("success") and result.get("display_expanded") is True
    }
    if expanded_indexes:
        return expanded_indexes

    for index in range(len(tool_results) - 1, -1, -1):
        if tool_results[index].get("success"):
            return {index}
    return set()


def _build_result_title(result: Dict[str, Any]) -> str:
    tool_name = str(result.get("tool_name", "result"))
    summary = str(result.get("summary", "")).strip()
    if summary:
        return f"{tool_name} | {summary}"
    return tool_name


def render_tool_results(tool_results: List[Dict[str, Any]], engineer_mode: bool = False) -> None:
    expanded_indexes = _get_expanded_indexes(tool_results)

    for index, result in enumerate(tool_results):
        if not result.get("success"):
            st.error(result.get("error_message", "오류가 발생했습니다."))
            continue

        title = _build_result_title(result)
        with st.expander(title, expanded=index in expanded_indexes):
            data = result.get("data", [])
            if data:
                st.dataframe(format_display_dataframe(data), width="stretch", hide_index=True)
            else:
                st.caption("표시할 데이터가 없습니다.")

            applied_params = result.get("applied_params", {})
            if applied_params:
                st.markdown("**이번 요청에 반영된 조건**")
                render_applied_params(applied_params)

            if engineer_mode and result.get("tool_name") == "analyze_current_data":
                render_analysis_summary(result, len(data))

                generated_code = str(result.get("generated_code", "")).strip()
                if generated_code:
                    st.markdown("**생성된 pandas 코드**")
                    st.code(generated_code, language="python")


def sync_context(extracted_params: Dict[str, Any]) -> None:
    for field in [
        "date",
        "process_name",
        "oper_num",
        "pkg_type1",
        "pkg_type2",
        "product_name",
        "line_name",
        "mode",
        "den",
        "tech",
        "lead",
        "mcp_no",
    ]:
        value = extracted_params.get(field)
        if value:
            st.session_state.context[field] = value
