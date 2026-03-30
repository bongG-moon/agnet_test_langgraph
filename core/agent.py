import json
import re
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Literal, TypedDict

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, START, StateGraph

from .config import SYSTEM_PROMPT, get_llm
from .data_analysis_engine import execute_analysis_query
from .data_tools import (
    build_current_datasets,
    dataset_requires_date,
    execute_retrieval_tools,
    get_dataset_label,
    list_available_dataset_labels,
    pick_retrieval_tools,
)
from .filter_utils import normalize_text
from .number_format import format_rows_for_display
from .parameter_resolver import resolve_required_params


QueryMode = Literal["retrieval", "followup_transform"]


class AgentGraphState(TypedDict, total=False):
    user_input: str
    chat_history: List[Dict[str, str]]
    context: Dict[str, Any]
    current_data: Dict[str, Any] | None
    extracted_params: Dict[str, Any]
    query_mode: QueryMode
    retrieval_keys: List[str]
    retrieval_jobs: List[Dict[str, Any]]
    result: Dict[str, Any]


APPLIED_PARAM_FIELDS = [
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
    "group_by",
]

POST_PROCESSING_KEYWORDS = [
    "상위",
    "하위",
    "정렬",
    "비교",
    "차이",
    "요약",
    "평균",
    "합계",
    "비율",
    "달성률",
    "추이",
    "순위",
    "목록",
    "list",
    "없는",
    "top",
    "rank",
    "group by",
]

KNOWN_DIMENSION_COLUMNS = [
    "WORK_DT",
    "OPER_NAME",
    "공정군",
    "라인",
    "MODE",
    "DEN",
    "TECH",
    "LEAD",
    "MCP_NO",
    "OPER_NUM",
    "PKG_TYPE1",
    "PKG_TYPE2",
    "PKG1",
    "PKG2",
    "TSV_DIE_TYP",
    "FACTORY",
    "FAMILY",
    "ORG",
]

DATE_COLUMNS = {"WORK_DT"}


def _build_recent_chat_text(chat_history: List[Dict[str, str]], max_messages: int = 6) -> str:
    if not chat_history:
        return "(이전 대화 없음)"

    lines = []
    for message in chat_history[-max_messages:]:
        content = str(message.get("content", "")).strip()
        if content:
            lines.append(f"- {message.get('role', 'unknown')}: {content}")
    return "\n".join(lines) if lines else "(이전 대화 없음)"


def _get_current_table_columns(current_data: Dict[str, Any] | None) -> List[str]:
    if not isinstance(current_data, dict):
        return []

    rows = current_data.get("data", [])
    if not isinstance(rows, list):
        return []

    columns = set()
    for row in rows:
        if isinstance(row, dict):
            columns.update(row.keys())
    return sorted(columns)


def _has_current_data(current_data: Dict[str, Any] | None) -> bool:
    return bool(isinstance(current_data, dict) and isinstance(current_data.get("data"), list) and current_data.get("data"))


def _collect_applied_params(extracted_params: Dict[str, Any]) -> Dict[str, Any]:
    return {field: extracted_params.get(field) for field in APPLIED_PARAM_FIELDS if extracted_params.get(field)}


def _attach_result_metadata(result: Dict[str, Any], extracted_params: Dict[str, Any], original_tool_name: str) -> Dict[str, Any]:
    if result.get("success"):
        result["original_tool_name"] = original_tool_name
        result["applied_params"] = _collect_applied_params(extracted_params)
    return result


def _build_unknown_retrieval_message() -> str:
    available_labels = list_available_dataset_labels()
    if not available_labels:
        return "조회 가능한 데이터셋 정보를 아직 불러오지 못했습니다. 질문 표현을 조금 더 구체적으로 바꿔 주세요."
    return "어떤 데이터를 조회할지 아직 판단하지 못했습니다. 현재 등록된 조회 대상은 " + ", ".join(available_labels) + " 입니다."


def _build_missing_date_message(retrieval_keys: List[str]) -> str:
    labels = [get_dataset_label(key) for key in retrieval_keys if dataset_requires_date(key)]
    if labels:
        return (
            "현재 요청에서는 날짜가 필요한 조회 대상이 포함되어 있습니다"
            f" ({', '.join(labels)}). "
            "해당 데이터의 기준 일자를 같이 알려주세요. 예: 오늘, 어제, 20260324"
        )
    return "이 요청은 날짜 없이도 조회할 수 있습니다."


def _mark_primary_result(tool_results: List[Dict[str, Any]], primary_index: int) -> List[Dict[str, Any]]:
    for index, result in enumerate(tool_results):
        result["display_expanded"] = index == primary_index
    return tool_results


def _mentions_grouping_expression(query_text: str) -> bool:
    return bool(re.search(r"([A-Za-z가-힣0-9_/\-]+)\s*(별|기준)", str(query_text or ""), flags=re.IGNORECASE))


def _needs_post_processing(query_text: str, extracted_params: Dict[str, Any] | None = None) -> bool:
    extracted_params = extracted_params or {}
    normalized = normalize_text(query_text)

    if extracted_params.get("group_by"):
        return True
    if _mentions_grouping_expression(query_text):
        return True
    return any(token in normalized for token in POST_PROCESSING_KEYWORDS)


def _has_explicit_date_reference(query_text: str) -> bool:
    normalized = normalize_text(query_text)
    if any(token in normalized for token in ["오늘", "어제", "today", "yesterday"]):
        return True
    return bool(re.search(r"\b20\d{6}\b", str(query_text or "")))


def _looks_like_new_data_request(query_text: str) -> bool:
    normalized = normalize_text(query_text)
    retrieval_keys = pick_retrieval_tools(query_text)
    retrieval_tokens = ["생산", "목표", "불량", "설비", "가동률", "wip", "수율", "홀드", "스크랩", "레시피", "lot", "조회"]

    if _has_explicit_date_reference(query_text):
        return True
    if len(retrieval_keys) >= 2:
        return True
    if retrieval_keys and any(token in normalized for token in ["조회", "데이터", "보여", "알려"]):
        return True
    return any(token in normalized for token in retrieval_tokens) and not _needs_post_processing(query_text)


def _prune_followup_params(user_input: str, extracted_params: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_text(user_input)
    cleaned = dict(extracted_params or {})
    filter_fields = [
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
    ]
    explicit_filter_intent = any(
        token in normalized
        for token in [
            "만",
            "필터",
            "조건",
            "공정",
            "공정번호",
            "oper",
            "pkg",
            "라인",
            "mode",
            "den",
            "tech",
            "lead",
            "mcp",
        ]
    )
    if not explicit_filter_intent:
        for field in filter_fields:
            cleaned[field] = None
    return cleaned


def _choose_query_mode(user_input: str, current_data: Dict[str, Any] | None) -> QueryMode:
    if _has_current_data(current_data) and not _looks_like_new_data_request(user_input):
        return "followup_transform"
    return "retrieval"


def _normalize_source_tag(value: str, fallback: str) -> str:
    normalized = re.sub(r"\W+", "_", str(value or "")).strip("_")
    return normalized or fallback


def _extract_date_slices(user_input: str, default_date: str | None) -> List[Dict[str, str]]:
    normalized = normalize_text(user_input)
    slices: List[Dict[str, str]] = []
    now = datetime.now()

    if "어제" in normalized or "yesterday" in normalized:
        slices.append({"label": "어제", "date": (now - timedelta(days=1)).strftime("%Y%m%d")})
    if "오늘" in normalized or "today" in normalized:
        slices.append({"label": "오늘", "date": now.strftime("%Y%m%d")})

    for explicit_date in re.findall(r"\b(20\d{6})\b", str(user_input or "")):
        if explicit_date not in {item["date"] for item in slices}:
            slices.append({"label": explicit_date, "date": explicit_date})

    if not slices and default_date:
        slices.append({"label": default_date, "date": default_date})

    return slices


def _build_retrieval_jobs(user_input: str, extracted_params: Dict[str, Any], retrieval_keys: List[str]) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    date_slices = _extract_date_slices(user_input, extracted_params.get("date"))
    use_repeated_date_slices = len(retrieval_keys) == 1 and len(date_slices) > 1

    for dataset_key in retrieval_keys:
        if use_repeated_date_slices:
            for date_slice in date_slices:
                job_params = dict(extracted_params)
                job_params["date"] = date_slice["date"]
                jobs.append(
                    {
                        "dataset_key": dataset_key,
                        "params": job_params,
                        "result_label": date_slice["label"],
                    }
                )
            continue

        job_params = dict(extracted_params)
        if len(date_slices) == 1:
            job_params["date"] = date_slices[0]["date"]
        jobs.append(
            {
                "dataset_key": dataset_key,
                "params": job_params,
                "result_label": None,
            }
        )

    return jobs


def _execute_retrieval_jobs(jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    repeated_dataset_keys = len({job["dataset_key"] for job in jobs}) != len(jobs)

    for index, job in enumerate(jobs, start=1):
        result = execute_retrieval_tools([job["dataset_key"]], job["params"])[0]
        result_label = job.get("result_label")
        if result_label:
            result["result_label"] = result_label
        if repeated_dataset_keys and result_label:
            result["dataset_key"] = f"{job['dataset_key']}__{result_label}"
            dataset_label = str(result.get("dataset_label", job["dataset_key"]))
            result["dataset_label"] = f"{dataset_label} ({result_label})"
        result["source_tag"] = _normalize_source_tag(
            result.get("result_label") or result.get("dataset_label") or result.get("tool_name", ""),
            fallback=f"source_{index}",
        )
        results.append(result)

    return results


def _should_suffix_metrics(tool_results: List[Dict[str, Any]]) -> bool:
    identifiers = [
        str(result.get("dataset_key") or result.get("tool_name") or "").split("__", 1)[0]
        for result in tool_results
    ]
    return len(identifiers) != len(set(identifiers))


def _should_exclude_date_from_join(tool_results: List[Dict[str, Any]]) -> bool:
    raw_dataset_keys = [str(result.get("dataset_key", "")).split("__", 1)[0] for result in tool_results]
    unique_dataset_keys = set(raw_dataset_keys)
    distinct_dates = {
        str(result.get("applied_params", {}).get("date", ""))
        for result in tool_results
        if result.get("applied_params", {}).get("date")
    }
    return len(unique_dataset_keys) == 1 and len(tool_results) > 1 and len(distinct_dates) > 1


def _pick_join_columns(frames: List[pd.DataFrame], exclude_date: bool) -> List[str]:
    if not frames:
        return []

    shared_columns = set(frames[0].columns)
    for frame in frames[1:]:
        shared_columns &= set(frame.columns)

    join_columns = [column for column in KNOWN_DIMENSION_COLUMNS if column in shared_columns]
    if exclude_date:
        join_columns = [column for column in join_columns if column not in DATE_COLUMNS]
    return join_columns


def _build_analysis_base_table(tool_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    prepared_frames: List[pd.DataFrame] = []
    source_names: List[str] = []
    suffix_metrics = _should_suffix_metrics(tool_results)

    for result in tool_results:
        rows = result.get("data", [])
        if not isinstance(rows, list) or not rows:
            continue

        frame = pd.DataFrame(rows)
        available_dimensions = [column for column in KNOWN_DIMENSION_COLUMNS if column in frame.columns]
        metric_columns = [column for column in frame.columns if column not in available_dimensions]
        if not available_dimensions or not metric_columns:
            continue

        if suffix_metrics:
            source_tag = str(result.get("source_tag") or "source")
            rename_map = {column: f"{column}_{source_tag}" for column in metric_columns}
            frame = frame.rename(columns=rename_map)

        prepared_frames.append(frame.copy())
        source_names.append(str(result.get("result_label") or result.get("dataset_label") or result.get("tool_name", "unknown")))

    if not prepared_frames:
        return {
            "success": False,
            "tool_name": "analysis_base_table",
            "error_message": "여러 조회 결과에서 공통 분석용 테이블을 만들지 못했습니다.",
            "data": [],
        }

    join_columns = _pick_join_columns(prepared_frames, exclude_date=_should_exclude_date_from_join(tool_results))
    if not join_columns:
        return {
            "success": False,
            "tool_name": "analysis_base_table",
            "error_message": "여러 조회 결과 사이에 공통 키 컬럼이 부족해 함께 분석할 수 없습니다.",
            "data": [],
        }

    merged_df = prepared_frames[0]
    for next_df in prepared_frames[1:]:
        next_join_columns = [column for column in join_columns if column in next_df.columns and column in merged_df.columns]
        if not next_join_columns:
            return {
                "success": False,
                "tool_name": "analysis_base_table",
                "error_message": "여러 조회 결과 사이에 공통 키 컬럼이 부족해 함께 분석할 수 없습니다.",
                "data": [],
            }
        merged_df = merged_df.merge(next_df, on=next_join_columns, how="outer")

    merged_df = merged_df.where(pd.notnull(merged_df), None)
    return {
        "success": True,
        "tool_name": "analysis_base_table",
        "data": merged_df.to_dict(orient="records"),
        "summary": f"복수 조회 분석용 테이블 생성: {', '.join(source_names)}, 총 {len(merged_df)}건",
        "source_tool_names": source_names,
        "join_columns": join_columns,
    }


def _build_multi_dataset_overview(tool_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    overview_rows = []
    for result in tool_results:
        overview_rows.append(
            {
                "데이터셋": result.get("dataset_label", result.get("dataset_key", "")),
                "행수": len(result.get("data", [])) if isinstance(result.get("data"), list) else 0,
                "요약": result.get("summary", ""),
            }
        )

    return {
        "success": True,
        "tool_name": "multi_dataset_overview",
        "data": overview_rows,
        "summary": f"복수 데이터셋 조회 완료: 총 {len(overview_rows)}개",
    }


def _format_result_preview(result: Dict[str, Any], max_rows: int = 5) -> str:
    rows = result.get("data", [])
    if not isinstance(rows, list) or not rows:
        return "없음"

    preview_rows, _ = format_rows_for_display([row for row in rows[:max_rows] if isinstance(row, dict)])
    return json.dumps(preview_rows, ensure_ascii=False, indent=2)


def _build_response_prompt(user_input: str, result: Dict[str, Any], chat_history: List[Dict[str, str]]) -> str:
    return f"""사용자에게 제조 데이터 분석 결과를 설명해 주세요.

사용자 질문:
{user_input}

최근 대화:
{_build_recent_chat_text(chat_history)}

현재 결과 요약:
{result.get('summary', '')}

현재 결과 건수:
{len(result.get('data', []))}건

현재 결과 미리보기:
{_format_result_preview(result)}

분석 계획:
{json.dumps(result.get('analysis_plan', {}), ensure_ascii=False)}

규칙:
1. 반드시 현재 결과 테이블 기준으로만 설명하세요.
2. 원본 전체 데이터를 본 것처럼 말하지 마세요.
3. 그룹화, 상위 N, 정렬 요청이면 그 결과 구조를 바로 설명하세요.
4. 미리보기의 K/M 단위를 다시 잘못 해석하지 마세요.
5. 3~5문장으로 짧고 명확하게 답하세요.
"""


def _generate_response(user_input: str, result: Dict[str, Any], chat_history: List[Dict[str, str]]) -> str:
    prompt = _build_response_prompt(user_input, result, chat_history)
    try:
        llm = get_llm()
        response = llm.invoke([SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt)])
        if isinstance(response.content, str):
            return response.content
        if isinstance(response.content, list):
            return "\n".join(str(item.get("text", "")) if isinstance(item, dict) else str(item) for item in response.content)
        return str(response.content)
    except Exception:
        return f"{result.get('summary', '결과를 확인했습니다.')} 아래 표를 함께 확인해 주세요."


def _run_analysis_after_retrieval(
    user_input: str,
    chat_history: List[Dict[str, str]],
    source_results: List[Dict[str, Any]],
    extracted_params: Dict[str, Any],
) -> Dict[str, Any] | None:
    if not source_results:
        return None
    if not _needs_post_processing(user_input, extracted_params):
        return None

    primary_source = source_results[-1]
    if not primary_source.get("success"):
        return None

    analysis_result = execute_analysis_query(
        query_text=user_input,
        data=primary_source.get("data", []),
        source_tool_name=primary_source.get("tool_name", ""),
    )
    analysis_result = _attach_result_metadata(
        analysis_result,
        extracted_params,
        primary_source.get("tool_name", ""),
    )

    if analysis_result.get("success"):
        analysis_result["current_datasets"] = build_current_datasets(source_results)
        tool_results = _mark_primary_result([*source_results, analysis_result], primary_index=len(source_results))
        return {
            "response": _generate_response(user_input, analysis_result, chat_history),
            "tool_results": tool_results,
            "current_data": analysis_result,
            "extracted_params": extracted_params,
            "awaiting_analysis_choice": True,
        }

    source_summary = _generate_response(user_input, primary_source, chat_history)
    response = (
        f"{analysis_result.get('error_message', '후처리 분석에 실패했습니다.')}\n\n"
        f"대신 조회된 원본 결과를 먼저 보여드립니다.\n\n{source_summary}"
    )
    tool_results = _mark_primary_result([*source_results, analysis_result], primary_index=len(source_results) - 1)
    return {
        "response": response,
        "tool_results": tool_results,
        "current_data": primary_source,
        "extracted_params": extracted_params,
        "awaiting_analysis_choice": True,
    }


def _run_multi_retrieval_jobs(
    user_input: str,
    chat_history: List[Dict[str, str]],
    current_data: Dict[str, Any] | None,
    jobs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    source_results = _execute_retrieval_jobs(jobs)
    for result, job in zip(source_results, jobs):
        _attach_result_metadata(result, job["params"], result.get("tool_name", ""))

    failed_results = [result for result in source_results if not result.get("success")]
    if failed_results:
        first_error = failed_results[0]
        return {
            "response": first_error.get("error_message", "복수 조회 중 오류가 발생했습니다."),
            "tool_results": source_results,
            "current_data": current_data,
            "extracted_params": jobs[0]["params"] if jobs else {},
            "awaiting_analysis_choice": bool(_has_current_data(current_data)),
        }

    current_datasets = build_current_datasets(source_results)

    if _needs_post_processing(user_input):
        analysis_base = _build_analysis_base_table(source_results)
        if not analysis_base.get("success"):
            overview_result = _build_multi_dataset_overview(source_results)
            overview_result = _attach_result_metadata(
                overview_result,
                jobs[0]["params"] if jobs else {},
                "+".join(job["dataset_key"] for job in jobs),
            )
            overview_result["current_datasets"] = current_datasets
            return {
                "response": analysis_base.get("error_message", "여러 데이터셋을 함께 분석할 공통 기준을 찾지 못했습니다."),
                "tool_results": _mark_primary_result([*source_results, overview_result], primary_index=len(source_results)),
                "current_data": overview_result,
                "extracted_params": jobs[0]["params"] if jobs else {},
                "awaiting_analysis_choice": True,
            }

        analysis_result = execute_analysis_query(
            query_text=user_input,
            data=analysis_base.get("data", []),
            source_tool_name=analysis_base.get("tool_name", ""),
        )
        analysis_result = _attach_result_metadata(
            analysis_result,
            jobs[0]["params"] if jobs else {},
            "+".join(job["dataset_key"] for job in jobs),
        )

        if analysis_result.get("success"):
            analysis_result["current_datasets"] = current_datasets
            analysis_result["analysis_base_info"] = {
                "join_columns": analysis_base.get("join_columns", []),
                "source_tool_names": analysis_base.get("source_tool_names", []),
            }
            return {
                "response": _generate_response(user_input, analysis_result, chat_history),
                "tool_results": _mark_primary_result([*source_results, analysis_result], primary_index=len(source_results)),
                "current_data": analysis_result,
                "extracted_params": jobs[0]["params"] if jobs else {},
                "awaiting_analysis_choice": True,
            }

        overview_result = _build_multi_dataset_overview(source_results)
        overview_result = _attach_result_metadata(
            overview_result,
            jobs[0]["params"] if jobs else {},
            "+".join(job["dataset_key"] for job in jobs),
        )
        overview_result["current_datasets"] = current_datasets
        return {
            "response": analysis_result.get("error_message", "복수 데이터셋 분석에 실패했습니다."),
            "tool_results": _mark_primary_result([*source_results, overview_result], primary_index=len(source_results)),
            "current_data": overview_result,
            "extracted_params": jobs[0]["params"] if jobs else {},
            "awaiting_analysis_choice": True,
        }

    overview_result = _build_multi_dataset_overview(source_results)
    overview_result = _attach_result_metadata(
        overview_result,
        jobs[0]["params"] if jobs else {},
        "+".join(job["dataset_key"] for job in jobs),
    )
    overview_result["current_datasets"] = current_datasets
    return {
        "response": _generate_response(user_input, overview_result, chat_history),
        "tool_results": _mark_primary_result([*source_results, overview_result], primary_index=len(source_results)),
        "current_data": overview_result,
        "extracted_params": jobs[0]["params"] if jobs else {},
        "awaiting_analysis_choice": True,
    }


def _run_followup_analysis(
    user_input: str,
    chat_history: List[Dict[str, str]],
    current_data: Dict[str, Any],
    extracted_params: Dict[str, Any],
) -> Dict[str, Any]:
    cleaned_params = _prune_followup_params(user_input, extracted_params)
    result = execute_analysis_query(
        query_text=user_input,
        data=current_data.get("data", []),
        source_tool_name=current_data.get("original_tool_name") or current_data.get("tool_name", ""),
    )
    result = _attach_result_metadata(
        result,
        cleaned_params,
        current_data.get("original_tool_name") or current_data.get("tool_name", ""),
    )
    tool_results = _mark_primary_result([result], primary_index=0)
    return {
        "response": _generate_response(user_input, result, chat_history) if result.get("success") else result.get("error_message", "분석에 실패했습니다."),
        "tool_results": tool_results,
        "current_data": result if result.get("success") else current_data,
        "extracted_params": cleaned_params,
        "awaiting_analysis_choice": bool(result.get("success")),
    }


def _run_retrieval(
    user_input: str,
    chat_history: List[Dict[str, str]],
    current_data: Dict[str, Any] | None,
    extracted_params: Dict[str, Any],
) -> Dict[str, Any]:
    retrieval_keys = pick_retrieval_tools(user_input)
    if not retrieval_keys:
        return {
            "response": _build_unknown_retrieval_message(),
            "tool_results": [],
            "current_data": current_data,
            "extracted_params": extracted_params,
            "awaiting_analysis_choice": bool(_has_current_data(current_data)),
        }

    jobs = _build_retrieval_jobs(user_input, extracted_params, retrieval_keys)
    missing_date_jobs = [
        job
        for job in jobs
        if dataset_requires_date(job["dataset_key"]) and not job["params"].get("date")
    ]
    if not jobs:
        return {
            "response": _build_unknown_retrieval_message(),
            "tool_results": [],
            "current_data": current_data,
            "extracted_params": extracted_params,
            "awaiting_analysis_choice": bool(_has_current_data(current_data)),
        }
    if missing_date_jobs:
        return {
            "response": _build_missing_date_message([job["dataset_key"] for job in missing_date_jobs]),
            "tool_results": [],
            "current_data": current_data,
            "extracted_params": extracted_params,
            "awaiting_analysis_choice": bool(_has_current_data(current_data)),
        }

    if len(jobs) > 1:
        return _run_multi_retrieval_jobs(user_input, chat_history, current_data, jobs)

    single_job = jobs[0]
    result = _execute_retrieval_jobs([single_job])[0]
    result = _attach_result_metadata(result, single_job["params"], result.get("tool_name", ""))

    if result.get("success"):
        post_processed = _run_analysis_after_retrieval(
            user_input=user_input,
            chat_history=chat_history,
            source_results=[result],
            extracted_params=single_job["params"],
        )
        if post_processed is not None:
            return post_processed

    tool_results = _mark_primary_result([result], primary_index=0)
    return {
        "response": _generate_response(user_input, result, chat_history) if result.get("success") else result.get("error_message", "조회에 실패했습니다."),
        "tool_results": tool_results,
        "current_data": result if result.get("success") else current_data,
        "extracted_params": single_job["params"],
        "awaiting_analysis_choice": bool(result.get("success")),
    }


def _resolve_request_node(state: AgentGraphState) -> AgentGraphState:
    chat_history = state.get("chat_history", [])
    context = state.get("context", {})
    current_data = state.get("current_data")

    extracted_params = resolve_required_params(
        user_input=state["user_input"],
        chat_history_text=_build_recent_chat_text(chat_history),
        current_data_columns=_get_current_table_columns(current_data),
        context=context,
    )
    return {
        "extracted_params": extracted_params,
        "query_mode": _choose_query_mode(state["user_input"], current_data),
    }


def _route_after_resolve(state: AgentGraphState) -> str:
    if state.get("query_mode") == "followup_transform" and isinstance(state.get("current_data"), dict):
        return "followup_analysis"
    return "plan_retrieval"


def _plan_retrieval_node(state: AgentGraphState) -> AgentGraphState:
    user_input = state["user_input"]
    current_data = state.get("current_data")
    extracted_params = state.get("extracted_params", {})
    retrieval_keys = pick_retrieval_tools(user_input)

    if not retrieval_keys:
        return {
            "retrieval_keys": [],
            "retrieval_jobs": [],
            "result": {
                "response": _build_unknown_retrieval_message(),
                "tool_results": [],
                "current_data": current_data,
                "extracted_params": extracted_params,
                "awaiting_analysis_choice": bool(_has_current_data(current_data)),
            },
        }

    jobs = _build_retrieval_jobs(user_input, extracted_params, retrieval_keys)
    if not jobs:
        return {
            "retrieval_keys": retrieval_keys,
            "retrieval_jobs": [],
            "result": {
                "response": _build_unknown_retrieval_message(),
                "tool_results": [],
                "current_data": current_data,
                "extracted_params": extracted_params,
                "awaiting_analysis_choice": bool(_has_current_data(current_data)),
            },
        }

    missing_date_jobs = [
        job
        for job in jobs
        if dataset_requires_date(job["dataset_key"]) and not job["params"].get("date")
    ]
    if missing_date_jobs:
        return {
            "retrieval_keys": retrieval_keys,
            "retrieval_jobs": jobs,
            "result": {
                "response": _build_missing_date_message([job["dataset_key"] for job in missing_date_jobs]),
                "tool_results": [],
                "current_data": current_data,
                "extracted_params": extracted_params,
                "awaiting_analysis_choice": bool(_has_current_data(current_data)),
            },
        }

    return {
        "retrieval_keys": retrieval_keys,
        "retrieval_jobs": jobs,
    }


def _route_after_retrieval_plan(state: AgentGraphState) -> str:
    if state.get("result"):
        return "finish"

    jobs = state.get("retrieval_jobs", [])
    if len(jobs) > 1:
        return "multi_retrieval"
    return "single_retrieval"


def _single_retrieval_node(state: AgentGraphState) -> AgentGraphState:
    extracted_params = state.get("extracted_params", {})
    chat_history = state.get("chat_history", [])
    current_data = state.get("current_data")
    jobs = state.get("retrieval_jobs", [])
    single_job = jobs[0]

    result = _execute_retrieval_jobs([single_job])[0]
    result = _attach_result_metadata(result, single_job["params"], result.get("tool_name", ""))

    if result.get("success"):
        post_processed = _run_analysis_after_retrieval(
            user_input=state["user_input"],
            chat_history=chat_history,
            source_results=[result],
            extracted_params=single_job["params"],
        )
        if post_processed is not None:
            return {"result": post_processed}

    tool_results = _mark_primary_result([result], primary_index=0)
    return {
        "result": {
            "response": _generate_response(state["user_input"], result, chat_history)
            if result.get("success")
            else result.get("error_message", "조회에 실패했습니다."),
            "tool_results": tool_results,
            "current_data": result if result.get("success") else current_data,
            "extracted_params": single_job["params"] or extracted_params,
            "awaiting_analysis_choice": bool(result.get("success")),
        }
    }


def _multi_retrieval_node(state: AgentGraphState) -> AgentGraphState:
    return {
        "result": _run_multi_retrieval_jobs(
            user_input=state["user_input"],
            chat_history=state.get("chat_history", []),
            current_data=state.get("current_data"),
            jobs=state.get("retrieval_jobs", []),
        )
    }


def _followup_analysis_node(state: AgentGraphState) -> AgentGraphState:
    current_data = state.get("current_data")
    if not isinstance(current_data, dict):
        return {
            "result": {
                "response": "현재 후속 분석에 사용할 데이터가 없습니다. 먼저 데이터를 조회해 주세요.",
                "tool_results": [],
                "current_data": current_data,
                "extracted_params": state.get("extracted_params", {}),
                "awaiting_analysis_choice": False,
            }
        }

    return {
        "result": _run_followup_analysis(
            user_input=state["user_input"],
            chat_history=state.get("chat_history", []),
            current_data=current_data,
            extracted_params=state.get("extracted_params", {}),
        )
    }


def _finish_node(state: AgentGraphState) -> AgentGraphState:
    result = dict(state.get("result", {}))
    if result:
        result.setdefault("execution_engine", "langgraph")
    return {"result": result}


@lru_cache(maxsize=1)
def _get_agent_graph():
    graph = StateGraph(AgentGraphState)
    graph.add_node("resolve_request", _resolve_request_node)
    graph.add_node("plan_retrieval", _plan_retrieval_node)
    graph.add_node("single_retrieval", _single_retrieval_node)
    graph.add_node("multi_retrieval", _multi_retrieval_node)
    graph.add_node("followup_analysis", _followup_analysis_node)
    graph.add_node("finish", _finish_node)

    graph.add_edge(START, "resolve_request")
    graph.add_conditional_edges(
        "resolve_request",
        _route_after_resolve,
        {
            "followup_analysis": "followup_analysis",
            "plan_retrieval": "plan_retrieval",
        },
    )
    graph.add_conditional_edges(
        "plan_retrieval",
        _route_after_retrieval_plan,
        {
            "finish": "finish",
            "single_retrieval": "single_retrieval",
            "multi_retrieval": "multi_retrieval",
        },
    )
    graph.add_edge("single_retrieval", "finish")
    graph.add_edge("multi_retrieval", "finish")
    graph.add_edge("followup_analysis", "finish")
    graph.add_edge("finish", END)
    return graph.compile()


def run_agent(
    user_input: str,
    chat_history: List[Dict[str, str]] | None = None,
    context: Dict[str, Any] | None = None,
    current_data: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    initial_state: AgentGraphState = {
        "user_input": user_input,
        "chat_history": chat_history or [],
        "context": context or {},
        "current_data": current_data if isinstance(current_data, dict) else None,
    }
    final_state = _get_agent_graph().invoke(initial_state)
    return dict(final_state.get("result", {}))
