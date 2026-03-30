import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

from langchain_core.messages import HumanMessage, SystemMessage

from .analysis_contracts import RequiredParams
from .config import SYSTEM_PROMPT, get_llm
from .domain_knowledge import build_domain_knowledge_prompt
from .filter_utils import normalize_text


def _extract_text_from_response(content: Any) -> str:
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


def _parse_json_block(text: str) -> Dict[str, Any]:
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


def _inherit_from_context(extracted_params: RequiredParams, context: Dict[str, Any] | None) -> RequiredParams:
    if not isinstance(context, dict):
        return extracted_params

    if not extracted_params.get("date") and context.get("date"):
        extracted_params["date"] = context["date"]
        extracted_params["date_inherited"] = True

    for field in [
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
        if extracted_params.get(field):
            continue
        if not context.get(field):
            continue
        extracted_params[field] = context[field]
        inherited_key = (
            "process_inherited"
            if field == "process_name"
            else "oper_num_inherited"
            if field == "oper_num"
            else "pkg_type1_inherited"
            if field == "pkg_type1"
            else "pkg_type2_inherited"
            if field == "pkg_type2"
            else "product_inherited"
            if field == "product_name"
            else "line_inherited"
            if field == "line_name"
            else f"{field}_inherited"
        )
        extracted_params[inherited_key] = True

    return extracted_params


def _fallback_date(text: str) -> str | None:
    lower = str(text or "").lower()
    now = datetime.now()
    if "오늘" in lower or "today" in lower:
        return now.strftime("%Y%m%d")
    if "어제" in lower or "yesterday" in lower:
        return (now - timedelta(days=1)).strftime("%Y%m%d")
    return None


def _detect_oper_num(text: str) -> List[str] | None:
    patterns = [
        r"(?:공정번호|oper_num|oper|operation)\s*[:=]?\s*(\d{4})",
        r"(\d{4})\s*번\s*공정",
    ]
    values: List[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, str(text or ""), flags=re.IGNORECASE):
            if match not in values:
                values.append(match)
    return values or None


def _detect_pkg_values(text: str, allowed_values: List[str]) -> List[str] | None:
    normalized = normalize_text(text)
    detected: List[str] = []
    for value in allowed_values:
        if normalize_text(value) in normalized and value not in detected:
            detected.append(value)
    return detected or None


def _normalize_special_product_name(value: Any) -> str | None:
    # Keep this helper simple so the intent is easy to follow:
    # HBM or 3DS questions should be treated as the same semantic filter.
    normalized = normalize_text(value)
    if not normalized:
        return None

    hbm_or_3ds_tokens = [
        "hbm_or_3ds",
        "hbm/3ds",
        "hbm",
        "3ds",
        "hbm제품",
        "hbm자재",
        "3ds제품",
    ]
    auto_tokens = ["auto_product", "auto", "auto향", "오토향", "차량향", "automotive"]

    if any(token in normalized for token in hbm_or_3ds_tokens):
        return "HBM_OR_3DS"
    if any(token in normalized for token in auto_tokens):
        return "AUTO_PRODUCT"
    return None


def _apply_domain_overrides(extracted_params: RequiredParams, user_input: str) -> RequiredParams:
    normalized = normalize_text(user_input)
    input_requested = any(token in normalized for token in ["투입량", "input", "인풋"])

    if not extracted_params.get("process_name") and input_requested:
        extracted_params["process_name"] = ["INPUT"]

    normalized_product_name = _normalize_special_product_name(extracted_params.get("product_name"))
    if normalized_product_name:
        extracted_params["product_name"] = normalized_product_name
    elif not extracted_params.get("product_name"):
        requested_special_product = _normalize_special_product_name(user_input)
        if requested_special_product:
            extracted_params["product_name"] = requested_special_product

    if extracted_params.get("process_name") == ["INPUT"] and not input_requested:
        extracted_params["process_name"] = None

    if not extracted_params.get("oper_num"):
        detected_oper_num = _detect_oper_num(user_input)
        if detected_oper_num:
            extracted_params["oper_num"] = detected_oper_num

    if not extracted_params.get("pkg_type1"):
        detected_pkg_type1 = _detect_pkg_values(user_input, ["FCBGA", "LFBGA"])
        if detected_pkg_type1:
            extracted_params["pkg_type1"] = detected_pkg_type1

    if not extracted_params.get("pkg_type2"):
        detected_pkg_type2 = _detect_pkg_values(user_input, ["ODP", "16DP", "SDP"])
        if detected_pkg_type2:
            extracted_params["pkg_type2"] = detected_pkg_type2

    return extracted_params


def resolve_required_params(
    user_input: str,
    chat_history_text: str,
    current_data_columns: List[str],
    context: Dict[str, Any] | None = None,
) -> RequiredParams:
    today = datetime.now().strftime("%Y%m%d")
    domain_prompt = build_domain_knowledge_prompt()
    prompt = f"""You are extracting retrieval parameters for a manufacturing data assistant.
Return JSON only.

Rules:
- Extract only retrieval-safe fields and grouping hints.
- Normalize today/yesterday into YYYYMMDD.
- Use domain knowledge to expand process groups.
- If a value is not explicit, return null.

Domain knowledge:
{domain_prompt}

Recent chat:
{chat_history_text}

Available current-data columns:
{", ".join(current_data_columns) if current_data_columns else "(none)"}

Today's date:
{today}

User question:
{user_input}

Return only:
{{
  "date": "YYYYMMDD or null",
  "process": ["value"] or null,
  "oper_num": ["value"] or null,
  "pkg_type1": ["value"] or null,
  "pkg_type2": ["value"] or null,
  "product_name": "string or null",
  "line_name": "string or null",
  "mode": ["value"] or null,
  "den": ["value"] or null,
  "tech": ["value"] or null,
  "lead": "string or null",
  "mcp_no": "string or null",
  "group_by": "column or null"
}}"""

    parsed: Dict[str, Any] = {}
    try:
        llm = get_llm()
        response = llm.invoke([SystemMessage(content=SYSTEM_PROMPT), HumanMessage(content=prompt)])
        parsed = _parse_json_block(_extract_text_from_response(response.content))
    except Exception:
        parsed = {}

    extracted_params: RequiredParams = {
        "date": parsed.get("date") or _fallback_date(user_input),
        "process_name": parsed.get("process"),
        "oper_num": parsed.get("oper_num"),
        "pkg_type1": parsed.get("pkg_type1"),
        "pkg_type2": parsed.get("pkg_type2"),
        "product_name": parsed.get("product_name"),
        "line_name": parsed.get("line_name"),
        "group_by": parsed.get("group_by"),
        "metrics": [],
        "mode": parsed.get("mode"),
        "den": parsed.get("den"),
        "tech": parsed.get("tech"),
        "lead": parsed.get("lead"),
        "mcp_no": parsed.get("mcp_no"),
    }
    extracted_params = _apply_domain_overrides(extracted_params, user_input)
    return _inherit_from_context(extracted_params, context)
