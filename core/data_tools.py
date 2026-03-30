import random
from typing import Any, Dict, List, Optional

from .domain_knowledge import (
    AUTO_SUFFIXES,
    DATASET_METADATA,
    PROCESS_SPECS,
    PRODUCTS,
    PRODUCT_TECH_FAMILY,
    SPECIAL_PRODUCT_ALIASES,
)
from .filter_utils import normalize_text
from .number_format import format_summary_quantity


DEFECTS_BY_FAMILY = {
    "DP": ["particle", "contamination", "edge crack", "surface stain"],
    "DA": ["die shift", "die tilt", "void", "epoxy bleed", "missing die"],
    "PCO": ["chip crack", "pickup miss", "warpage", "edge chipping"],
    "DC": ["mark misread", "die crack", "orientation miss", "size mismatch"],
    "DI": ["vision fail", "foreign material", "inspection miss"],
    "DS": ["saw crack", "burr", "edge chip", "trim miss"],
    "FCB": ["bump open", "bump short", "underfill void", "warpage", "bridge"],
    "BM": ["mask miss", "offset", "contamination", "coverage fail"],
    "PC": ["plating spot", "void", "surface scratch", "color mismatch"],
    "WB": ["nsop", "lifted bond", "heel crack", "wire sweep", "short wire"],
    "QCSPC": ["inspection fail", "dimension ng", "scratch", "contamination"],
    "SAT": ["delamination", "void", "crack", "acoustic ng"],
    "PL": ["peel fail", "label miss", "surface damage"],
    "ETC": ["visual ng", "dimension ng", "trace miss"],
}


EQUIPMENT_BY_FAMILY = {
    "DP": [("DP-01", "Wet Cleaner"), ("DP-02", "Back Grinder")],
    "DA": [("DA-01", "ASM AD830"), ("DA-02", "Datacon 2200 evo")],
    "PCO": [("PCO-01", "Pick and Place"), ("PCO-02", "Optical Sorter")],
    "DC": [("DC-01", "Dicing Saw"), ("DC-02", "Vision Marker")],
    "DI": [("DI-01", "Inspection Station")],
    "DS": [("DS-01", "Sawing Station")],
    "FCB": [("FCB-01", "TC Bonder"), ("FCB-02", "Reflow Oven")],
    "BM": [("BM-01", "Ball Mount Tool")],
    "PC": [("PC-01", "Plating Tool"), ("PC-02", "Cleaning Station")],
    "WB": [("WB-01", "K&S IConn"), ("WB-02", "K&S IConn Plus")],
    "QCSPC": [("QC-01", "AOI"), ("QC-02", "3D Inspector")],
    "SAT": [("SAT-01", "SAT Tool")],
    "PL": [("PL-01", "Pack Line")],
    "ETC": [("ETC-01", "General Station")],
}


DOWNTIME_BY_FAMILY = {
    "DP": ["material hold", "chemical change", "tray feeder jam"],
    "DA": ["PM overdue", "vacuum leak", "nozzle clog", "vision align fail"],
    "PCO": ["pickup arm alarm", "vision mismatch", "tray shortage"],
    "DC": ["blade wear", "camera alarm", "setup change"],
    "DI": ["inspection recipe hold", "vision tuning"],
    "DS": ["saw blade replace", "coolant low", "alignment fail"],
    "FCB": ["reflow temp alarm", "underfill clog", "robot home error"],
    "BM": ["mask change", "alignment fail"],
    "PC": ["bath exchange", "temperature alarm"],
    "WB": ["capillary wear", "bond force drift", "material shortage"],
    "QCSPC": ["aoi calibration", "review backlog"],
    "SAT": ["scan setup hold", "review hold"],
    "PL": ["label printer fault", "tray shortage"],
    "ETC": ["operator wait", "qa hold"],
}


WIP_STATUS_BY_FAMILY = {
    "DP": ["QUEUED", "RUNNING", "WAIT_DA", "WAIT_MATERIAL", "HOLD"],
    "DA": ["QUEUED", "RUNNING", "WAIT_PCO", "WAIT_WB", "HOLD"],
    "PCO": ["QUEUED", "RUNNING", "WAIT_DC", "REWORK", "HOLD"],
    "DC": ["QUEUED", "RUNNING", "WAIT_DI", "REWORK"],
    "DI": ["QUEUED", "RUNNING", "WAIT_DS", "HOLD"],
    "DS": ["QUEUED", "RUNNING", "WAIT_FCB", "WAIT_WB", "HOLD"],
    "FCB": ["QUEUED", "RUNNING", "WAIT_BM", "WAIT_PC", "HOLD"],
    "BM": ["QUEUED", "RUNNING", "WAIT_PC", "HOLD"],
    "PC": ["QUEUED", "RUNNING", "WAIT_QCSPC", "HOLD"],
    "WB": ["QUEUED", "RUNNING", "WAIT_QCSPC", "REWORK", "HOLD"],
    "QCSPC": ["QUEUED", "RUNNING", "WAIT_SAT", "WAIT_PL"],
    "SAT": ["QUEUED", "RUNNING", "WAIT_PL", "REVIEW"],
    "PL": ["QUEUED", "RUNNING", "SHIP_READY", "COMPLETE"],
    "ETC": ["QUEUED", "RUNNING", "REVIEW", "HOLD"],
}


YIELD_FAIL_BINS_BY_FAMILY = {
    "DP": ["particle", "alignment_ng", "surface_ng"],
    "DA": ["die_shift", "void_fail", "attach_miss"],
    "PCO": ["chip_crack", "pickup_ng", "vision_ng"],
    "DC": ["mark_ng", "crack_ng", "orientation_ng"],
    "DI": ["visual_ng", "inspection_ng", "foreign_material"],
    "DS": ["burr_ng", "saw_crack", "trim_ng"],
    "FCB": ["bump_open", "bridge", "warpage"],
    "BM": ["offset_ng", "coverage_ng", "mask_ng"],
    "PC": ["surface_ng", "void_ng", "color_ng"],
    "WB": ["nsop", "wire_open", "bond_lift"],
    "QCSPC": ["inspection_ng", "scratch", "dimension_ng"],
    "SAT": ["delamination", "void", "crack"],
    "PL": ["label_ng", "packing_ng", "tray_mix"],
    "ETC": ["visual_ng", "dimension_ng", "review_ng"],
}


HOLD_REASONS_BY_FAMILY = {
    "DP": ["incoming inspection hold", "material moisture check", "wafer ID mismatch"],
    "DA": ["epoxy cure verification", "die attach void review", "recipe approval hold"],
    "PCO": ["pickup review hold", "tray setup hold"],
    "DC": ["blade wear inspection", "vision review hold"],
    "DI": ["inspection review hold", "recipe update hold"],
    "DS": ["saw review hold", "trim review hold"],
    "FCB": ["bump coplanarity review", "reflow profile hold", "underfill void review"],
    "BM": ["ball mount review", "alignment review"],
    "PC": ["plating review hold", "chemistry review hold"],
    "WB": ["bond pull outlier", "capillary replacement hold", "loop height review"],
    "QCSPC": ["inspection review", "dimension review"],
    "SAT": ["scan review hold", "customer review hold"],
    "PL": ["label verification", "shipping spec hold", "QA final release"],
    "ETC": ["operator review hold", "qa disposition hold"],
}


SCRAP_REASONS_BY_FAMILY = {
    "DP": ["incoming damage", "contamination", "moisture exposure"],
    "DA": ["die crack", "missing die", "epoxy overflow"],
    "PCO": ["pickup damage", "edge crack", "vision reject"],
    "DC": ["marking fail", "die crack", "dicing damage"],
    "DI": ["inspection reject", "foreign material"],
    "DS": ["saw crack", "burr", "trim fail"],
    "FCB": ["bump bridge", "underfill void", "warpage"],
    "BM": ["offset", "coverage fail", "mask defect"],
    "PC": ["surface damage", "void", "color fail"],
    "WB": ["wire short", "bond lift", "pad damage"],
    "QCSPC": ["inspection fail", "dimension fail", "scratch"],
    "SAT": ["acoustic fail", "crack", "void"],
    "PL": ["packing damage", "label NG", "qty mismatch"],
    "ETC": ["visual fail", "qa reject"],
}


RECIPE_BASE_BY_FAMILY = {
    "DP": {"temp_c": 115, "pressure_kpa": 70, "process_time_sec": 300},
    "DA": {"temp_c": 168, "pressure_kpa": 112, "process_time_sec": 510},
    "PCO": {"temp_c": 90, "pressure_kpa": 45, "process_time_sec": 240},
    "DC": {"temp_c": 40, "pressure_kpa": 18, "process_time_sec": 220},
    "DI": {"temp_c": 28, "pressure_kpa": 0, "process_time_sec": 180},
    "DS": {"temp_c": 35, "pressure_kpa": 20, "process_time_sec": 210},
    "FCB": {"temp_c": 238, "pressure_kpa": 126, "process_time_sec": 470},
    "BM": {"temp_c": 125, "pressure_kpa": 65, "process_time_sec": 260},
    "PC": {"temp_c": 78, "pressure_kpa": 40, "process_time_sec": 320},
    "WB": {"temp_c": 132, "pressure_kpa": 88, "process_time_sec": 360},
    "QCSPC": {"temp_c": 30, "pressure_kpa": 0, "process_time_sec": 200},
    "SAT": {"temp_c": 32, "pressure_kpa": 0, "process_time_sec": 260},
    "PL": {"temp_c": 28, "pressure_kpa": 0, "process_time_sec": 240},
    "ETC": {"temp_c": 30, "pressure_kpa": 0, "process_time_sec": 180},
}


LOT_STATUS_FLOW = ["WAIT", "RUNNING", "MOVE_OUT", "HOLD", "REWORK", "COMPLETE"]
HOLD_OWNERS = ["PE", "PIE", "QA", "Process", "Equipment", "Customer Quality"]


def _stable_seed(date_text: str, offset: int = 0) -> int:
    normalized = str(date_text or "").strip()
    if normalized.isdigit():
        return int(normalized) + offset
    return sum(ord(ch) for ch in normalized) + offset


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    text = str(value).strip()
    return [text] if text else []


def _normalize_key(value: Any) -> str:
    text = normalize_text(value)
    return text.replace("/", "").replace("-", "").replace("_", "").replace(" ", "")


def _match_exact(target: Any, allowed: Any) -> bool:
    values = _as_list(allowed)
    if not values:
        return True
    target_key = _normalize_key(target)
    return any(target_key == _normalize_key(item) for item in values)


def _match_mcp_no(target: Any, allowed: Any) -> bool:
    values = _as_list(allowed)
    if not values:
        return True
    normalized_target = normalize_text(target)
    return any(normalized_target.startswith(normalize_text(item)) for item in values)


def _is_auto_product(mcp_no: str) -> bool:
    suffix = str(mcp_no or "").strip()[-1:].upper()
    return suffix in AUTO_SUFFIXES


def _matches_product(row: Dict[str, Any], product_name: Optional[str]) -> bool:
    if not product_name:
        return True

    query = normalize_text(product_name)
    hbm_or_3ds_tokens = ["HBM_OR_3DS", "HBM/3DS", *SPECIAL_PRODUCT_ALIASES["HBM_OR_3DS"]]
    auto_product_tokens = ["AUTO_PRODUCT", "AUTO", *SPECIAL_PRODUCT_ALIASES["AUTO_PRODUCT"]]

    # These are semantic filters, not a literal product code match.
    # "HBM" or "3DS" should both mean TSV products.
    if any(normalize_text(token) in query for token in hbm_or_3ds_tokens):
        return str(row.get("TSV_DIE_TYP", "")).upper() == "TSV"

    if any(normalize_text(token) in query for token in auto_product_tokens):
        return _is_auto_product(str(row.get("MCP_NO", "")))

    aliases: List[str] = [
        str(row.get("MODE", "")),
        str(row.get("DEN", "")),
        str(row.get("TECH", "")),
        str(row.get("MCP_NO", "")),
        str(row.get("LEAD", "")),
        str(row.get("PKG_TYPE1", "")),
        str(row.get("PKG_TYPE2", "")),
        str(row.get("TSV_DIE_TYP", "")),
        f"{row.get('MODE', '')} {row.get('DEN', '')} {row.get('TECH', '')}",
    ]

    if str(row.get("TSV_DIE_TYP", "")).upper() == "TSV":
        aliases.extend(["HBM_OR_3DS", "HBM/3DS", *SPECIAL_PRODUCT_ALIASES["HBM_OR_3DS"]])

    if _is_auto_product(str(row.get("MCP_NO", ""))):
        aliases.extend(["AUTO_PRODUCT", "AUTO", *SPECIAL_PRODUCT_ALIASES["AUTO_PRODUCT"]])

    return any(query in normalize_text(value) for value in aliases if str(value).strip())


def _apply_common_filters(rows: List[Dict[str, Any]], params: Dict[str, Any]) -> List[Dict[str, Any]]:
    filtered = []
    for row in rows:
        if not _match_exact(row.get("OPER_NAME", ""), params.get("process_name")):
            continue
        if not _match_exact(row.get("OPER_NUM", ""), params.get("oper_num")):
            continue
        if not _match_exact(row.get("PKG_TYPE1", ""), params.get("pkg_type1")):
            continue
        if not _match_exact(row.get("PKG_TYPE2", ""), params.get("pkg_type2")):
            continue
        if not _match_exact(row.get("라인", ""), params.get("line_name")):
            continue
        if not _matches_product(row, params.get("product_name")):
            continue
        if not _match_exact(row.get("MODE", ""), params.get("mode")):
            continue
        if not _match_exact(row.get("DEN", ""), params.get("den")):
            continue
        if not _match_exact(row.get("TECH", ""), params.get("tech")):
            continue
        if not _match_exact(row.get("LEAD", ""), params.get("lead")):
            continue
        if not _match_mcp_no(row.get("MCP_NO", ""), params.get("mcp_no")):
            continue
        filtered.append(row)
    return filtered


def _iter_valid_process_product_pairs():
    for spec in PROCESS_SPECS:
        for product in PRODUCTS:
            if spec["family"] in PRODUCT_TECH_FAMILY.get(product["TECH"], set()):
                yield spec, product


def _make_lot_id(date: str, family: str, index: int) -> str:
    family_code = family.replace("/", "").replace("_", "")[:4]
    return f"LOT-{date[-4:]}-{family_code}-{index:03d}"


def _build_base_row(date: str, spec: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "WORK_DT": date,
        "OPER_NAME": spec["OPER_NAME"],
        "공정군": spec["family"],
        "OPER_NUM": spec["OPER_NUM"],
        "PKG_TYPE1": product["PKG_TYPE1"],
        "PKG_TYPE2": product["PKG_TYPE2"],
        "TSV_DIE_TYP": product["TSV_DIE_TYP"],
        "MODE": product["MODE"],
        "DEN": product["DEN"],
        "TECH": product["TECH"],
        "LEAD": product["LEAD"],
        "MCP_NO": product["MCP_NO"],
        "라인": spec["라인"],
    }


def _pick_equipment(family: str, process_name: str) -> tuple[str, str]:
    candidates = EQUIPMENT_BY_FAMILY.get(family) or EQUIPMENT_BY_FAMILY["ETC"]
    index = abs(hash(f"{family}:{process_name}")) % len(candidates)
    return candidates[index]


def get_production_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        base = 3200 if spec["family"] in {"DP", "DA"} else 2400
        qty = int(base * random.uniform(0.55, 1.18))
        row = _build_base_row(date, spec, product)
        row["production"] = qty
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    total = sum(int(item["production"]) for item in rows)
    return {
        "success": True,
        "tool_name": "get_production_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 총 생산량 {format_summary_quantity(total)}",
    }


def get_target_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        target = 3600 if spec["family"] in {"DP", "DA"} else 2600
        row = _build_base_row(date, spec, product)
        row["target"] = target
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    total = sum(int(item["target"]) for item in rows)
    return {
        "success": True,
        "tool_name": "get_target_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 총 목표량 {format_summary_quantity(total)}",
    }


def get_defect_rate(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 2000))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        inspection_qty = random.randint(2500, 8000)
        family = spec["family"]
        rate_floor = 0.004 if family in {"DP", "PL"} else 0.008
        rate_ceiling = 0.018 if family in {"WB", "FCB"} else 0.028
        defect_qty = int(inspection_qty * random.uniform(rate_floor, rate_ceiling))
        row = _build_base_row(date, spec, product)
        row["inspection_qty"] = inspection_qty
        row["불량수량"] = defect_qty
        row["defect_rate"] = round((defect_qty / inspection_qty) * 100, 2)
        row["주요불량유형"] = random.choice(DEFECTS_BY_FAMILY.get(family, DEFECTS_BY_FAMILY["ETC"]))
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    avg_rate = sum(float(item["defect_rate"]) for item in rows) / len(rows) if rows else 0.0
    return {
        "success": True,
        "tool_name": "get_defect_rate",
        "data": rows,
        "summary": f"총 {len(rows)}건, 평균 불량률 {avg_rate:.2f}%",
    }


def get_equipment_status(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 3000))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        equip_id, equip_name = _pick_equipment(spec["family"], spec["OPER_NAME"])
        util = round(random.uniform(62, 97), 1)
        planned = 24.0
        actual = round(planned * util / 100, 1)
        row = _build_base_row(date, spec, product)
        row["설비ID"] = equip_id
        row["설비명"] = equip_name
        row["planned_hours"] = planned
        row["actual_hours"] = actual
        row["가동률"] = util
        row["비가동사유"] = "none" if util > 90 else random.choice(DOWNTIME_BY_FAMILY.get(spec["family"], DOWNTIME_BY_FAMILY["ETC"]))
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    avg_util = sum(float(item["가동률"]) for item in rows) / len(rows) if rows else 0.0
    return {
        "success": True,
        "tool_name": "get_equipment_status",
        "data": rows,
        "summary": f"총 {len(rows)}건, 평균 가동률 {avg_util:.1f}%",
    }


def get_wip_status(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 4000))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        row = _build_base_row(date, spec, product)
        row["재공수량"] = random.randint(150, 2600)
        row["avg_wait_minutes"] = random.randint(10, 240)
        row["상태"] = random.choice(WIP_STATUS_BY_FAMILY.get(spec["family"], WIP_STATUS_BY_FAMILY["ETC"]))
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    total = sum(int(item["재공수량"]) for item in rows)
    delayed = sum(1 for item in rows if item["상태"] in {"HOLD", "REWORK", "WAIT_QA", "WAIT_MATERIAL"})
    return {
        "success": True,
        "tool_name": "get_wip_status",
        "data": rows,
        "summary": f"총 {len(rows)}건, 총 WIP {format_summary_quantity(total)} EA, 대기/보류 {delayed}건",
    }


def get_yield_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 5000))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        tested_qty = random.randint(2200, 7800)
        base_yield = 98.8 if spec["family"] in {"DP", "PL"} else 96.5
        if spec["family"] in {"WB", "FCB"}:
            base_yield = 94.5
        yield_rate = round(max(82.0, min(99.9, random.uniform(base_yield - 4.5, base_yield + 1.2))), 2)
        row = _build_base_row(date, spec, product)
        row["tested_qty"] = tested_qty
        row["pass_qty"] = int(tested_qty * yield_rate / 100)
        row["yield_rate"] = yield_rate
        row["dominant_fail_bin"] = random.choice(YIELD_FAIL_BINS_BY_FAMILY.get(spec["family"], YIELD_FAIL_BINS_BY_FAMILY["ETC"]))
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    avg_yield = sum(float(item["yield_rate"]) for item in rows) / len(rows) if rows else 0.0
    return {
        "success": True,
        "tool_name": "get_yield_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 평균 수율 {avg_yield:.2f}%",
    }


def get_hold_lot_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 6000))
    rows: List[Dict[str, Any]] = []
    for index, (spec, product) in enumerate(_iter_valid_process_product_pairs(), start=1):
        if random.random() < 0.45:
            continue
        row = _build_base_row(date, spec, product)
        row["lot_id"] = _make_lot_id(date, spec["family"], index)
        row["hold_qty"] = random.randint(80, 1800)
        row["hold_reason"] = random.choice(HOLD_REASONS_BY_FAMILY.get(spec["family"], HOLD_REASONS_BY_FAMILY["ETC"]))
        row["hold_owner"] = random.choice(HOLD_OWNERS)
        row["hold_hours"] = round(random.uniform(1.5, 42.0), 1)
        row["hold_status"] = random.choice(["OPEN", "REVIEW", "WAIT_DISPOSITION"])
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    total_hold = sum(int(item["hold_qty"]) for item in rows)
    avg_hold_hours = sum(float(item["hold_hours"]) for item in rows) / len(rows) if rows else 0.0
    return {
        "success": True,
        "tool_name": "get_hold_lot_data",
        "data": rows,
        "summary": (
            f"총 {len(rows)}건, 총 홀드 수량 {format_summary_quantity(total_hold)}, 평균 홀드 시간 {avg_hold_hours:.1f}h"
            if rows
            else "총 0건, 총 홀드 수량 0"
        ),
    }


def get_scrap_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 7000))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        input_qty = random.randint(1800, 7200)
        scrap_qty = int(input_qty * random.uniform(0.002, 0.028))
        row = _build_base_row(date, spec, product)
        row["scrap_qty"] = scrap_qty
        row["scrap_rate"] = round((scrap_qty / input_qty) * 100, 2)
        row["scrap_reason"] = random.choice(SCRAP_REASONS_BY_FAMILY.get(spec["family"], SCRAP_REASONS_BY_FAMILY["ETC"]))
        row["loss_cost_usd"] = int(scrap_qty * random.uniform(1.8, 8.5))
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    total_scrap = sum(int(item["scrap_qty"]) for item in rows)
    total_cost = sum(int(item["loss_cost_usd"]) for item in rows)
    return {
        "success": True,
        "tool_name": "get_scrap_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 총 스크랩 {format_summary_quantity(total_scrap)}, 총 손실비용 ${total_cost:,}",
    }


def get_recipe_condition_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 8000))
    rows: List[Dict[str, Any]] = []
    for spec, product in _iter_valid_process_product_pairs():
        base = RECIPE_BASE_BY_FAMILY.get(spec["family"], RECIPE_BASE_BY_FAMILY["ETC"])
        row = _build_base_row(date, spec, product)
        row["recipe_id"] = f"RC-{spec['family'][:3]}-{random.randint(10, 99)}"
        row["recipe_version"] = f"v{random.randint(1, 3)}.{random.randint(0, 9)}"
        row["temp_c"] = round(base["temp_c"] + random.uniform(-6, 6), 1)
        row["pressure_kpa"] = round(max(0, base["pressure_kpa"] + random.uniform(-12, 12)), 1)
        row["process_time_sec"] = int(base["process_time_sec"] + random.uniform(-60, 60))
        row["operator_id"] = f"OP-{random.randint(100, 999)}"
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    return {
        "success": True,
        "tool_name": "get_recipe_condition_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 공정 조건/레시피 이력 조회 완료",
    }


def get_lot_trace_data(params: Dict[str, Any]) -> Dict[str, Any]:
    date = str(params["date"])
    random.seed(_stable_seed(date, 9000))
    rows: List[Dict[str, Any]] = []
    for index, (spec, product) in enumerate(_iter_valid_process_product_pairs(), start=1):
        if random.random() < 0.35:
            continue
        row = _build_base_row(date, spec, product)
        row["lot_id"] = _make_lot_id(date, spec["family"], index)
        row["wafer_id"] = f"WF-{random.randint(1000, 9999)}"
        row["route_step"] = random.randint(3, 28)
        row["current_status"] = random.choice(LOT_STATUS_FLOW)
        row["elapsed_hours"] = round(random.uniform(2.0, 96.0), 1)
        row["next_process"] = random.choice(
            [item["OPER_NAME"] for item in PROCESS_SPECS if item["family"] == spec["family"]]
        )
        row["hold_reason"] = random.choice(HOLD_REASONS_BY_FAMILY.get(spec["family"], HOLD_REASONS_BY_FAMILY["ETC"])) if random.random() < 0.25 else "none"
        rows.append(row)
    rows = _apply_common_filters(rows, params)
    avg_elapsed = sum(float(item["elapsed_hours"]) for item in rows) / len(rows) if rows else 0.0
    return {
        "success": True,
        "tool_name": "get_lot_trace_data",
        "data": rows,
        "summary": f"총 {len(rows)}건, 평균 체류 시간 {avg_elapsed:.1f}h",
    }


DATASET_TOOL_FUNCTIONS = {
    "production": get_production_data,
    "target": get_target_data,
    "defect": get_defect_rate,
    "equipment": get_equipment_status,
    "wip": get_wip_status,
    "yield": get_yield_data,
    "hold": get_hold_lot_data,
    "scrap": get_scrap_data,
    "recipe": get_recipe_condition_data,
    "lot_trace": get_lot_trace_data,
}


DATASET_REGISTRY = {
    dataset_key: {
        "label": DATASET_METADATA[dataset_key]["label"],
        "tool_name": tool_fn.__name__,
        "tool": tool_fn,
        "keywords": DATASET_METADATA[dataset_key]["keywords"],
        "requires_date": dataset_key in {"production", "target", "defect", "equipment", "wip", "yield", "hold", "scrap", "recipe", "lot_trace"},
    }
    for dataset_key, tool_fn in DATASET_TOOL_FUNCTIONS.items()
}


RETRIEVAL_TOOL_MAP = {key: meta["tool"] for key, meta in DATASET_REGISTRY.items()}


def get_dataset_label(dataset_key: str) -> str:
    dataset_meta = DATASET_REGISTRY.get(dataset_key, {})
    return str(dataset_meta.get("label", dataset_key))


def list_available_dataset_labels() -> List[str]:
    return [str(meta.get("label", key)) for key, meta in DATASET_REGISTRY.items()]


def dataset_requires_date(dataset_key: str) -> bool:
    dataset_meta = DATASET_REGISTRY.get(dataset_key, {})
    return bool(dataset_meta.get("requires_date", False))


def pick_retrieval_tools(query_text: str) -> List[str]:
    query = normalize_text(query_text)
    selected: List[str] = []

    for dataset_key, dataset_meta in DATASET_REGISTRY.items():
        keywords = dataset_meta.get("keywords", [])
        if any(normalize_text(token) in query for token in keywords):
            selected.append(dataset_key)

    explicit_trace_tokens = ["trace", "이력", "추적", "traceability"]
    if "hold" in selected and "lot_trace" in selected and not any(normalize_text(token) in query for token in explicit_trace_tokens):
        selected = [item for item in selected if item != "lot_trace"]

    return selected


def pick_retrieval_tool(query_text: str) -> str | None:
    selected = pick_retrieval_tools(query_text)
    return selected[0] if selected else None


def execute_retrieval_tools(dataset_keys: List[str], params: Dict[str, Any]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for dataset_key in dataset_keys:
        dataset_meta = DATASET_REGISTRY.get(dataset_key)
        if not dataset_meta:
            continue

        result = dataset_meta["tool"](params)
        if isinstance(result, dict):
            result["dataset_key"] = dataset_key
            result["dataset_label"] = dataset_meta["label"]
        results.append(result)
    return results


def build_current_datasets(tool_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    datasets: Dict[str, Any] = {}
    for result in tool_results:
        dataset_key = result.get("dataset_key")
        if not dataset_key:
            continue

        rows = result.get("data", [])
        first_row = rows[0] if isinstance(rows, list) and rows else {}
        datasets[dataset_key] = {
            "label": result.get("dataset_label", get_dataset_label(str(dataset_key))),
            "tool_name": result.get("tool_name"),
            "summary": result.get("summary", ""),
            "row_count": len(rows) if isinstance(rows, list) else 0,
            "columns": list(first_row.keys()) if isinstance(first_row, dict) else [],
            "data": rows if isinstance(rows, list) else [],
        }
    return datasets
