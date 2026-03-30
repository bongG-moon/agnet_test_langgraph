from typing import Any, Dict, List, Tuple


QUANTITY_KEYWORDS = {
    "qty",
    "quantity",
    "count",
    "production",
    "target",
    "inspection",
    "수량",
    "재공",
}

NON_QUANTITY_KEYWORDS = {
    "rate",
    "ratio",
    "percent",
    "minutes",
    "minute",
    "hour",
    "hours",
    "가동률",
    "불량률",
    "대기시간",
}

REDUNDANT_UNIT_COLUMNS = {"단위"}


def is_quantity_column(column_name: str) -> bool:
    name = str(column_name or "").strip().lower()
    if not name:
        return False
    if any(keyword in name for keyword in NON_QUANTITY_KEYWORDS):
        return False
    return any(keyword in name for keyword in QUANTITY_KEYWORDS)


def pick_quantity_unit(values: List[Any]) -> str | None:
    numeric_values = [abs(float(value)) for value in values if isinstance(value, (int, float)) and not isinstance(value, bool)]
    if not numeric_values:
        return None
    max_abs = max(numeric_values)
    if max_abs >= 1_000_000:
        return "M"
    if max_abs >= 10_000:
        return "K"
    return None


def format_number_by_unit(value: Any, unit: str | None) -> Any:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return value
    if unit == "K":
        return f"{value / 1_000:,.2f}K"
    if unit == "M":
        return f"{value / 1_000_000:,.2f}M"
    if float(value).is_integer():
        return f"{int(value):,}"
    return f"{float(value):,.2f}"


def build_quantity_unit_map(rows: List[Dict[str, Any]]) -> Dict[str, str | None]:
    unit_map: Dict[str, str | None] = {}
    if not rows:
        return unit_map

    columns = set()
    for row in rows:
        if isinstance(row, dict):
            columns.update(row.keys())

    for column in columns:
        if not is_quantity_column(column):
            continue
        values = [row.get(column) for row in rows if isinstance(row, dict)]
        unit_map[str(column)] = pick_quantity_unit(values)
    return unit_map


def format_rows_with_quantity_units(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str | None]]:
    unit_map = build_quantity_unit_map(rows)
    formatted_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        formatted_row: Dict[str, Any] = {}
        for key, value in row.items():
            formatted_row[str(key)] = format_number_by_unit(value, unit_map.get(str(key)))
        formatted_rows.append(formatted_row)
    return formatted_rows, unit_map


def format_rows_for_display(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str | None]]:
    formatted_rows, unit_map = format_rows_with_quantity_units(rows)
    display_rows: List[Dict[str, Any]] = []

    for row in formatted_rows:
        display_row: Dict[str, Any] = {}
        for key, value in row.items():
            if key in REDUNDANT_UNIT_COLUMNS and any(unit_map.get(column) for column in unit_map):
                continue
            renamed_key = f"{key} ({unit_map[key]})" if unit_map.get(key) else key
            display_row[renamed_key] = value
        display_rows.append(display_row)

    return display_rows, unit_map


def format_summary_quantity(value: float | int) -> str:
    unit = pick_quantity_unit([value])
    formatted = format_number_by_unit(value, unit)
    return str(formatted)
