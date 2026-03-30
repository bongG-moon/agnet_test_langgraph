from typing import Any, Dict, List, Optional
from typing_extensions import TypedDict


class RequiredParams(TypedDict, total=False):
    date: Optional[str]
    process_name: Optional[Any]
    oper_num: Optional[Any]
    pkg_type1: Optional[Any]
    pkg_type2: Optional[Any]
    product_name: Optional[str]
    line_name: Optional[str]
    mode: Optional[Any]
    den: Optional[Any]
    tech: Optional[Any]
    lead: Optional[str]
    mcp_no: Optional[str]
    group_by: Optional[str]
    metrics: List[str]
    date_inherited: bool
    process_inherited: bool
    oper_num_inherited: bool
    pkg_type1_inherited: bool
    pkg_type2_inherited: bool
    product_inherited: bool
    line_inherited: bool
    mode_inherited: bool
    den_inherited: bool
    tech_inherited: bool
    lead_inherited: bool
    mcp_no_inherited: bool


class DatasetProfile(TypedDict):
    columns: List[str]
    row_count: int
    sample_rows: List[Dict[str, Any]]


class PreprocessPlan(TypedDict, total=False):
    intent: str
    operations: List[str]
    output_columns: List[str]
    group_by_columns: List[str]
    partition_by_columns: List[str]
    filters: List[Dict[str, Any]]
    sort_by: str
    sort_order: str
    top_n: int
    top_n_per_group: int
    metric_column: str
    warnings: List[str]
    code: str
    source: str
