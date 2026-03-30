import ast
from typing import Any, Dict, List, Tuple

import pandas as pd


FORBIDDEN_NAMES = {
    "open",
    "exec",
    "eval",
    "compile",
    "__import__",
    "os",
    "sys",
    "subprocess",
    "socket",
    "requests",
    "httpx",
}

FORBIDDEN_NODES = (
    ast.Import,
    ast.ImportFrom,
    ast.With,
    ast.Try,
    ast.While,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.Lambda,
    ast.Delete,
)

SAFE_BUILTINS = {
    "len": len,
    "min": min,
    "max": max,
    "sum": sum,
    "sorted": sorted,
    "abs": abs,
    "round": round,
    "str": str,
    "int": int,
    "float": float,
    "list": list,
}


def _has_result_assignment(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "result":
                    return True
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "result":
                return True
    return False


def validate_python_code(code: str) -> Tuple[bool, str | None]:
    # LLM이 생성한 코드를 그대로 실행하면 위험하므로,
    # import, 파일 접근, 특수 속성 접근 등을 먼저 막습니다.
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return False, f"생성된 pandas 코드 문법 오류: {exc}"

    for node in ast.walk(tree):
        if isinstance(node, FORBIDDEN_NODES):
            return False, f"허용되지 않는 구문입니다: {type(node).__name__}"
        if isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            return False, f"허용되지 않는 이름입니다: {node.id}"
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            return False, "dunder 속성 접근은 허용되지 않습니다."

    if not _has_result_assignment(tree):
        return False, "pandas 코드는 `result = ...` 할당이 필요합니다."

    return True, None


def execute_safe_dataframe_code(code: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
    # 문자열 형태의 pandas 코드를 실제로 실행하는 곳입니다.
    # 단, validate_python_code를 통과한 코드만 실행합니다.
    ok, error = validate_python_code(code)
    if not ok:
        return {"success": False, "error_message": error, "data": []}

    df = pd.DataFrame(data or [])
    local_vars = {"df": df.copy(), "pd": pd, "result": None}
    safe_globals = {"__builtins__": SAFE_BUILTINS}

    try:
        exec(code, safe_globals, local_vars)
    except Exception as exc:
        return {"success": False, "error_message": f"분석 코드 실행 실패: {exc}", "data": []}

    result = local_vars.get("result")
    if result is None:
        return {"success": False, "error_message": "분석 코드가 result 변수를 만들지 않았습니다.", "data": []}

    if isinstance(result, pd.Series):
        result = result.to_frame().reset_index()

    if not isinstance(result, pd.DataFrame):
        return {"success": False, "error_message": "분석 결과가 DataFrame이 아닙니다.", "data": []}

    result = result.where(pd.notnull(result), None)
    return {"success": True, "data": result.to_dict(orient="records")}
