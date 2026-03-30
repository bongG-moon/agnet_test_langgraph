from core.analysis_helpers import find_missing_dimensions, format_missing_column_message


def test_date_phrase_is_not_treated_as_missing_dimension():
    columns = ["WORK_DT", "OPER_NAME", "MODE", "production"]
    missing = find_missing_dimensions("어제 기준으로 생산량은 있는데 재공은 없는 제품 LIST를 보여줘", columns)
    assert "어제" not in missing


def test_missing_column_message_explains_merge_suffixes():
    message = format_missing_column_message(
        ["재공수량"],
        ["OPER_NAME", "MODE", "FACTORY_x", "FACTORY_y", "production"],
    )
    assert "_x`/`_y" in message
