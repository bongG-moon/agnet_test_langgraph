from ui_renderer import _get_expanded_indexes


def test_expanded_indexes_follow_primary_marker():
    tool_results = [
        {"success": True, "tool_name": "source", "display_expanded": False},
        {"success": True, "tool_name": "final", "display_expanded": True},
    ]

    assert _get_expanded_indexes(tool_results) == {1}


def test_expanded_indexes_fall_back_to_last_success():
    tool_results = [
        {"success": True, "tool_name": "source"},
        {"success": False, "tool_name": "error"},
        {"success": True, "tool_name": "final"},
    ]

    assert _get_expanded_indexes(tool_results) == {2}
