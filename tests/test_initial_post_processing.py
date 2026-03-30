from core import agent


def _mock_source_result():
    return [
        {
            "success": True,
            "tool_name": "get_production_data",
            "dataset_key": "production",
            "dataset_label": "생산",
            "summary": "원본 생산 데이터",
            "data": [
                {"WORK_DT": "20260330", "OPER_NAME": "WET1", "MODE": "DDR5", "production": 100},
                {"WORK_DT": "20260330", "OPER_NAME": "WET1", "MODE": "LPDDR5", "production": 200},
            ],
        }
    ]


def test_initial_retrieval_runs_post_processing(monkeypatch):
    monkeypatch.setattr(agent, "pick_retrieval_tools", lambda text: ["production"])
    monkeypatch.setattr(agent, "execute_retrieval_tools", lambda keys, params: _mock_source_result())
    monkeypatch.setattr(
        agent,
        "execute_analysis_query",
        lambda query_text, data, source_tool_name="": {
            "success": True,
            "tool_name": "analyze_current_data",
            "summary": "MODE별 생산량",
            "data": [
                {"MODE": "LPDDR5", "production": 200},
                {"MODE": "DDR5", "production": 100},
            ],
            "analysis_plan": {"intent": "group by mode"},
            "transformation_summary": {"group_by_columns": ["MODE"]},
            "generated_code": "result = df.groupby('MODE', as_index=False)['production'].sum()",
        },
    )
    monkeypatch.setattr(agent, "_generate_response", lambda user_input, result, chat_history: "ok")

    result = agent._run_retrieval(
        user_input="오늘 WET1 공정에서 MODE별 생산량 알려줘",
        chat_history=[],
        current_data=None,
        extracted_params={"date": "20260330", "process_name": ["WET1"], "group_by": "MODE"},
    )

    assert result["current_data"]["tool_name"] == "analyze_current_data"
    assert [item.get("display_expanded") for item in result["tool_results"]] == [False, True]
    assert result["tool_results"][1]["applied_params"]["group_by"] == "MODE"


def test_initial_retrieval_keeps_source_result_when_analysis_fails(monkeypatch):
    monkeypatch.setattr(agent, "pick_retrieval_tools", lambda text: ["production"])
    monkeypatch.setattr(agent, "execute_retrieval_tools", lambda keys, params: _mock_source_result())
    monkeypatch.setattr(
        agent,
        "execute_analysis_query",
        lambda query_text, data, source_tool_name="": {
            "success": False,
            "tool_name": "analyze_current_data",
            "error_message": "analysis failed",
            "data": [],
        },
    )
    monkeypatch.setattr(agent, "_generate_response", lambda user_input, result, chat_history: "source-ok")

    result = agent._run_retrieval(
        user_input="오늘 WET1 공정에서 MODE별 생산량 알려줘",
        chat_history=[],
        current_data=None,
        extracted_params={"date": "20260330", "process_name": ["WET1"], "group_by": "MODE"},
    )

    assert result["current_data"]["tool_name"] == "get_production_data"
    assert [item.get("display_expanded") for item in result["tool_results"]] == [True, False]
    assert result["response"].startswith("analysis failed")


def test_build_retrieval_jobs_repeats_same_dataset_for_multiple_dates():
    jobs = agent._build_retrieval_jobs(
        user_input="어제 DA공정 생산량과 오늘 DA공정 생산량을 비교해줘",
        extracted_params={"date": "20260330", "process_name": ["D/A1"]},
        retrieval_keys=["production"],
    )

    assert len(jobs) == 2
    assert [job["result_label"] for job in jobs] == ["어제", "오늘"]
    assert jobs[0]["params"]["process_name"] == ["D/A1"]
    assert jobs[0]["params"]["date"] != jobs[1]["params"]["date"]


def test_analysis_base_table_uses_meaningful_metric_names_for_same_dataset_compare():
    tool_results = [
        {
            "success": True,
            "tool_name": "get_production_data",
            "dataset_key": "production__어제",
            "dataset_label": "생산 (어제)",
            "result_label": "어제",
            "source_tag": "어제",
            "applied_params": {"date": "20260329"},
            "data": [{"WORK_DT": "20260329", "OPER_NAME": "D/A1", "MODE": "DDR5", "production": 100}],
        },
        {
            "success": True,
            "tool_name": "get_production_data",
            "dataset_key": "production__오늘",
            "dataset_label": "생산 (오늘)",
            "result_label": "오늘",
            "source_tag": "오늘",
            "applied_params": {"date": "20260330"},
            "data": [{"WORK_DT": "20260330", "OPER_NAME": "D/A1", "MODE": "DDR5", "production": 120}],
        },
    ]

    result = agent._build_analysis_base_table(tool_results)

    assert result["success"] is True
    columns = set(result["data"][0].keys())
    assert "production_어제" in columns
    assert "production_오늘" in columns
    assert "production_x" not in columns
    assert "production_y" not in columns


def test_unknown_retrieval_message_uses_registered_dataset_labels(monkeypatch):
    monkeypatch.setattr(agent, "pick_retrieval_tools", lambda text: [])
    monkeypatch.setattr(agent, "list_available_dataset_labels", lambda: ["생산", "WIP", "수율"])

    result = agent._run_retrieval(
        user_input="뭐를 볼 수 있어?",
        chat_history=[],
        current_data=None,
        extracted_params={},
    )

    assert "생산, WIP, 수율" in result["response"]


def test_missing_date_only_blocks_date_required_datasets(monkeypatch):
    monkeypatch.setattr(agent, "pick_retrieval_tools", lambda text: ["production"])
    monkeypatch.setattr(
        agent,
        "_build_retrieval_jobs",
        lambda user_input, extracted_params, retrieval_keys: [
            {"dataset_key": "production", "params": {"process_name": ["D/A1"]}, "result_label": None}
        ],
    )
    monkeypatch.setattr(agent, "dataset_requires_date", lambda dataset_key: dataset_key == "production")
    monkeypatch.setattr(agent, "get_dataset_label", lambda dataset_key: "생산")

    result = agent._run_retrieval(
        user_input="DA 공정 생산량 보여줘",
        chat_history=[],
        current_data=None,
        extracted_params={"process_name": ["D/A1"]},
    )

    assert "생산" in result["response"]
    assert "오늘, 어제, 20260324" in result["response"]


def test_missing_date_does_not_block_dataset_without_date_requirement(monkeypatch):
    monkeypatch.setattr(agent, "pick_retrieval_tools", lambda text: ["custom_lookup"])
    monkeypatch.setattr(
        agent,
        "_build_retrieval_jobs",
        lambda user_input, extracted_params, retrieval_keys: [
            {"dataset_key": "custom_lookup", "params": {"process_name": ["D/A1"]}, "result_label": None}
        ],
    )
    monkeypatch.setattr(agent, "dataset_requires_date", lambda dataset_key: False)
    monkeypatch.setattr(
        agent,
        "_execute_retrieval_jobs",
        lambda jobs: [
            {
                "success": True,
                "tool_name": "get_custom_lookup",
                "dataset_key": "custom_lookup",
                "dataset_label": "사용자 조회",
                "summary": "ok",
                "data": [{"OPER_NAME": "D/A1", "value": 1}],
            }
        ],
    )
    monkeypatch.setattr(agent, "_generate_response", lambda user_input, result, chat_history: "ok")

    result = agent._run_retrieval(
        user_input="커스텀 조회 보여줘",
        chat_history=[],
        current_data=None,
        extracted_params={"process_name": ["D/A1"]},
    )

    assert result["current_data"]["tool_name"] == "get_custom_lookup"


def test_run_agent_returns_langgraph_execution_marker(monkeypatch):
    monkeypatch.setattr(
        agent,
        "resolve_required_params",
        lambda user_input, chat_history_text, current_data_columns, context: {
            "date": "20260330",
            "process_name": ["WET1"],
        },
    )
    monkeypatch.setattr(agent, "pick_retrieval_tools", lambda text: ["production"])
    monkeypatch.setattr(
        agent,
        "execute_retrieval_tools",
        lambda keys, params: [
            {
                "success": True,
                "tool_name": "get_production_data",
                "dataset_key": "production",
                "dataset_label": "생산",
                "summary": "ok",
                "data": [{"WORK_DT": "20260330", "OPER_NAME": "WET1", "production": 100}],
            }
        ],
    )
    monkeypatch.setattr(agent, "_generate_response", lambda user_input, result, chat_history: "ok")

    result = agent.run_agent(
        user_input="오늘 WET1 생산량 보여줘",
        chat_history=[],
        context={},
        current_data=None,
    )

    assert result["execution_engine"] == "langgraph"
