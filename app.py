"""Streamlit entrypoint for the beginner-friendly LangGraph demo app."""

import streamlit as st

from core.agent import run_agent
from ui_renderer import init_session_state, render_context, render_tool_results, sync_context


st.set_page_config(page_title="Compact Manufacturing Chat", layout="wide")


def _get_saved_chat_history():
    """Keep only the minimal fields the agent needs from the UI message list."""

    return [{"role": message["role"], "content": message["content"]} for message in st.session_state.messages]


def _render_saved_chat_history() -> None:
    """Replay prior chat messages so a page refresh keeps the conversation readable."""

    engineer_mode = bool(st.session_state.get("engineer_mode", False))
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            if message.get("tool_results"):
                render_tool_results(message["tool_results"], engineer_mode=engineer_mode)


def _run_chat_turn(user_input: str):
    """Bridge the Streamlit session state and the LangGraph runtime.

    The agent only knows about plain inputs such as chat history, current context,
    and the current table. After the agent finishes, we sync the returned context
    back into the UI session so follow-up questions keep working.
    """

    result = run_agent(
        user_input=user_input,
        chat_history=_get_saved_chat_history(),
        context=st.session_state.context,
        current_data=st.session_state.current_data,
    )

    tool_results = result.get("tool_results", [])
    extracted_params = result.get("extracted_params", {})
    if tool_results:
        sync_context(extracted_params)

    st.session_state.current_data = result.get("current_data")
    return result


def _render_display_options() -> None:
    """Expose a simple toggle so users can switch between clean view and debug view."""

    st.session_state.engineer_mode = st.toggle(
        "ENG'R 모드",
        value=bool(st.session_state.get("engineer_mode", False)),
        help="켜면 pandas 처리 요약과 생성 코드를 함께 보여줍니다.",
    )


def main() -> None:
    """Render the chat app and handle one user turn at a time.

    This function is intentionally short:
    1. prepare Streamlit session state
    2. draw the current screen
    3. wait for one user input
    4. call the agent
    5. render the answer and persist it back into session history
    """

    init_session_state()

    st.title("제조 데이터 채팅 분석")
    st.caption("초기 질문에서도 필요하면 바로 후처리까지 실행하고, 최종 답변에 쓰인 표를 중심으로 보여줍니다.")
    _render_display_options()
    render_context()
    _render_saved_chat_history()

    user_input = st.chat_input("예: 오늘 XX공정에서 MODE별 생산량 알려줘")
    if not user_input:
        return

    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    result = _run_chat_turn(user_input)
    response = result.get("response", "응답을 생성하지 못했습니다.")
    tool_results = result.get("tool_results", [])
    engineer_mode = bool(st.session_state.get("engineer_mode", False))

    with st.chat_message("assistant"):
        st.markdown(response)
        if tool_results:
            render_tool_results(tool_results, engineer_mode=engineer_mode)

    st.session_state.messages.append(
        {
            "role": "assistant",
            "content": response,
            "tool_results": tool_results,
        }
    )


if __name__ == "__main__":
    main()
