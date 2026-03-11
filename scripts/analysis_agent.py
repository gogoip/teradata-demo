"""Groq-backed analysis/chat agent for the S2 Streamlit demo."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from langgraph.graph import END, START, StateGraph


class AgentState(TypedDict):
    system_prompt: str
    context: str
    user_prompt: str
    answer: str


SYSTEM_PROMPT = """You are a workload optimization agent for a Teradata telemetry demo.
Explain what is driving excess TCore-style consumption, which offenders matter most, and what changes after remediation.
Use only the provided context. If evidence is missing, say so.
Keep answers concise, specific, operational, and action-oriented.
Prefer: what changed, who is driving avoidable consumption, what to inspect next, and what the selected remediation improves.
Do not explain the app implementation unless asked."""


@lru_cache(maxsize=1)
def _agent_graph():
    graph = StateGraph(AgentState)

    def respond(state: AgentState) -> AgentState:
        llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0.2,
            groq_api_key=os.environ["GROQ_API_KEY"],
        )
        messages = [
            SystemMessage(content=state["system_prompt"]),
            HumanMessage(
                content=(
                    "Context:\n"
                    f"{state['context']}\n\n"
                    "User request:\n"
                    f"{state['user_prompt']}"
                )
            ),
        ]
        answer = llm.invoke(messages).content
        if not isinstance(answer, str):
            answer = str(answer)
        return {**state, "answer": answer}

    graph.add_node("respond", respond)
    graph.add_edge(START, "respond")
    graph.add_edge("respond", END)
    return graph.compile()


def groq_ready() -> bool:
    return bool(os.getenv("GROQ_API_KEY"))


def build_agent_context(
    *,
    workload: str,
    check_mode: str,
    latest_ts: str | None,
    selected_snapshot: dict[str, Any] | None,
    latest_outlier: dict[str, Any] | None,
    band_preview: list[dict[str, Any]],
    outlier_preview: list[dict[str, Any]],
    tcore_summary: dict[str, Any] | None = None,
    top_offenders: list[dict[str, Any]] | None = None,
    top_consumers: list[dict[str, Any]] | None = None,
    top_skew_drivers: list[dict[str, Any]] | None = None,
    remediation: str | None = None,
    comparison_mode: str | None = None,
    actions: list[dict[str, Any]] | None = None,
) -> str:
    payload = {
        "workload": workload,
        "check_mode": check_mode,
        "latest_data_timestamp": latest_ts,
        "selected_snapshot": selected_snapshot,
        "latest_outlier": latest_outlier,
        "band_preview": band_preview,
        "outlier_preview": outlier_preview,
        "tcore_summary": tcore_summary,
        "top_offenders": top_offenders or [],
        "top_consumers": top_consumers or [],
        "top_skew_drivers": top_skew_drivers or [],
        "remediation": remediation,
        "comparison_mode": comparison_mode,
        "recommended_actions": actions or [],
    }
    return json.dumps(payload, indent=2, default=str)


def run_agent(user_prompt: str, context: str) -> str:
    state: AgentState = {
        "system_prompt": SYSTEM_PROMPT,
        "context": context,
        "user_prompt": user_prompt,
        "answer": "",
    }
    result = _agent_graph().invoke(state)
    return result["answer"]


def to_chat_message(role: str, content: str):
    if role == "assistant":
        return AIMessage(content=content)
    return HumanMessage(content=content)
