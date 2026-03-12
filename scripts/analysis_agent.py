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
Prefer: what changed, which QueryID or step is driving avoidable consumption, what telemetry evidence supports it, what to inspect next, and what the selected remediation improves.
If telemetry families are missing, say that explicitly.
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


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fallback_analysis(context: str) -> str:
    try:
        payload = json.loads(context)
    except json.JSONDecodeError:
        return "Live agent is unavailable. Fallback analysis could not parse the telemetry context."

    parts: list[str] = []
    tcore = payload.get("tcore_summary") or {}
    excess = _safe_float(tcore.get("excess_tcore"))
    savings = _safe_float(tcore.get("projected_savings_score"))
    if excess is not None:
        msg = f"Estimated excess TCore is {excess:.2f}"
        if savings is not None and savings > 0:
            msg += f"; selected remediation projects {savings:.2f} savings"
        parts.append(msg + ".")

    offenders = payload.get("top_offenders") or []
    if offenders:
        top = offenders[0]
        target = top.get("entity_id") or top.get("workload") or "unknown"
        top_excess = _safe_float(top.get("excess_tcore"))
        if top_excess is not None:
            parts.append(f"Top workload offender is `{target}` with excess TCore {top_excess:.2f}.")
        else:
            parts.append(f"Top workload offender is `{target}`.")

    consumers = payload.get("top_consumers") or []
    if consumers:
        top = consumers[0]
        target = top.get("entity_id") or "unknown"
        cpu_excess = _safe_float(top.get("cpu_excess"))
        if cpu_excess is not None:
            parts.append(f"Top noisy consumer is `{target}` with CPU excess {cpu_excess:.2f}.")

    skew = payload.get("top_skew_drivers") or []
    if skew:
        top = skew[0]
        qid = top.get("query_id")
        sid = top.get("step_id")
        latest_skew = _safe_float(top.get("latest_skew"))
        if qid and sid:
            msg = f"Top skew driver is QueryID `{qid}` step `{sid}`"
            if latest_skew is not None:
                msg += f" with skew {latest_skew:.2f}"
            parts.append(msg + ".")

    latest_outlier = payload.get("latest_outlier") or {}
    if latest_outlier:
        target = latest_outlier.get("entity_id") or "unknown"
        observed = _safe_float(latest_outlier.get("observed"))
        expected = _safe_float(latest_outlier.get("expected"))
        if observed is not None and expected is not None:
            parts.append(f"Latest outlier is `{target}` with observed {observed:.2f} versus expected {expected:.2f}.")

    actions = payload.get("recommended_actions") or []
    if actions:
        first = actions[0]
        action = first.get("action")
        target = first.get("target")
        if action and target:
            parts.append(f"Next action: {action} on `{target}`.")

    if not parts:
        return "Live agent is unavailable. Fallback analysis found telemetry context, but there was not enough signal to summarize."
    return " ".join(parts)


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
    telemetry_backend: str | None = None,
    telemetry_scope: dict[str, Any] | None = None,
) -> str:
    payload = {
        "workload": workload,
        "check_mode": check_mode,
        "telemetry_backend": telemetry_backend,
        "telemetry_scope": telemetry_scope or {},
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
    try:
        result = _agent_graph().invoke(state)
        return result["answer"]
    except Exception:
        return _fallback_analysis(context)


def to_chat_message(role: str, content: str):
    if role == "assistant":
        return AIMessage(content=content)
    return HumanMessage(content=content)
