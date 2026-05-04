from __future__ import annotations

import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
sys.path.append(str(ROOT / "src"))

from squadron_ai.analytics import (
    AssistantResponse,
    answer_demo_question,
    delayed_missions,
    kpis,
    mission_success_by_squadron,
    operational_risk_by_squadron,
    readiness_anomalies,
)
from squadron_ai.demo_data import load_demo_dataset
from squadron_ai.snowflake_client import SnowflakeClient, load_config


st.set_page_config(page_title="Cortex AI Squadron Assistant", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.4rem; padding-bottom: 2rem;}
    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #deded6;
        border-radius: 8px;
        padding: 0.85rem 0.9rem;
    }
    [data-testid="stMetricValue"] {font-size: 1.45rem;}
    </style>
    """,
    unsafe_allow_html=True,
)


def render_metric_row(metrics: dict[str, str]) -> None:
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics.items()):
        col.metric(label, value)


def filter_dataset(dataset, squadrons: list[str], date_range: tuple[pd.Timestamp, pd.Timestamp]):
    start, end = date_range
    missions = dataset.missions[
        dataset.missions["squadron"].isin(squadrons)
        & (dataset.missions["mission_date"].dt.date >= start)
        & (dataset.missions["mission_date"].dt.date <= end)
    ].copy()
    readiness = dataset.readiness[dataset.readiness["squadron"].isin(squadrons)].copy()
    personnel = dataset.personnel[dataset.personnel["squadron"].isin(squadrons)].copy()
    maintenance = dataset.maintenance_logs[dataset.maintenance_logs["squadron"].isin(squadrons)].copy()
    incidents = dataset.incident_reports[dataset.incident_reports["squadron"].isin(squadrons)].copy()
    parts = dataset.parts_inventory.copy()

    from squadron_ai.demo_data import DemoDataset

    return DemoDataset(missions, readiness, personnel, maintenance, incidents, parts)


def render_success_chart(success_df: pd.DataFrame) -> None:
    chart = (
        alt.Chart(success_df)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("squadron:N", title="Squadron"),
            y=alt.Y("success_rate:Q", title="Mission success rate", axis=alt.Axis(format="%")),
            color=alt.Color("squadron:N", legend=None, scale=alt.Scale(range=["#2f6f73", "#a85432", "#596b8f"])),
            tooltip=[
                "squadron",
                "missions",
                alt.Tooltip("success_rate:Q", format=".0%"),
                alt.Tooltip("avg_readiness:Q", format=".1f"),
                "delayed_missions",
            ],
        )
        .properties(height=270)
    )
    st.altair_chart(chart, use_container_width=True)


def render_readiness_chart(readiness_df: pd.DataFrame) -> None:
    chart = (
        alt.Chart(readiness_df)
        .mark_circle(size=150)
        .encode(
            x=alt.X("aircraft_id:N", title="Aircraft"),
            y=alt.Y("readiness_score:Q", title="Readiness score", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color("readiness_state:N", scale=alt.Scale(range=["#2f6f73", "#d6a21f", "#b63b3b"])),
            tooltip=["aircraft_id", "squadron", "aircraft_type", "readiness_state", "readiness_score", "open_maintenance_items"],
        )
        .properties(height=270)
    )
    st.altair_chart(chart, use_container_width=True)


def render_risk_chart(risk_df: pd.DataFrame) -> None:
    chart = (
        alt.Chart(risk_df)
        .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            y=alt.Y("squadron:N", title="Squadron", sort="-x"),
            x=alt.X("risk_score:Q", title="Operational risk score"),
            color=alt.Color("squadron:N", legend=None, scale=alt.Scale(range=["#b63b3b", "#a85432", "#2f6f73"])),
            tooltip=["squadron", "risk_score", alt.Tooltip("success_rate:Q", format=".0%"), "readiness_anomalies", "total_delay_minutes"],
        )
        .properties(height=230)
    )
    st.altair_chart(chart, use_container_width=True)


def run_demo_mode(question: str, filtered_dataset) -> AssistantResponse:
    return answer_demo_question(question, filtered_dataset)


def run_snowflake_mode(question: str) -> AssistantResponse:
    config = load_config()
    client = SnowflakeClient(config)
    if config is None and not client.is_native_streamlit:
        fallback = answer_demo_question(question, load_demo_dataset())
        fallback.answer += "\n\nDemo mode is active because Snowflake credentials are not configured."
        fallback.route = "demo-fallback"
        return fallback
    return client.ask(question)


dataset = load_demo_dataset()

with st.sidebar:
    st.header("Controls")
    mode = st.radio("Data source", ["Demo", "Snowflake"], horizontal=True)
    squadron_options = sorted(dataset.missions["squadron"].unique().tolist())
    squadrons = st.multiselect("Squadrons", squadron_options, default=squadron_options)
    min_date = dataset.missions["mission_date"].dt.date.min()
    max_date = dataset.missions["mission_date"].dt.date.max()
    date_range = st.date_input("Mission date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if len(date_range) != 2:
        date_range = (min_date, max_date)
    st.caption("Snowflake mode uses Cortex Analyst when `SNOWFLAKE_PAT` is configured, then falls back to curated SQL.")
    st.divider()
    st.subheader("Example Questions")
    examples = [
        "Which squadron had the highest mission success rate last month?",
        "Summarize delayed missions and likely causes.",
        "Find anomalies in aircraft readiness.",
        "Generate a weekly squadron operations report.",
        "Which aircraft need maintenance attention?",
        "Which parts or supplies are creating operational risk?",
    ]
    selected = st.selectbox("Load a question", examples)

filtered_dataset = filter_dataset(dataset, squadrons or squadron_options, date_range)

st.title("Cortex AI Squadron Data Assistant")
st.caption("Natural-language analytics for missions, aircraft readiness, personnel availability, maintenance logs, incidents, and parts risk.")

render_metric_row(kpis(filtered_dataset))

tab_chat, tab_ops, tab_evidence, tab_cortex = st.tabs(["Assistant", "Operations", "Evidence", "Cortex Assets"])

with tab_chat:
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "Ask about mission success, delays, readiness anomalies, maintenance context, parts risk, or weekly reports.",
            }
        ]
    if "last_response" not in st.session_state:
        st.session_state.last_response = None

    left, right = st.columns([0.8, 0.2])
    with left:
        question = st.chat_input("Ask a squadron operations question")
    with right:
        ask_loaded = st.button("Ask Example", type="primary", use_container_width=True)
    if ask_loaded and not question:
        question = selected

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    if question:
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            with st.spinner("Analyzing squadron data"):
                response = run_demo_mode(question, filtered_dataset) if mode == "Demo" else run_snowflake_mode(question)
            st.write(response.answer)
            st.caption(f"Route: {response.route}")
            if response.result_table is not None and len(response.result_table) > 0:
                st.dataframe(response.result_table, use_container_width=True, hide_index=True)
            with st.expander("Generated SQL / Query Plan", expanded=False):
                st.code(response.generated_sql or "No SQL generated for this response.", language="sql")
            with st.expander("Evidence", expanded=False):
                if response.evidence is not None and len(response.evidence) > 0:
                    st.dataframe(response.evidence, use_container_width=True, hide_index=True)
                else:
                    st.write("No evidence rows returned.")
            if response.caveats:
                st.warning(" ".join(response.caveats))
            st.session_state.messages.append({"role": "assistant", "content": response.answer})
            st.session_state.last_response = response

    response = st.session_state.last_response
    if response and response.answer:
        st.download_button(
            "Download Latest Answer",
            data=response.answer,
            file_name="squadron_ai_answer.txt",
            mime="text/plain",
        )

with tab_ops:
    left, right = st.columns(2)
    with left:
        st.subheader("Mission Performance")
        render_success_chart(mission_success_by_squadron(filtered_dataset.missions))
        st.dataframe(filtered_dataset.missions.sort_values("mission_date", ascending=False), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Aircraft Readiness")
        render_readiness_chart(filtered_dataset.readiness)
        st.dataframe(readiness_anomalies(filtered_dataset.readiness), use_container_width=True, hide_index=True)

    st.subheader("Operational Risk")
    risk = operational_risk_by_squadron(filtered_dataset)
    render_risk_chart(risk)
    st.dataframe(risk, use_container_width=True, hide_index=True)

with tab_evidence:
    st.subheader("Maintenance Logs")
    st.dataframe(filtered_dataset.maintenance_logs.sort_values("log_date", ascending=False), use_container_width=True, hide_index=True)
    st.subheader("Incident Reports")
    st.dataframe(filtered_dataset.incident_reports.sort_values("incident_date", ascending=False), use_container_width=True, hide_index=True)
    st.subheader("Parts Inventory")
    st.dataframe(filtered_dataset.parts_inventory, use_container_width=True, hide_index=True)
    st.subheader("Delayed Missions")
    st.dataframe(delayed_missions(filtered_dataset.missions), use_container_width=True, hide_index=True)

with tab_cortex:
    st.subheader("Snowflake Cortex Implementation")
    st.markdown(
        "This app is designed to run in demo mode, local Snowflake mode, or native Snowflake Streamlit mode. "
        "In Snowflake mode, it attempts Cortex Analyst first, then uses curated SQL plus Cortex Search and AI_COMPLETE."
    )
    st.code(
        """
-- Cortex Analyst REST endpoint used by the app
POST /api/v2/cortex/analyst/message
{
  "messages": [{"role": "user", "content": [{"type": "text", "text": "<question>"}]}],
  "semantic_model_file": "@SQUADRON_AI_DB.OPERATIONS.SEMANTIC_MODELS/squadron_operations.semantic.yaml"
}

-- Cortex Search retrieval
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'SQUADRON_AI_DB.OPERATIONS.OPERATIONAL_SEARCH_SERVICE',
  '{"query": "hydraulic avionics engine readiness", "columns": ["DOCUMENT_TEXT"], "limit": 5}'
);

-- AI report generation
SELECT AI_COMPLETE(
  'claude-4-sonnet',
  'Generate a weekly squadron operations report: '
  || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
)
FROM VW_SQUADRON_RISK;
        """,
        language="sql",
    )
    st.info("Upload the semantic YAML in `semantic_model/` to the `SEMANTIC_MODELS` stage before enabling Cortex Analyst.")
