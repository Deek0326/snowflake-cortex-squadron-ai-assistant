from __future__ import annotations

import copy
import ast
import sys
import time
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


st.set_page_config(page_title="Cortex AI Unit Operations Assistant", layout="wide")

st.markdown(
    """
    <style>
    :root {
        --ink: #162026;
        --muted: #667782;
        --panel: #ffffff;
        --line: #d8dfdc;
        --teal: #1f7775;
        --amber: #c7901e;
        --red: #b64a3e;
        --mist: #eef4f1;
    }
    .stApp {
        background:
            linear-gradient(180deg, rgba(21, 37, 44, 0.08), rgba(21, 37, 44, 0) 260px),
            #f6f7f2;
    }
    .block-container {padding-top: 1.1rem; padding-bottom: 2.4rem; max-width: 1500px;}
    h1, h2, h3 {letter-spacing: 0;}
    h1 {
        font-size: 2.35rem !important;
        line-height: 1.08 !important;
        margin-bottom: 0.25rem !important;
    }
    [data-testid="stSidebar"] {
        background: #111b22;
        color: #e9f0ed;
        border-right: 1px solid #26343b;
    }
    [data-testid="stSidebar"] * {color: #e9f0ed;}
    [data-testid="stSidebar"] .stCaption,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] span {color: #d7e2df !important;}
    [data-testid="stSidebar"] .stCaption p {
        color: #d7e2df !important;
        opacity: 1 !important;
    }
    [data-testid="stSidebar"] code {
        color: #9be3dd !important;
        background: rgba(42, 137, 132, 0.22) !important;
        border: 1px solid rgba(155, 227, 221, 0.28);
        border-radius: 4px;
        padding: 0.08rem 0.25rem;
    }
    [data-testid="stSidebar"] hr {
        border-color: #30434b;
    }
    [data-testid="stSidebar"] [data-baseweb="tag"] {
        background: #2c8a86 !important;
        border-radius: 6px !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div,
    [data-testid="stSidebar"] [data-baseweb="base-input"],
    [data-testid="stSidebar"] [data-testid="stDateInput"] {
        background: #18262e;
        border-color: #30434b;
        border-radius: 6px;
    }
    [data-testid="stMetric"] {
        background: linear-gradient(180deg, #ffffff, #f7faf8);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 0.9rem 0.95rem;
        box-shadow: 0 8px 24px rgba(23, 37, 42, 0.06);
        min-height: 104px;
    }
    [data-testid="stMetricLabel"] {
        color: #40515a;
        font-weight: 700;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.55rem;
        color: var(--ink);
    }
    .ops-hero {
        background:
            linear-gradient(120deg, rgba(17, 31, 38, 0.96), rgba(25, 62, 64, 0.92)),
            repeating-linear-gradient(90deg, rgba(255,255,255,0.06) 0, rgba(255,255,255,0.06) 1px, transparent 1px, transparent 42px);
        border: 1px solid #233a40;
        border-radius: 8px;
        padding: 1.3rem 1.45rem;
        margin-bottom: 1rem;
        color: #f4fbf8;
        box-shadow: 0 18px 45px rgba(17, 31, 38, 0.18);
    }
    .ops-hero h1 {
        color: #f4fbf8 !important;
        margin: 0 !important;
    }
    .ops-hero p {
        color: #b8c9c7;
        margin: 0.45rem 0 0;
        max-width: 980px;
    }
    .status-strip {
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem;
        margin-top: 1rem;
    }
    .status-pill {
        border: 1px solid rgba(255,255,255,0.18);
        border-radius: 999px;
        padding: 0.34rem 0.62rem;
        color: #dbe9e6;
        background: rgba(255,255,255,0.08);
        font-size: 0.82rem;
        font-weight: 700;
    }
    .status-pill strong {color: #ffffff;}
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.25rem;
        border-bottom: 1px solid #d9e1de;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 6px 6px 0 0;
        padding: 0.55rem 0.85rem;
        font-weight: 700;
    }
    .stTabs [aria-selected="true"] {
        background: #e8f2ef;
        color: #176d69;
    }
    [data-testid="stChatMessage"] {
        border: 1px solid #e1e6e3;
        border-radius: 8px;
        background: rgba(255,255,255,0.72);
        padding: 0.45rem;
    }
    [data-testid="stDataFrame"] {
        border: 1px solid #dce4e1;
        border-radius: 8px;
        overflow: hidden;
    }
    div[data-testid="stExpander"] {
        border: 1px solid #dce4e1;
        border-radius: 8px;
        background: rgba(255,255,255,0.65);
    }
    .stButton > button,
    .stDownloadButton > button {
        border-radius: 6px;
        border: 1px solid #1f7775;
        background: #1f7775;
        color: white;
        font-weight: 800;
    }
    .stButton > button:hover,
    .stDownloadButton > button:hover {
        border-color: #155f5d;
        background: #155f5d;
        color: white;
    }
    .stAlert {
        border-radius: 8px;
    }
    section.main p, .stMarkdown p {
        color: #42535b;
    }
    @media (max-width: 900px) {
        .ops-hero {padding: 1rem;}
        h1 {font-size: 1.75rem !important;}
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def render_metric_row(metrics: dict[str, str]) -> None:
    cols = st.columns(len(metrics))
    for col, (label, value) in zip(cols, metrics.items()):
        col.metric(label, value)


def display_table(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "squadron": "unit",
            "squadron_name": "unit_name",
            "SQUADRON_NAME": "UNIT_NAME",
        }
    )


def _intent_profile(question: str) -> dict[str, list[str]]:
    lowered = question.lower()
    if "success" in lowered or "highest" in lowered:
        return {
            "intent": ["mission success ranking"],
            "columns": ["success_rate", "success_count", "mission_count"],
            "answer_terms": ["highest", "success"],
        }
    if "delay" in lowered or "delayed" in lowered:
        return {
            "intent": ["delay analysis"],
            "columns": ["delay_reason", "delayed_missions", "total_delay_minutes"],
            "answer_terms": ["delay"],
        }
    if "readiness" in lowered or "anomal" in lowered or "maintenance" in lowered:
        return {
            "intent": ["readiness risk"],
            "columns": ["aircraft_id", "readiness_score", "readiness_state"],
            "answer_terms": ["readiness", "maintenance", "anomal"],
        }
    if "part" in lowered or "supply" in lowered or "inventory" in lowered:
        return {
            "intent": ["parts risk"],
            "columns": ["part_name", "quantity_on_hand", "reorder_point"],
            "answer_terms": ["part", "supply", "inventory"],
        }
    return {
        "intent": ["general operations question"],
        "columns": [],
        "answer_terms": [],
    }


def _response_columns(response: AssistantResponse) -> set[str]:
    if response.result_table is None:
        return set()
    return {str(column).lower() for column in response.result_table.columns}


def _top_entity_mentioned(response: AssistantResponse) -> bool:
    if response.result_table is None or response.result_table.empty:
        return False
    candidate_columns = ["UNIT_NAME", "SQUADRON_NAME", "unit_name", "squadron_name", "unit", "squadron"]
    for column in candidate_columns:
        if column in response.result_table.columns:
            value = str(response.result_table.iloc[0][column]).lower()
            return bool(value) and value in response.answer.lower()
    return False


def _semantic_checks(response: AssistantResponse, question: str) -> tuple[int, list[dict[str, str]]]:
    profile = _intent_profile(question)
    columns = _response_columns(response)
    normalized_columns = {column.replace("unit", "squadron") for column in columns} | columns
    expected_columns = profile["columns"]
    matched_columns = [column for column in expected_columns if column in normalized_columns]
    answer_lower = response.answer.lower()
    matched_terms = [term for term in profile["answer_terms"] if term in answer_lower]

    checks = [
        {
            "check": "Intent classified",
            "result": "pass",
            "detail": ", ".join(profile["intent"]),
        },
        {
            "check": "Expected metrics present",
            "result": "pass" if not expected_columns or matched_columns else "review",
            "detail": ", ".join(matched_columns) if matched_columns else "No matching expected metric columns",
        },
        {
            "check": "Answer grounded in returned rows",
            "result": "pass" if _top_entity_mentioned(response) or response.route == "demo" else "review",
            "detail": "Top returned entity appears in the answer" if _top_entity_mentioned(response) else "No top entity detected in text",
        },
        {
            "check": "Question language reflected",
            "result": "pass" if not profile["answer_terms"] or matched_terms else "review",
            "detail": ", ".join(matched_terms) if matched_terms else "Answer may be too generic",
        },
    ]
    score = round(sum(1 for check in checks if check["result"] == "pass") / len(checks) * 100)
    return score, checks


def _latency_band(latency_ms: int) -> str:
    if latency_ms <= 3000:
        return "fast"
    if latency_ms <= 9000:
        return "acceptable"
    return "optimize"


def attach_evaluation(response: AssistantResponse, question: str, mode: str, latency_ms: int) -> AssistantResponse:
    rows = 0 if response.result_table is None else len(response.result_table)
    semantic_score, semantic_checks = _semantic_checks(response, question)
    response.latency_ms = latency_ms
    response.evaluation = {
        "Question": question,
        "Mode": mode,
        "Route": response.route,
        "Cortex used": response.route == "cortex-analyst",
        "Fallback used": response.route != "cortex-analyst",
        "SQL generated": bool(response.generated_sql),
        "Rows returned": rows,
        "Evidence rows": 0 if response.evidence is None else len(response.evidence),
        "Latency ms": latency_ms,
        "Latency band": _latency_band(latency_ms),
        "Semantic score": semantic_score,
        "Attempts": int(response.diagnostics.get("attempts", 1)),
        "Retry strategy": str(response.diagnostics.get("retry_strategy", "single_step")),
        "Cortex SQL normalized": bool(response.diagnostics.get("cortex_sql_normalized", False)),
        "Caveats": len(response.caveats),
        "Status": "pass" if (rows > 0 or bool(response.answer)) and semantic_score >= 75 else "review",
    }
    response.diagnostics["semantic_checks"] = str(semantic_checks)
    return response


def render_evaluation_panel(response: AssistantResponse | None) -> None:
    st.subheader("Runtime Evaluation")
    st.caption("This panel measures the answer that was already generated. It does not run extra Cortex or Snowflake queries.")
    if response is None or not response.evaluation:
        st.info("Ask a question to populate route, latency, fallback, and result-quality checks.")
        return

    evaluation = response.evaluation
    cols = st.columns(5)
    cols[0].metric("Route", str(evaluation["Route"]))
    cols[1].metric("Latency", f"{evaluation['Latency ms']} ms")
    cols[2].metric("Quality", f"{evaluation['Semantic score']}%")
    cols[3].metric("Fallback", "Yes" if evaluation["Fallback used"] else "No")
    cols[4].metric("Status", str(evaluation["Status"]).upper())

    checks = pd.DataFrame(
        [
            {"check": "Cortex generated executable SQL", "result": "pass" if evaluation["Cortex used"] else "fallback"},
            {"check": "SQL/query plan available", "result": "pass" if evaluation["SQL generated"] else "review"},
            {"check": "Rows returned for inspection", "result": "pass" if int(evaluation["Rows returned"]) > 0 else "review"},
            {"check": "Semantic answer score", "result": "pass" if int(evaluation["Semantic score"]) >= 75 else "review"},
            {"check": "Latency band", "result": str(evaluation["Latency band"])},
            {"check": "Retry/normalization path", "result": str(evaluation["Retry strategy"])},
            {"check": "Warnings/caveats", "result": "review" if int(evaluation["Caveats"]) > 0 else "pass"},
        ]
    )
    st.dataframe(checks, use_container_width=True, hide_index=True)

    st.subheader("Semantic Quality Checks")
    try:
        semantic_checks = ast.literal_eval(str(response.diagnostics.get("semantic_checks", "[]")))
    except (SyntaxError, ValueError):
        semantic_checks = []
    st.dataframe(pd.DataFrame(semantic_checks), use_container_width=True, hide_index=True)

    st.subheader("No-Cost Evaluation Suite")
    st.caption("Static expected checks for demo coverage. These are not executed automatically, so they do not spend Snowflake credits.")
    st.dataframe(
        pd.DataFrame(
            [
                {
                    "question": "Which unit had the highest mission success rate last month?",
                    "expected_route": "cortex-analyst",
                    "expected_columns": "UNIT_NAME, MISSION_COUNT, SUCCESS_RATE",
                },
                {
                    "question": "Find anomalies in aircraft readiness.",
                    "expected_route": "cortex-analyst or curated fallback",
                    "expected_columns": "AIRCRAFT_ID, UNIT_NAME, READINESS_SCORE",
                },
                {
                    "question": "Which parts are creating operational risk?",
                    "expected_route": "cortex-analyst or curated fallback",
                    "expected_columns": "PART_NAME, AIRCRAFT_ID, QUANTITY_ON_HAND",
                },
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.subheader("Cost And Latency Controls")
    st.caption("These controls describe the current app strategy without running more queries.")
    st.dataframe(
        pd.DataFrame(
            [
                {"control": "Identical Snowflake questions cached for 5 minutes", "impact": "Reduces repeated Cortex/Snowflake calls during demos"},
                {"control": "Cortex SQL normalization before fallback", "impact": "Repairs common semantic-model aliases before spending a fallback query"},
                {"control": "Curated SQL fallback", "impact": "Keeps the app usable when generated SQL is not executable"},
                {"control": "Evaluation after answer generation", "impact": "Adds observability without extra Cortex calls"},
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )


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
            x=alt.X("squadron:N", title="Unit"),
            y=alt.Y("success_rate:Q", title="Mission success rate", axis=alt.Axis(format="%")),
            color=alt.Color("squadron:N", legend=None, scale=alt.Scale(range=["#2f6f73", "#a85432", "#596b8f"])),
            tooltip=[
                alt.Tooltip("squadron:N", title="Unit"),
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
            tooltip=[
                "aircraft_id",
                alt.Tooltip("squadron:N", title="Unit"),
                "aircraft_type",
                "readiness_state",
                "readiness_score",
                "open_maintenance_items",
            ],
        )
        .properties(height=270)
    )
    st.altair_chart(chart, use_container_width=True)


def render_risk_chart(risk_df: pd.DataFrame) -> None:
    chart = (
        alt.Chart(risk_df)
        .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            y=alt.Y("squadron:N", title="Unit", sort="-x"),
            x=alt.X("risk_score:Q", title="Operational risk score"),
            color=alt.Color("squadron:N", legend=None, scale=alt.Scale(range=["#b63b3b", "#a85432", "#2f6f73"])),
            tooltip=[
                alt.Tooltip("squadron:N", title="Unit"),
                "risk_score",
                alt.Tooltip("success_rate:Q", format=".0%"),
                "readiness_anomalies",
                "total_delay_minutes",
            ],
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


@st.cache_data(ttl=300, show_spinner=False)
def cached_snowflake_response(question: str) -> AssistantResponse:
    response = run_snowflake_mode(question)
    response.diagnostics["cache_policy"] = "ttl_300_seconds"
    return response


dataset = load_demo_dataset()

with st.sidebar:
    st.header("Controls")
    mode = st.radio("Data source", ["Demo", "Snowflake"], horizontal=True)
    squadron_options = sorted(dataset.missions["squadron"].unique().tolist())
    squadrons = st.multiselect("Units", squadron_options, default=squadron_options)
    min_date = dataset.missions["mission_date"].dt.date.min()
    max_date = dataset.missions["mission_date"].dt.date.max()
    date_range = st.date_input("Mission date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
    if len(date_range) != 2:
        date_range = (min_date, max_date)
    st.caption("Snowflake mode uses Cortex Analyst when `SNOWFLAKE_PAT` is configured, then falls back to curated SQL.")
    st.caption(f"Snowflake config: {'ready' if load_config() else 'missing'}")
    st.divider()
    st.subheader("Example Questions")
    examples = [
        "Which unit had the highest mission success rate last month?",
        "Summarize delayed missions and likely causes.",
        "Find anomalies in aircraft readiness.",
        "Generate a weekly unit operations report.",
        "Which aircraft need maintenance attention?",
        "Which parts or supplies are creating operational risk?",
    ]
    selected = st.selectbox("Load a question", examples)

filtered_dataset = filter_dataset(dataset, squadrons or squadron_options, date_range)

st.markdown(
    """
    <div class="ops-hero">
        <h1>Cortex AI Unit Operations Assistant</h1>
        <p>Natural-language command center for mission performance, readiness, delays, personnel availability, maintenance evidence, incidents, and parts risk.</p>
        <div class="status-strip">
            <span class="status-pill">Mode <strong>Snowflake + Cortex</strong></span>
            <span class="status-pill">Semantic layer <strong>enabled</strong></span>
            <span class="status-pill">Fallback <strong>curated SQL</strong></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

render_metric_row(kpis(filtered_dataset))

tab_chat, tab_ops, tab_evaluation, tab_evidence, tab_cortex = st.tabs(
    ["Assistant", "Operations", "Evaluation", "Evidence", "Cortex Assets"]
)

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
        question = st.chat_input("Ask a unit operations question")
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
            with st.spinner("Analyzing unit operations data"):
                start_time = time.perf_counter()
                response = (
                    run_demo_mode(question, filtered_dataset)
                    if mode == "Demo"
                    else copy.deepcopy(cached_snowflake_response(question))
                )
                latency_ms = int((time.perf_counter() - start_time) * 1000)
                response = attach_evaluation(response, question, mode, latency_ms)
            st.write(response.answer)
            st.caption(f"Route: {response.route}")
            if response.result_table is not None and len(response.result_table) > 0:
                st.dataframe(display_table(response.result_table), use_container_width=True, hide_index=True)
            with st.expander("Generated SQL / Query Plan", expanded=False):
                st.code(response.generated_sql or "No SQL generated for this response.", language="sql")
            with st.expander("Evidence", expanded=False):
                if response.evidence is not None and len(response.evidence) > 0:
                    st.dataframe(display_table(response.evidence), use_container_width=True, hide_index=True)
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
            file_name="unit_operations_ai_answer.txt",
            mime="text/plain",
        )

with tab_ops:
    left, right = st.columns(2)
    with left:
        st.subheader("Mission Performance")
        render_success_chart(mission_success_by_squadron(filtered_dataset.missions))
        st.dataframe(display_table(filtered_dataset.missions.sort_values("mission_date", ascending=False)), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Aircraft Readiness")
        render_readiness_chart(filtered_dataset.readiness)
        st.dataframe(display_table(readiness_anomalies(filtered_dataset.readiness)), use_container_width=True, hide_index=True)

    st.subheader("Operational Risk")
    risk = operational_risk_by_squadron(filtered_dataset)
    render_risk_chart(risk)
    st.dataframe(display_table(risk), use_container_width=True, hide_index=True)

with tab_evaluation:
    render_evaluation_panel(st.session_state.get("last_response"))

with tab_evidence:
    st.subheader("Maintenance Logs")
    st.dataframe(display_table(filtered_dataset.maintenance_logs.sort_values("log_date", ascending=False)), use_container_width=True, hide_index=True)
    st.subheader("Incident Reports")
    st.dataframe(display_table(filtered_dataset.incident_reports.sort_values("incident_date", ascending=False)), use_container_width=True, hide_index=True)
    st.subheader("Parts Inventory")
    st.dataframe(filtered_dataset.parts_inventory, use_container_width=True, hide_index=True)
    st.subheader("Delayed Missions")
    st.dataframe(display_table(delayed_missions(filtered_dataset.missions)), use_container_width=True, hide_index=True)

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
  'Generate a weekly unit operations report: '
  || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
)
FROM VW_SQUADRON_RISK;
        """,
        language="sql",
    )
    st.info("Upload the semantic YAML in `semantic_model/` to the `SEMANTIC_MODELS` stage before enabling Cortex Analyst.")
