from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from .analytics import AssistantResponse


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    password: str | None
    role: str
    warehouse: str
    database: str
    schema: str
    semantic_model: str
    cortex_model: str
    cortex_search_service: str
    pat: str | None = None


def load_config() -> SnowflakeConfig | None:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env", override=True)
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
    ]
    if not all(os.getenv(key) for key in required):
        return None

    password = os.getenv("SNOWFLAKE_PASSWORD")
    pat = os.getenv("SNOWFLAKE_PAT")
    if not password and not pat:
        return None

    database = os.getenv("SNOWFLAKE_DATABASE", "SQUADRON_AI_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "OPERATIONS")
    return SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=password,
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=database,
        schema=schema,
        semantic_model=os.getenv(
            "SNOWFLAKE_SEMANTIC_MODEL",
            f"@{database}.{schema}.SEMANTIC_MODELS/squadron_operations.semantic.yaml",
        ),
        cortex_model=os.getenv("SNOWFLAKE_CORTEX_MODEL", "claude-4-sonnet"),
        cortex_search_service=os.getenv(
            "SNOWFLAKE_CORTEX_SEARCH_SERVICE",
            f"{database}.{schema}.OPERATIONAL_SEARCH_SERVICE",
        ),
        pat=pat,
    )


def get_active_snowpark_session():
    try:
        from snowflake.snowpark.context import get_active_session

        return get_active_session()
    except Exception:
        return None


class SnowflakeClient:
    def __init__(self, config: SnowflakeConfig | None = None):
        self.config = config
        self._conn = None
        self._session = get_active_snowpark_session()

    @property
    def is_native_streamlit(self) -> bool:
        return self._session is not None

    def connect(self):
        if self._conn is None:
            if self.config is None or (self.config.password is None and self.config.pat is None):
                raise RuntimeError("Snowflake credentials are not configured.")
            import snowflake.connector

            self._conn = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.pat or self.config.password,
                role=self.config.role,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
            )
        return self._conn

    def query_df(self, sql: str) -> pd.DataFrame:
        if self.config is not None:
            sql = f"USE DATABASE {self.config.database}; USE SCHEMA {self.config.schema};\n{sql}"
        if self._session is not None:
            return self._session.sql(sql).to_pandas()
        conn = self.connect()
        statements = _split_sql_statements(sql)
        with conn.cursor() as cur:
            for statement in statements[:-1]:
                cur.execute(statement)
        return pd.read_sql(statements[-1], conn)

    def execute_scalar(self, sql: str) -> str:
        if self._session is not None:
            rows = self._session.sql(sql).collect()
            return "" if not rows else str(rows[0][0])
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        return "" if row is None else str(row[0])

    def ask(self, question: str) -> AssistantResponse:
        analyst = self.ask_cortex_analyst(question)
        if analyst.generated_sql:
            executable_sql = _normalize_cortex_sql(analyst.generated_sql)
            normalized_sql = executable_sql != analyst.generated_sql
            try:
                df = self.query_df(executable_sql)
            except Exception as exc:
                fallback = self.answer_with_curated_sql(question)
                fallback.diagnostics.update(
                    {
                        "attempts": 2,
                        "cortex_sql_generated": True,
                        "cortex_sql_normalized": normalized_sql,
                        "fallback_reason": "cortex_sql_execution_failed",
                        "retry_strategy": "normalize_cortex_sql_then_curated_sql",
                    }
                )
                fallback.caveats.insert(
                    0,
                    f"Cortex Analyst generated SQL, but executing it failed: {str(exc).splitlines()[0]}",
                )
                fallback.generated_sql = (
                    "-- Cortex Analyst generated SQL failed, so curated SQL fallback was used.\n"
                    + analyst.generated_sql
                    + "\n\n-- Normalized executable Cortex SQL:\n"
                    + executable_sql
                    + "\n\n-- Curated SQL fallback:\n"
                    + (fallback.generated_sql or "")
                )
                return fallback
            analyst.result_table = df
            analyst.generated_sql = executable_sql
            analyst.diagnostics.update(
                {
                    "attempts": 1,
                    "cortex_sql_generated": True,
                    "cortex_sql_normalized": normalized_sql,
                    "fallback_reason": "",
                    "retry_strategy": "cortex_analyst_sql",
                }
            )
            analyst.evidence = self.search_evidence(question)
            if _is_report_question(question):
                summary = self.generate_ai_summary(question, df, analyst.evidence)
                if summary:
                    analyst.answer = summary
            if not analyst.answer:
                analyst.answer = "Cortex Analyst generated SQL and returned the results below."
            return analyst
        fallback = self.answer_with_curated_sql(question)
        fallback.caveats = analyst.caveats + fallback.caveats
        fallback.diagnostics.update(
            {
                "attempts": 1,
                "cortex_sql_generated": False,
                "cortex_sql_normalized": False,
                "fallback_reason": "cortex_analyst_no_sql",
                "retry_strategy": "curated_sql",
            }
        )
        return fallback

    def ask_cortex_analyst(self, question: str) -> AssistantResponse:
        if self.config is None or not self.config.pat:
            return AssistantResponse(
                answer="",
                caveats=["Cortex Analyst REST was skipped because SNOWFLAKE_PAT is not configured."],
                route="snowflake-curated-sql",
            )

        url = f"https://{self.config.account}.snowflakecomputing.com/api/v2/cortex/analyst/message"
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}],
                }
            ],
            "semantic_model_file": self.config.semantic_model,
        }
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {self.config.pat}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=45,
        )
        if response.status_code >= 400:
            detail = response.text[:300].replace("\n", " ")
            return AssistantResponse(
                answer="",
                caveats=[f"Cortex Analyst REST returned HTTP {response.status_code}: {detail}; using curated SQL fallback."],
                route="snowflake-curated-sql",
            )

        body = response.json()
        sql, text, suggestions = _extract_analyst_content(body)
        answer = text or ("Cortex Analyst generated SQL for this question." if sql else "")
        if suggestions:
            answer += "\n\nCortex Analyst needs clarification: " + "; ".join(suggestions[:3])
        return AssistantResponse(
            answer=answer,
            generated_sql=sql,
            caveats=[] if sql else ["Cortex Analyst did not return SQL; using curated SQL fallback."],
            route="cortex-analyst",
        )

    def search_evidence(self, question: str, limit: int = 6) -> pd.DataFrame:
        if self.config is None:
            return pd.DataFrame()
        params = json.dumps(
            {
                "query": question,
                "columns": ["SOURCE_TYPE", "SOURCE_ID", "SQUADRON_NAME", "AIRCRAFT_ID", "EVENT_DATE", "SEVERITY", "DOCUMENT_TEXT"],
                "limit": limit,
            }
        )
        sql = f"""
        SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            '{self.config.cortex_search_service}',
            '{_sql_literal(params)}'
        ) AS SEARCH_RESPONSE
        """
        try:
            raw = self.execute_scalar(sql)
        except Exception as exc:
            return pd.DataFrame(
                {
                    "notice": [
                        "Cortex Search evidence is unavailable. Run sql/03_cortex_objects.sql or continue with SQL-only Snowflake mode."
                    ],
                    "detail": [str(exc).splitlines()[0]],
                }
            )
        try:
            parsed = json.loads(raw)
            return pd.DataFrame(parsed.get("results", []))
        except Exception:
            return pd.DataFrame({"raw_search_response": [raw]})

    def generate_ai_summary(self, question: str, table: pd.DataFrame, evidence: pd.DataFrame | None) -> str:
        if self.config is None:
            return ""
        context = {
            "question": question,
            "result_rows": table.head(20).to_dict(orient="records"),
            "evidence": [] if evidence is None else evidence.head(10).to_dict(orient="records"),
        }
        prompt = (
            "You are a unit operations analyst. Answer the user's question using only this JSON context. "
            "Be concise, include operational implications, and separate evidence from inference. Context: "
            + json.dumps(context, default=str)
        )
        sql = f"SELECT AI_COMPLETE('{self.config.cortex_model}', '{_sql_literal(prompt)}')"
        try:
            return self.execute_scalar(sql)
        except Exception:
            return ""

    def answer_with_curated_sql(self, question: str) -> AssistantResponse:
        lowered = question.lower()
        caveats = (
            ["Using curated SQL fallback after Cortex Analyst did not return executable SQL."]
            if self.config and self.config.pat
            else ["Using curated SQL fallback. Configure SNOWFLAKE_PAT to enable Cortex Analyst NL-to-SQL."]
        )
        diagnostics = {
            "attempts": 1,
            "cortex_sql_generated": False,
            "cortex_sql_normalized": False,
            "fallback_reason": "curated_sql_route",
            "retry_strategy": "curated_sql",
        }

        if "highest" in lowered and "success" in lowered:
            sql = """
            SELECT squadron_name, mission_count, success_count,
                   ROUND(success_count / NULLIF(mission_count, 0), 3) AS success_rate,
                   avg_mission_readiness,
                   delayed_mission_count
            FROM VW_SQUADRON_MISSION_PERFORMANCE
            ORDER BY success_rate DESC
            LIMIT 5
            """
            df = self.query_df(sql)
            top = df.iloc[0]
            return AssistantResponse(
                answer=f"{top['SQUADRON_NAME']} has the highest success rate at {top['SUCCESS_RATE']:.0%}.",
                result_table=df,
                generated_sql=sql.strip(),
                evidence=self.search_evidence(question),
                caveats=caveats,
                route="snowflake-curated-sql",
                diagnostics=diagnostics,
            )

        if "delay" in lowered or "delayed" in lowered:
            sql = """
            SELECT squadron_name, delay_reason, COUNT(*) AS delayed_missions,
                   SUM(delay_minutes) AS total_delay_minutes,
                   AVG(readiness_score) AS avg_readiness
            FROM MISSIONS
            WHERE delay_minutes > 0
            GROUP BY squadron_name, delay_reason
            ORDER BY total_delay_minutes DESC
            """
            df = self.query_df(sql)
            evidence = self.search_evidence(question)
            answer = self.generate_ai_summary(question, df, evidence)
            if not answer:
                top = df.iloc[0]
                answer = (
                    f"The largest delay cluster is {top['DELAY_REASON']} for {top['SQUADRON_NAME']}, "
                    f"totaling {int(top['TOTAL_DELAY_MINUTES'])} minutes across {int(top['DELAYED_MISSIONS'])} missions."
                )
                caveats.append("AI_COMPLETE is unavailable, so this response uses SQL summary fallback.")
            return AssistantResponse(answer, df, sql.strip(), evidence, caveats, "snowflake-curated-sql", diagnostics=diagnostics)

        if "report" in lowered or "weekly" in lowered:
            sql = "SELECT * FROM VW_SQUADRON_RISK ORDER BY risk_score DESC"
            df = self.query_df(sql)
            evidence = self.search_evidence(question)
            answer = self.generate_ai_summary(question, df, evidence)
            if not answer:
                top = df.iloc[0]
                answer = (
                    f"{top['SQUADRON_NAME']} has the highest current operational risk score at {top['RISK_SCORE']}. "
                    "Review the risk table below for mission, readiness, delay, and personnel drivers."
                )
                caveats.append("AI_COMPLETE is unavailable, so this response uses SQL summary fallback.")
            return AssistantResponse(
                answer,
                df,
                sql,
                evidence,
                caveats,
                "snowflake-curated-sql",
                diagnostics=diagnostics,
            )

        if "part" in lowered or "supply" in lowered or "inventory" in lowered:
            sql = """
            SELECT part_name, aircraft_id, quantity_on_hand, reorder_point, expected_restock_date, priority
            FROM PARTS_INVENTORY
            WHERE quantity_on_hand <= reorder_point
            ORDER BY quantity_on_hand ASC, priority ASC
            """
            df = self.query_df(sql)
            return AssistantResponse(
                "Parts below reorder point are listed below.",
                df,
                sql.strip(),
                self.search_evidence(question),
                caveats,
                "snowflake-curated-sql",
                diagnostics=diagnostics,
            )

        sql = """
        SELECT aircraft_id, squadron_name, aircraft_type, readiness_state, readiness_score, open_maintenance_items
        FROM AIRCRAFT_READINESS
        WHERE readiness_state <> 'READY' OR readiness_score < 70 OR open_maintenance_items >= 3
        ORDER BY readiness_score ASC
        """
        df = self.query_df(sql)
        return AssistantResponse(
            "Readiness anomalies are listed below with retrieved operational context.",
            df,
            sql.strip(),
            self.search_evidence(question),
            caveats,
            "snowflake-curated-sql",
            diagnostics=diagnostics,
        )


def _extract_analyst_content(body: dict[str, Any]) -> tuple[str | None, str, list[str]]:
    message = body.get("message", {})
    content = message.get("content") or body.get("content") or []
    sql: str | None = None
    text_parts: list[str] = []
    suggestions: list[str] = []

    def visit(value: Any) -> None:
        nonlocal sql
        if isinstance(value, dict):
            block_type = value.get("type")
            if block_type == "sql":
                sql = value.get("statement") or value.get("sql") or sql
            elif block_type == "text":
                text = value.get("text")
                if text:
                    text_parts.append(text)
            elif block_type in {"suggestion", "suggestions"}:
                suggestion_value = value.get("suggestions") or value.get("suggestion") or []
                suggestions.extend(suggestion_value if isinstance(suggestion_value, list) else [str(suggestion_value)])
            if "statement" in value and isinstance(value["statement"], str):
                sql = value["statement"]
            if "sql" in value and isinstance(value["sql"], str):
                sql = value["sql"]
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    for block in content:
        visit(block)
    return sql, "\n\n".join(text_parts), suggestions


def _sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _normalize_cortex_sql(sql: str) -> str:
    replacements = {
        "__missions": "MISSIONS",
        "__aircraft_readiness": "AIRCRAFT_READINESS",
        "__incident_reports": "INCIDENT_REPORTS",
        "__parts_inventory": "PARTS_INVENTORY",
        "__personnel_availability": "PERSONNEL_AVAILABILITY",
    }
    normalized = sql
    for logical_name, table_name in replacements.items():
        normalized = normalized.replace(logical_name, table_name)
    normalized = normalized.replace("success_indicator", "IFF(success_flag, 1, 0)")
    return normalized


def _split_sql_statements(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]


def _is_report_question(question: str) -> bool:
    lowered = question.lower()
    return "report" in lowered or "weekly" in lowered or "summarize" in lowered
