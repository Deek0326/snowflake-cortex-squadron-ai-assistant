from __future__ import annotations

import json
import os
from dataclasses import dataclass
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
    load_dotenv()
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
            if self.config is None or self.config.password is None:
                raise RuntimeError("Snowflake password credentials are not configured.")
            import snowflake.connector

            self._conn = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                role=self.config.role,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
            )
        return self._conn

    def query_df(self, sql: str) -> pd.DataFrame:
        if self._session is not None:
            return self._session.sql(sql).to_pandas()
        conn = self.connect()
        return pd.read_sql(sql, conn)

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
            df = self.query_df(analyst.generated_sql)
            analyst.result_table = df
            analyst.evidence = self.search_evidence(question)
            if _is_report_question(question):
                analyst.answer = self.generate_ai_summary(question, df, analyst.evidence)
            elif not analyst.answer:
                analyst.answer = "Cortex Analyst generated SQL and returned the results below."
            return analyst
        return self.answer_with_curated_sql(question)

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
            return AssistantResponse(
                answer="",
                caveats=[f"Cortex Analyst REST returned HTTP {response.status_code}; using curated SQL fallback."],
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
        raw = self.execute_scalar(sql)
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
            "You are a squadron operations analyst. Answer the user's question using only this JSON context. "
            "Be concise, include operational implications, and separate evidence from inference. Context: "
            + json.dumps(context, default=str)
        )
        sql = f"SELECT AI_COMPLETE('{self.config.cortex_model}', '{_sql_literal(prompt)}')"
        return self.execute_scalar(sql)

    def answer_with_curated_sql(self, question: str) -> AssistantResponse:
        lowered = question.lower()
        caveats = ["Using curated SQL fallback. Configure SNOWFLAKE_PAT to enable Cortex Analyst NL-to-SQL."]

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
            answer = self.generate_ai_summary(question, df, self.search_evidence(question))
            return AssistantResponse(answer, df, sql.strip(), self.search_evidence(question), caveats, "snowflake-ai-complete")

        if "report" in lowered or "weekly" in lowered:
            sql = "SELECT * FROM VW_SQUADRON_RISK ORDER BY risk_score DESC"
            df = self.query_df(sql)
            evidence = self.search_evidence(question)
            return AssistantResponse(
                self.generate_ai_summary(question, df, evidence),
                df,
                sql,
                evidence,
                caveats,
                "snowflake-ai-complete",
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
        )


def _extract_analyst_content(body: dict[str, Any]) -> tuple[str | None, str, list[str]]:
    message = body.get("message", {})
    content = message.get("content", [])
    sql: str | None = None
    text_parts: list[str] = []
    suggestions: list[str] = []
    for block in content:
        block_type = block.get("type")
        if block_type == "sql":
            sql = block.get("statement") or block.get("sql")
        elif block_type == "text":
            text = block.get("text")
            if text:
                text_parts.append(text)
        elif block_type in {"suggestion", "suggestions"}:
            value = block.get("suggestions") or block.get("suggestion") or []
            suggestions.extend(value if isinstance(value, list) else [str(value)])
    return sql, "\n\n".join(text_parts), suggestions


def _sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _is_report_question(question: str) -> bool:
    lowered = question.lower()
    return "report" in lowered or "weekly" in lowered or "summarize" in lowered
