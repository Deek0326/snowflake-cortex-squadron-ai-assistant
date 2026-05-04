from __future__ import annotations

import re
from dataclasses import dataclass, field

import pandas as pd

from .demo_data import DemoDataset


@dataclass
class AssistantResponse:
    answer: str
    result_table: pd.DataFrame | None = None
    generated_sql: str | None = None
    evidence: pd.DataFrame | None = None
    caveats: list[str] = field(default_factory=list)
    route: str = "demo"


def mission_success_by_squadron(missions: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        missions.assign(success=missions["success_flag"].astype(int))
        .groupby("squadron", as_index=False)
        .agg(
            missions=("mission_id", "count"),
            successes=("success", "sum"),
            avg_readiness=("readiness_score", "mean"),
            avg_delay_minutes=("delay_minutes", "mean"),
            delayed_missions=("delay_minutes", lambda values: int((values > 0).sum())),
        )
    )
    grouped["success_rate"] = grouped["successes"] / grouped["missions"]
    return grouped.sort_values(["success_rate", "avg_readiness"], ascending=[False, False])


def delayed_missions(missions: pd.DataFrame) -> pd.DataFrame:
    return missions.loc[missions["delay_minutes"] > 0].sort_values("delay_minutes", ascending=False)


def delay_causes(missions: pd.DataFrame) -> pd.DataFrame:
    delayed = delayed_missions(missions)
    return (
        delayed.groupby(["squadron", "delay_reason"], dropna=False, as_index=False)
        .agg(
            delayed_missions=("mission_id", "count"),
            total_delay_minutes=("delay_minutes", "sum"),
            avg_readiness=("readiness_score", "mean"),
        )
        .sort_values("total_delay_minutes", ascending=False)
    )


def readiness_anomalies(readiness: pd.DataFrame) -> pd.DataFrame:
    scored = readiness.copy()
    type_mean = scored.groupby("aircraft_type")["readiness_score"].transform("mean")
    type_std = scored.groupby("aircraft_type")["readiness_score"].transform(lambda values: values.std(ddof=0)).replace(0, 1)
    scored["readiness_z_score"] = ((scored["readiness_score"] - type_mean) / type_std).round(2)
    scored["severity_score"] = (
        (100 - scored["readiness_score"])
        + scored["open_maintenance_items"].fillna(0) * 6
        + scored["readiness_state"].map({"Down": 35, "Limited": 18, "Ready": 0}).fillna(10)
    )
    scored["severity"] = pd.cut(
        scored["severity_score"],
        bins=[-1, 35, 60, 90, 200],
        labels=["Low", "Medium", "High", "Critical"],
    ).astype(str)
    anomalies = scored.loc[
        (scored["readiness_state"] != "Ready")
        | (scored["readiness_score"] < 70)
        | (scored["open_maintenance_items"] >= 3)
        | (scored["readiness_z_score"] <= -1)
    ].copy()
    anomalies["anomaly_reason"] = anomalies.apply(_readiness_reason, axis=1)
    return anomalies.sort_values(["severity_score", "readiness_score"], ascending=[False, True])


def _readiness_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if row["readiness_state"] != "Ready":
        reasons.append(f"{row['readiness_state']} readiness state")
    if row["readiness_score"] < 70:
        reasons.append("readiness score below 70")
    if row["open_maintenance_items"] >= 3:
        reasons.append("high open maintenance count")
    if row["readiness_z_score"] <= -1:
        reasons.append("below aircraft-type baseline")
    return ", ".join(reasons) or "watchlist"


def operational_risk_by_squadron(dataset: DemoDataset) -> pd.DataFrame:
    success = mission_success_by_squadron(dataset.missions)
    anomalies = readiness_anomalies(dataset.readiness)
    delayed = delayed_missions(dataset.missions)
    personnel = dataset.personnel.copy()

    risk = success.merge(personnel[["squadron", "availability_rate"]], on="squadron", how="left")
    risk = risk.merge(
        anomalies.groupby("squadron", as_index=False).agg(readiness_anomalies=("aircraft_id", "count")),
        on="squadron",
        how="left",
    )
    risk = risk.merge(
        delayed.groupby("squadron", as_index=False).agg(total_delay_minutes=("delay_minutes", "sum")),
        on="squadron",
        how="left",
    )
    risk[["readiness_anomalies", "total_delay_minutes"]] = risk[
        ["readiness_anomalies", "total_delay_minutes"]
    ].fillna(0)
    risk["risk_score"] = (
        (1 - risk["success_rate"]) * 40
        + (1 - risk["availability_rate"]) * 25
        + risk["readiness_anomalies"] * 12
        + risk["total_delay_minutes"] / 10
    ).round(1)
    return risk.sort_values("risk_score", ascending=False)


def operational_corpus(dataset: DemoDataset) -> pd.DataFrame:
    logs = dataset.maintenance_logs.rename(columns={"log_id": "source_id", "log_date": "event_date", "log_text": "document_text"})
    logs = logs.assign(source_type="MAINTENANCE")[["source_type", "source_id", "squadron", "aircraft_id", "event_date", "severity", "document_text"]]

    incidents = dataset.incident_reports.rename(
        columns={"incident_id": "source_id", "incident_date": "event_date", "report_text": "document_text"}
    )
    incidents = incidents.assign(source_type="INCIDENT")[
        ["source_type", "source_id", "squadron", "aircraft_id", "event_date", "severity", "document_text"]
    ]

    missions = dataset.missions.rename(columns={"mission_id": "source_id", "mission_date": "event_date", "mission_notes": "document_text"})
    missions = missions.assign(source_type="MISSION", severity=missions["status"])[
        ["source_type", "source_id", "squadron", "aircraft_id", "event_date", "severity", "document_text"]
    ]
    return pd.concat([logs, incidents, missions], ignore_index=True)


def search_operational_evidence(dataset: DemoDataset, query: str, limit: int = 6) -> pd.DataFrame:
    corpus = operational_corpus(dataset)
    tokens = [token for token in re.findall(r"[a-z0-9]+", query.lower()) if len(token) > 2]
    domain_boosts = {
        "delay": ["delay", "delayed", "late", "preflight"],
        "readiness": ["ready", "limited", "down", "inspection", "warning"],
        "maintenance": ["hydraulic", "avionics", "engine", "parts", "sensor", "warning"],
        "anomaly": ["unexpected", "recurred", "warning", "down", "leak"],
    }
    for token in list(tokens):
        tokens.extend(domain_boosts.get(token, []))
    tokens = sorted(set(tokens))

    if not tokens:
        return corpus.sort_values("event_date", ascending=False).head(limit)

    scored = corpus.copy()
    scored["search_score"] = scored["document_text"].str.lower().apply(lambda text: sum(token in text for token in tokens))
    scored["search_score"] += scored["severity"].map({"Critical": 3, "High": 2, "Medium": 1}).fillna(0)
    return (
        scored.loc[scored["search_score"] > 0]
        .sort_values(["search_score", "event_date"], ascending=[False, False])
        .head(limit)
    )


def kpis(dataset: DemoDataset) -> dict[str, str]:
    success = mission_success_by_squadron(dataset.missions)
    delayed = delayed_missions(dataset.missions)
    anomalies = readiness_anomalies(dataset.readiness)
    availability = dataset.personnel["availability_rate"].mean()
    risk = operational_risk_by_squadron(dataset).iloc[0]
    return {
        "Top squadron": str(success.iloc[0]["squadron"]),
        "Success rate": f"{success.iloc[0]['success_rate']:.0%}",
        "Delayed missions": str(len(delayed)),
        "Readiness anomalies": str(len(anomalies)),
        "Highest risk": str(risk["squadron"]),
        "Personnel availability": f"{availability:.0%}",
    }


def generate_weekly_report(dataset: DemoDataset) -> str:
    success = mission_success_by_squadron(dataset.missions)
    delayed = delayed_missions(dataset.missions)
    anomalies = readiness_anomalies(dataset.readiness)
    risk = operational_risk_by_squadron(dataset)
    top = success.iloc[0]
    highest_risk = risk.iloc[0]
    delay_summary = delay_causes(dataset.missions).head(3)
    delay_text = "; ".join(
        f"{row.squadron} {row.delay_reason}: {int(row.total_delay_minutes)} minutes"
        for row in delay_summary.itertuples()
    )
    return (
        f"Weekly squadron operations report: {top['squadron']} leads mission execution with a "
        f"{top['success_rate']:.0%} success rate. {highest_risk['squadron']} carries the highest operational "
        f"risk because its success rate, personnel availability, readiness anomalies, and delay load combine to "
        f"a risk score of {highest_risk['risk_score']}. Across the wing, {len(delayed)} missions were delayed "
        f"and {len(anomalies)} aircraft readiness anomalies require attention. Primary delay drivers are {delay_text}. "
        "Recommended actions: prioritize Falcon flight-control and hydraulic parts, keep RX-302 on shortened "
        "inspection intervals, protect Viper sensor restock, and review crew availability before high-tempo sorties."
    )


def answer_demo_question(question: str, dataset: DemoDataset) -> AssistantResponse:
    lowered = question.lower()
    evidence = search_operational_evidence(dataset, question)

    if "highest" in lowered and "success" in lowered:
        table = mission_success_by_squadron(dataset.missions)
        top = table.iloc[0]
        return AssistantResponse(
            answer=(
                f"{top['squadron']} has the highest mission success rate at {top['success_rate']:.0%} "
                f"across {int(top['missions'])} missions. Its average mission readiness was "
                f"{top['avg_readiness']:.1f}, with {int(top['delayed_missions'])} delayed missions."
            ),
            result_table=table,
            generated_sql=DEMO_SQL["success_rate"],
            evidence=evidence,
            caveats=["Demo mode uses local sample data. Snowflake mode can use Cortex Analyst-generated SQL."],
        )

    if "delayed" in lowered or "delay" in lowered:
        table = delayed_missions(dataset.missions)
        causes = delay_causes(dataset.missions)
        top_cause = causes.iloc[0]
        return AssistantResponse(
            answer=(
                f"There are {len(table)} delayed missions. The largest delay cluster is "
                f"{top_cause['delay_reason']} for {top_cause['squadron']}, totaling "
                f"{int(top_cause['total_delay_minutes'])} minutes. Maintenance, parts, weather, and crew "
                "availability are the strongest likely causes."
            ),
            result_table=causes,
            generated_sql=DEMO_SQL["delays"],
            evidence=evidence,
            caveats=["Likely causes combine explicit delay reasons with related maintenance and incident evidence."],
        )

    if "anomal" in lowered or "readiness" in lowered:
        table = readiness_anomalies(dataset.readiness)
        critical = table.iloc[0]
        return AssistantResponse(
            answer=(
                f"I found {len(table)} readiness anomalies. The highest-priority item is "
                f"{critical['aircraft_id']} in {critical['squadron']} with severity {critical['severity']}: "
                f"{critical['anomaly_reason']}."
            ),
            result_table=table,
            generated_sql=DEMO_SQL["readiness_anomalies"],
            evidence=evidence,
            caveats=["Severity is a transparent demo score, not an official maintenance classification."],
        )

    if "report" in lowered or "weekly" in lowered:
        return AssistantResponse(
            answer=generate_weekly_report(dataset),
            result_table=operational_risk_by_squadron(dataset),
            generated_sql=DEMO_SQL["weekly_report"],
            evidence=evidence,
            caveats=["In Snowflake mode, AI_COMPLETE can generate this report from live metrics and retrieved text."],
        )

    if "part" in lowered or "supply" in lowered or "inventory" in lowered:
        parts = dataset.parts_inventory.sort_values(["quantity_on_hand", "priority"])
        return AssistantResponse(
            answer=(
                "Parts risk is concentrated in flight-control, hydraulic, and avionics items. "
                "FX-202 has no flight-control actuator on hand, and FX-201 is near the hydraulic seal kit floor."
            ),
            result_table=parts,
            generated_sql=DEMO_SQL["parts"],
            evidence=evidence,
            caveats=["Inventory values are sample records meant to demonstrate supply-aware operations analysis."],
        )

    return AssistantResponse(
        answer="I searched operational notes, incidents, and maintenance logs for the most relevant evidence.",
        result_table=evidence,
        generated_sql=DEMO_SQL["search"],
        evidence=evidence,
        caveats=["This path behaves like a local approximation of Cortex Search retrieval."],
    )


DEMO_SQL = {
    "success_rate": """
SELECT squadron_name,
       COUNT(*) AS mission_count,
       SUM(IFF(success_flag, 1, 0)) AS success_count,
       SUM(IFF(success_flag, 1, 0)) / NULLIF(COUNT(*), 0) AS success_rate
FROM MISSIONS
WHERE mission_date >= DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE))
  AND mission_date < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY squadron_name
ORDER BY success_rate DESC
LIMIT 1;
""".strip(),
    "delays": """
SELECT squadron_name, delay_reason, COUNT(*) AS delayed_missions, SUM(delay_minutes) AS total_delay_minutes
FROM MISSIONS
WHERE delay_minutes > 0
GROUP BY squadron_name, delay_reason
ORDER BY total_delay_minutes DESC;
""".strip(),
    "readiness_anomalies": """
SELECT aircraft_id, squadron_name, aircraft_type, readiness_state, readiness_score, open_maintenance_items
FROM AIRCRAFT_READINESS
WHERE readiness_state <> 'READY' OR readiness_score < 70 OR open_maintenance_items >= 3
ORDER BY readiness_score ASC;
""".strip(),
    "weekly_report": """
SELECT AI_COMPLETE(
  '<model>',
  'Generate a weekly squadron operations report from metrics and retrieved evidence: '
  || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
)
FROM VW_SQUADRON_RISK;
""".strip(),
    "parts": """
SELECT part_name, aircraft_id, quantity_on_hand, reorder_point, expected_restock_date, priority
FROM PARTS_INVENTORY
WHERE quantity_on_hand <= reorder_point
ORDER BY quantity_on_hand ASC, priority ASC;
""".strip(),
    "search": """
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
  'SQUADRON_AI_DB.OPERATIONS.OPERATIONAL_SEARCH_SERVICE',
  '{"query": "<user question>", "columns": ["SOURCE_TYPE", "SQUADRON_NAME", "AIRCRAFT_ID", "DOCUMENT_TEXT"], "limit": 6}'
);
""".strip(),
}
