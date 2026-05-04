from __future__ import annotations

from squadron_ai.analytics import (
    answer_demo_question,
    delayed_missions,
    mission_success_by_squadron,
    operational_risk_by_squadron,
    readiness_anomalies,
    search_operational_evidence,
)
from squadron_ai.demo_data import load_demo_dataset


def test_success_rate_ranks_raptor_first():
    dataset = load_demo_dataset()
    result = mission_success_by_squadron(dataset.missions)

    assert result.iloc[0]["squadron"] == "Raptor"
    assert result.iloc[0]["success_rate"] > 0.7


def test_delayed_missions_only_returns_delays():
    dataset = load_demo_dataset()
    result = delayed_missions(dataset.missions)

    assert len(result) > 0
    assert (result["delay_minutes"] > 0).all()


def test_readiness_anomalies_flags_falcon_aircraft():
    dataset = load_demo_dataset()
    result = readiness_anomalies(dataset.readiness)

    aircraft_ids = set(result["aircraft_id"])
    assert {"FX-201", "FX-202"}.issubset(aircraft_ids)
    assert result.iloc[0]["severity"] in {"High", "Critical"}


def test_operational_risk_scores_squadrons():
    dataset = load_demo_dataset()
    result = operational_risk_by_squadron(dataset)

    assert "risk_score" in result.columns
    assert result.iloc[0]["risk_score"] >= result.iloc[-1]["risk_score"]


def test_search_finds_hydraulic_evidence():
    dataset = load_demo_dataset()
    result = search_operational_evidence(dataset, "hydraulic leak")

    assert len(result) > 0
    assert result["document_text"].str.lower().str.contains("hydraulic").any()


def test_demo_answer_includes_sql_and_evidence():
    dataset = load_demo_dataset()
    response = answer_demo_question("Summarize delayed missions and likely causes.", dataset)

    assert response.answer
    assert response.generated_sql
    assert response.evidence is not None
    assert response.result_table is not None

