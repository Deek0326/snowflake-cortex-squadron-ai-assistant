CREATE DATABASE IF NOT EXISTS SQUADRON_AI_DB;
CREATE SCHEMA IF NOT EXISTS SQUADRON_AI_DB.OPERATIONS;

USE DATABASE SQUADRON_AI_DB;
USE SCHEMA OPERATIONS;

CREATE OR REPLACE STAGE SEMANTIC_MODELS
    DIRECTORY = (ENABLE = TRUE);

CREATE OR REPLACE TABLE SQUADRONS (
    squadron_id NUMBER AUTOINCREMENT,
    squadron_name VARCHAR,
    base_location VARCHAR,
    commander VARCHAR,
    primary_mission VARCHAR
);

CREATE OR REPLACE TABLE MISSIONS (
    mission_id NUMBER AUTOINCREMENT,
    squadron_name VARCHAR,
    mission_type VARCHAR,
    aircraft_id VARCHAR,
    mission_date DATE,
    status VARCHAR,
    success_flag BOOLEAN,
    readiness_score NUMBER(5,2),
    delay_minutes NUMBER,
    delay_reason VARCHAR,
    mission_notes VARCHAR
);

CREATE OR REPLACE TABLE AIRCRAFT_READINESS (
    aircraft_id VARCHAR,
    squadron_name VARCHAR,
    aircraft_type VARCHAR,
    snapshot_date DATE,
    readiness_state VARCHAR,
    readiness_score NUMBER(5,2),
    open_maintenance_items NUMBER,
    next_inspection_date DATE
);

CREATE OR REPLACE TABLE PERSONNEL_AVAILABILITY (
    squadron_name VARCHAR,
    snapshot_date DATE,
    assigned_personnel NUMBER,
    available_personnel NUMBER,
    pilots_available NUMBER,
    maintenance_crew_available NUMBER
);

CREATE OR REPLACE TABLE MAINTENANCE_LOGS (
    log_id NUMBER AUTOINCREMENT,
    aircraft_id VARCHAR,
    squadron_name VARCHAR,
    log_date DATE,
    severity VARCHAR,
    log_text VARCHAR
);

CREATE OR REPLACE TABLE INCIDENT_REPORTS (
    incident_id NUMBER AUTOINCREMENT,
    squadron_name VARCHAR,
    aircraft_id VARCHAR,
    incident_date DATE,
    severity VARCHAR,
    report_text VARCHAR
);

CREATE OR REPLACE TABLE PARTS_INVENTORY (
    part_name VARCHAR,
    aircraft_id VARCHAR,
    quantity_on_hand NUMBER,
    reorder_point NUMBER,
    expected_restock_date DATE,
    priority VARCHAR
);

CREATE OR REPLACE VIEW VW_SQUADRON_MISSION_PERFORMANCE AS
SELECT
    squadron_name,
    COUNT(*) AS mission_count,
    SUM(IFF(success_flag, 1, 0)) AS success_count,
    AVG(readiness_score) AS avg_mission_readiness,
    AVG(delay_minutes) AS avg_delay_minutes,
    COUNT_IF(delay_minutes > 0) AS delayed_mission_count
FROM MISSIONS
GROUP BY squadron_name;

CREATE OR REPLACE VIEW VW_SQUADRON_RISK AS
WITH mission_perf AS (
    SELECT *
    FROM VW_SQUADRON_MISSION_PERFORMANCE
),
personnel AS (
    SELECT
        squadron_name,
        SUM(available_personnel) / NULLIF(SUM(assigned_personnel), 0) AS personnel_availability_rate
    FROM PERSONNEL_AVAILABILITY
    GROUP BY squadron_name
),
readiness AS (
    SELECT
        squadron_name,
        COUNT_IF(readiness_state <> 'READY' OR readiness_score < 70 OR open_maintenance_items >= 3) AS readiness_anomalies
    FROM AIRCRAFT_READINESS
    GROUP BY squadron_name
),
delays AS (
    SELECT squadron_name, SUM(delay_minutes) AS total_delay_minutes
    FROM MISSIONS
    WHERE delay_minutes > 0
    GROUP BY squadron_name
)
SELECT
    m.squadron_name,
    m.mission_count,
    m.success_count,
    m.success_count / NULLIF(m.mission_count, 0) AS success_rate,
    m.avg_mission_readiness,
    m.delayed_mission_count,
    COALESCE(p.personnel_availability_rate, 0) AS personnel_availability_rate,
    COALESCE(r.readiness_anomalies, 0) AS readiness_anomalies,
    COALESCE(d.total_delay_minutes, 0) AS total_delay_minutes,
    ROUND(
        (1 - (m.success_count / NULLIF(m.mission_count, 0))) * 40
        + (1 - COALESCE(p.personnel_availability_rate, 1)) * 25
        + COALESCE(r.readiness_anomalies, 0) * 12
        + COALESCE(d.total_delay_minutes, 0) / 10,
        1
    ) AS risk_score
FROM mission_perf m
LEFT JOIN personnel p USING (squadron_name)
LEFT JOIN readiness r USING (squadron_name)
LEFT JOIN delays d USING (squadron_name);
