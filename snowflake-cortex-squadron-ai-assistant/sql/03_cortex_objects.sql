USE DATABASE SQUADRON_AI_DB;
USE SCHEMA OPERATIONS;

CREATE OR REPLACE VIEW VW_OPERATIONAL_TEXT_CORPUS AS
SELECT
    'MAINTENANCE' AS source_type,
    TO_VARCHAR(log_id) AS source_id,
    squadron_name,
    aircraft_id,
    log_date AS event_date,
    severity,
    log_text AS document_text
FROM MAINTENANCE_LOGS
UNION ALL
SELECT
    'MISSION' AS source_type,
    TO_VARCHAR(mission_id) AS source_id,
    squadron_name,
    aircraft_id,
    mission_date AS event_date,
    status AS severity,
    mission_notes AS document_text
FROM MISSIONS
UNION ALL
SELECT
    'INCIDENT' AS source_type,
    TO_VARCHAR(incident_id) AS source_id,
    squadron_name,
    aircraft_id,
    incident_date AS event_date,
    severity,
    report_text AS document_text
FROM INCIDENT_REPORTS;

CREATE OR REPLACE CORTEX SEARCH SERVICE OPERATIONAL_SEARCH_SERVICE
    ON document_text
    ATTRIBUTES source_type, source_id, squadron_name, aircraft_id, event_date, severity
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 hour'
AS
SELECT source_type, source_id, squadron_name, aircraft_id, event_date, severity, document_text
FROM VW_OPERATIONAL_TEXT_CORPUS;

-- AI_COMPLETE: summarize delayed missions and likely causes.
SELECT AI_COMPLETE(
    'claude-4-sonnet',
    'Summarize delayed missions and likely causes for unit operations leadership: '
    || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
) AS delayed_mission_summary
FROM (
    SELECT mission_id, squadron_name, mission_type, mission_date, delay_minutes, delay_reason, mission_notes
    FROM MISSIONS
    WHERE delay_minutes > 0
);

-- AI_COMPLETE: generate a weekly unit operations report.
SELECT AI_COMPLETE(
    'claude-4-sonnet',
    'Generate a weekly unit operations report with mission performance, readiness risks, personnel constraints, and recommendations: '
    || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
) AS weekly_operations_report
FROM VW_SQUADRON_MISSION_PERFORMANCE;

-- Cortex Search preview query for maintenance context.
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'SQUADRON_AI_DB.OPERATIONS.OPERATIONAL_SEARCH_SERVICE',
        '{"query": "hydraulic avionics engine readiness delay", "columns": ["SOURCE_TYPE", "SQUADRON_NAME", "AIRCRAFT_ID", "DOCUMENT_TEXT"], "limit": 5}'
    )
) AS search_results;
