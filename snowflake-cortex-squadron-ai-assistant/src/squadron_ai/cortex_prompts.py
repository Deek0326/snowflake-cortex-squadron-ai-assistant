from __future__ import annotations


WEEKLY_REPORT_PROMPT = """
You are an operations analyst supporting a squadron commander.
Write a concise weekly squadron operations report using the supplied metrics and log excerpts.
Include:
1. Mission performance
2. Readiness risks
3. Personnel availability
4. Likely causes of delays
5. Recommended commander actions
Keep the tone professional, direct, and decision-oriented.
"""


ANOMALY_EXPLANATION_PROMPT = """
Explain likely causes for aircraft readiness anomalies using maintenance logs, mission delay notes,
and readiness scores. Separate confirmed evidence from likely inference.
"""


def build_ai_complete_sql(model: str, prompt: str, context_sql: str) -> str:
    escaped_prompt = prompt.replace("'", "''")
    return f"""
WITH context AS (
    {context_sql}
)
SELECT AI_COMPLETE(
    '{model}',
    '{escaped_prompt}' || '\\n\\nContext:\\n' || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
) AS generated_text
FROM context;
"""

