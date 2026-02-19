# Oilfield Incident Intelligence - Demo Guide

This document showcases the capabilities of the multi-agent AI system built for the **Elasticsearch Agent Builder Hackathon 2026**.

---

## Architecture: Multi-Agent Pipeline

```
User Query
    |
    v
[Agent 1: OilField Incident Retriever]
    - Semantic search via ELSER-2 on `oilfield-incidents` index
    - ES|QL analytics for pattern detection
    - Identifies matching incidents, root causes & remediation
    |
    v
[Agent 2: OilField Incident Validator]
    - Cross-checks sensor thresholds (vibration Hz, temp C, pressure PSI)
    - Validates recurrence patterns
    - Generates action plans with workflow trigger recommendations
    |
    v
Structured Incident Analysis Report
```

---

## Index: `oilfield-incidents`

| Field | Type | Description |
|---|---|---|
| `description` | Semantic Text (ELSER-2) | Full incident narrative (enables AI search) |
| `incident_id` | Keyword | Unique identifier (e.g., INC-001) |
| `incident_type` | Keyword | Category (ESP_FAILURE, H2S_GAS_RELEASE, etc.) |
| `location` | Keyword | Platform/field name |
| `well_id` | Keyword | Well identifier |
| `equipment_id` | Keyword | Specific equipment unit |
| `sensor_vibration_hz` | Float | Vibration sensor reading |
| `sensor_temp_celsius` | Float | Temperature sensor reading |
| `sensor_pressure_psi` | Float | Pressure sensor reading |
| `severity` | Keyword | LOW / MEDIUM / HIGH / CRITICAL |
| `root_cause` | Text | Diagnosed root cause |
| `remediation` | Text | Steps taken to resolve |
| `assigned_team` | Keyword | Team responsible |
| `cost_usd` | Float | Financial impact |
| `downtime_hours` | Float | Operational downtime |
| `recurrence_count` | Integer | Number of times issue recurred |

---

## Sample Queries & Agent Responses

### Query 1: Incident Pattern Matching
**User:** `We have a pump on Block 7 Platform Alpha showing high vibration at 150 Hz and it stopped suddenly. Find similar past incidents and tell me the most likely root cause and remediation steps.`

**Agent Response Highlights:**
- Matched INC-001 (145 Hz vibration, Jan 2024) and INC-005 (162 Hz, May 2024)
- Root cause: **Sand ingress causing bearing wear and seizure**
- Recommended: Replace pump unit, upgrade to premium sand control screen, install continuous sand monitoring
- Estimated impact: 14-16 hours downtime, $85,000-$95,000 cost
- Team: **Mechanical-Team-A**

### Query 2: Cost Analytics
**User:** `What are the top 3 most expensive incident types in our history? Show total costs and average downtime for each.`

**Agent Actions:**
- Generates ES|QL query against `oilfield-incidents` index
- Aggregates by `incident_type`, sums `cost_usd`, averages `downtime_hours`
- Returns ranked cost breakdown

### Query 3: Equipment Health Trend
**User:** `Which wells have had the most incidents in the past 6 months and what equipment keeps failing?`

**Agent Actions:**
- Filters by `timestamp` range
- Groups by `well_id` and `equipment_id`
- Returns well health ranking with failure frequency

---

## Tools Used Per Agent

### OilField Incident Retriever (7 tools)
| Tool | Purpose |
|---|---|
| `platform.core.search` | Semantic search on incident descriptions |
| `platform.core.execute_esql` | Run analytics queries |
| `platform.core.generate_esql` | Auto-generate ES\|QL from natural language |
| `platform.core.get_document_by_id` | Fetch full incident details |
| `platform.core.get_index_mapping` | Understand data schema |
| `platform.core.list_indices` | Discover available data |
| `platform.core.get_workflow_execution_status` | Track workflow progress |

### OilField Incident Validator (7 tools)
Same toolset, focused on validation and cross-checking sensor thresholds against historical baselines.

---

## Running the Demo

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set environment variables
export ELASTIC_CLOUD_ID="your-cloud-id"
export ELASTIC_API_KEY="your-api-key"

# 3. Ingest sample data
python scripts/ingest_data.py

# 4. Run the Streamlit demo app
streamlit run app/demo_app.py
```

---

## Hackathon Submission
- **Track**: Elasticsearch Agent Builder
- **Category**: Industrial AI / Predictive Maintenance
- **Tech**: Elastic Agent Builder, ELSER-2 (semantic search), ES|QL, Anthropic Claude 4.5
