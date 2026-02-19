"""
Triage Agent - Core agent for oilfield incident classification and prioritization
Built on top of Elastic Agent Builder framework
"""

import json
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


SEVERITY_MATRIX = {
    "CRITICAL": {
        "score_range": (80, 100),
        "response_time_hours": 1,
        "escalation_required": True,
        "description": "Immediate threat to life, catastrophic equipment failure, or major environmental release"
    },
    "HIGH": {
        "score_range": (60, 79),
        "response_time_hours": 4,
        "escalation_required": True,
        "description": "Significant safety risk, major production loss, or regulatory violation"
    },
    "MEDIUM": {
        "score_range": (40, 59),
        "response_time_hours": 24,
        "escalation_required": False,
        "description": "Moderate risk, production impact, equipment damage without immediate danger"
    },
    "LOW": {
        "score_range": (0, 39),
        "response_time_hours": 72,
        "escalation_required": False,
        "description": "Minor incident, near-miss, or procedural deviation"
    }
}


INCIDENT_TYPES = [
    "WELL_BLOWOUT",
    "PIPELINE_LEAK",
    "EQUIPMENT_FAILURE",
    "FIRE_EXPLOSION",
    "CHEMICAL_SPILL",
    "PERSONNEL_INJURY",
    "ENVIRONMENTAL_RELEASE",
    "H2S_GAS_RELEASE",
    "PRESSURE_ANOMALY",
    "STRUCTURAL_FAILURE",
    "ELECTRICAL_FAULT",
    "NEAR_MISS",
    "PROCEDURAL_VIOLATION",
    "CONTRACTOR_INCIDENT"
]


class TriageAgent:
    """
    AI-powered triage agent for oilfield incident management.
    Uses Elastic Agent Builder for natural language understanding and
    Elasticsearch for historical incident retrieval and pattern matching.
    """

    def __init__(self, agent_endpoint: str, api_key: str, elastic_client=None):
        self.agent_endpoint = agent_endpoint
        self.api_key = api_key
        self.elastic_client = elastic_client
        self.conversation_id = None

    def start_conversation(self) -> str:
        """Start a new conversation with the Elastic Agent"""
        headers = {
            "Authorization": f"ApiKey {self.api_key}",
            "Content-Type": "application/json"
        }
        response = requests.post(
            f"{self.agent_endpoint}/conversations",
            headers=headers
        )
        data = response.json()
        self.conversation_id = data.get("id")
        return self.conversation_id

    def classify_incident(self, incident_description: str,
                          additional_context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Classify an incident using the Elastic Agent Builder.
        Returns structured classification with severity, type, and recommendations.
        """
        prompt = self._build_classification_prompt(incident_description, additional_context)

        headers = {
            "Authorization": f"ApiKey {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "message": prompt,
            "conversationId": self.conversation_id
        }

        try:
            response = requests.post(
                f"{self.agent_endpoint}/chat",
                headers=headers,
                json=payload
            )
            agent_response = response.json()
            return self._parse_classification_response(agent_response, incident_description)
        except Exception as e:
            logger.error(f"Agent classification failed: {e}")
            return self._fallback_classification(incident_description)

    def _build_classification_prompt(self, description: str,
                                     context: Optional[Dict] = None) -> str:
        """Build a structured prompt for incident classification"""
        context_str = ""
        if context:
            context_str = f"""
Additional Context:
- Location: {context.get('location', 'Unknown')}
- Equipment: {context.get('equipment', 'Unknown')}
- Personnel on site: {context.get('personnel', 'Unknown')}
- Time of incident: {context.get('timestamp', datetime.now().isoformat())}
"""

        return f"""You are an expert oilfield safety and operations AI assistant.

Analyze this incident report and provide structured classification:

INCIDENT DESCRIPTION:
{description}
{context_str}

Provide your analysis in JSON format with these fields:
1. incident_type: One of {INCIDENT_TYPES}
2. severity: CRITICAL/HIGH/MEDIUM/LOW
3. severity_score: 0-100
4. immediate_actions: List of 3-5 immediate response actions
5. root_cause_hypothesis: Most likely root cause
6. similar_incidents_keywords: Keywords to search historical incidents
7. escalation_contacts: List of teams to notify
8. estimated_resolution_hours: Estimated time to resolve
9. regulatory_reporting_required: true/false
10. safety_bulletin_required: true/false

Respond ONLY with the JSON object."""

    def _parse_classification_response(self, response: Dict,
                                        original_desc: str) -> Dict[str, Any]:
        """Parse the agent response into structured incident data"""
        try:
            content = response.get("message", {}).get("content", "{}")
            if isinstance(content, str):
                # Extract JSON from response
                start = content.find("{")
                end = content.rfind("}") + 1
                if start >= 0 and end > start:
                    classification = json.loads(content[start:end])
                else:
                    raise ValueError("No JSON found in response")
            else:
                classification = content

            # Enrich with metadata
            classification["original_description"] = original_desc
            classification["triage_timestamp"] = datetime.now().isoformat()
            classification["triage_agent_version"] = "1.0.0"

            # Get severity details
            severity = classification.get("severity", "MEDIUM")
            if severity in SEVERITY_MATRIX:
                classification["severity_details"] = SEVERITY_MATRIX[severity]

            return classification

        except Exception as e:
            logger.error(f"Failed to parse agent response: {e}")
            return self._fallback_classification(original_desc)

    def _fallback_classification(self, description: str) -> Dict[str, Any]:
        """Rule-based fallback classification when agent is unavailable"""
        desc_lower = description.lower()

        # Simple keyword-based severity
        if any(kw in desc_lower for kw in ["blowout", "explosion", "fire", "fatality", "death"]):
            severity = "CRITICAL"
            score = 90
        elif any(kw in desc_lower for kw in ["injury", "leak", "spill", "h2s", "pressure"]):
            severity = "HIGH"
            score = 65
        elif any(kw in desc_lower for kw in ["equipment", "failure", "malfunction"]):
            severity = "MEDIUM"
            score = 45
        else:
            severity = "LOW"
            score = 20

        return {
            "incident_type": "EQUIPMENT_FAILURE",
            "severity": severity,
            "severity_score": score,
            "immediate_actions": [
                "Assess the situation and ensure personnel safety",
                "Notify immediate supervisor",
                "Isolate affected equipment if safe to do so",
                "Document initial observations"
            ],
            "root_cause_hypothesis": "Under investigation - agent unavailable for detailed analysis",
            "triage_timestamp": datetime.now().isoformat(),
            "triage_agent_version": "1.0.0-fallback",
            "original_description": description
        }

    def get_similar_incidents(self, keywords: List[str],
                              index_name: str = "oilfield-incidents") -> List[Dict]:
        """Search for similar historical incidents using ES|QL"""
        if not self.elastic_client:
            return []

        keyword_filter = " OR ".join(keywords[:3])  # Use top 3 keywords
        esql_query = f"""
        FROM {index_name}
        | WHERE description LIKE "*{keywords[0]}*"
        | SORT severity_score DESC
        | LIMIT 5
        | KEEP incident_id, timestamp, incident_type, severity, description, resolution_time_hours
        """

        try:
            results = self.elastic_client.esql_query(esql_query)
            return results.get("values", [])
        except Exception as e:
            logger.error(f"Failed to retrieve similar incidents: {e}")
            return []

    def generate_incident_report(self, classification: Dict[str, Any],
                                  incident_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive incident report"""
        return {
            "report_id": f"RPT-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            "generated_at": datetime.now().isoformat(),
            "incident_summary": {
                "id": incident_data.get("incident_id"),
                "type": classification.get("incident_type"),
                "severity": classification.get("severity"),
                "severity_score": classification.get("severity_score"),
                "location": incident_data.get("location"),
                "timestamp": incident_data.get("timestamp")
            },
            "triage_results": classification,
            "recommended_actions": classification.get("immediate_actions", []),
            "escalation_required": SEVERITY_MATRIX.get(
                classification.get("severity", "MEDIUM"), {}
            ).get("escalation_required", False),
            "response_deadline": SEVERITY_MATRIX.get(
                classification.get("severity", "MEDIUM"), {}
            ).get("response_time_hours", 24),
            "regulatory_reporting": classification.get("regulatory_reporting_required", False)
        }
