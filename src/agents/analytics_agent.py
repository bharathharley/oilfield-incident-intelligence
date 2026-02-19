"""
Analytics Agent - ES|QL powered analytics for oilfield incident trends
Provides natural language querying over historical incident data
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class AnalyticsAgent:
    """
    Analytics agent that translates natural language questions into ES|QL queries
    and returns structured analytics results about oilfield incidents.
    """

    def __init__(self, elastic_client, index_name: str = "oilfield-incidents"):
        self.elastic_client = elastic_client
        self.index_name = index_name

    def get_incident_trends(self, days: int = 30) -> Dict[str, Any]:
        """Get incident trends over the past N days using ES|QL"""
        query = f"""
        FROM {self.index_name}
        | WHERE timestamp >= NOW() - {days} days
        | STATS
            total_incidents = COUNT(*),
            avg_severity_score = AVG(severity_score),
            total_injuries = SUM(injuries),
            total_fatalities = SUM(fatalities),
            total_financial_impact = SUM(financial_impact)
          BY incident_type
        | SORT total_incidents DESC
        """
        try:
            result = self.elastic_client.esql_query(query)
            return {
                "period_days": days,
                "data": result.get("values", []),
                "columns": result.get("columns", []),
                "query": query
            }
        except Exception as e:
            logger.error(f"Trend analysis failed: {e}")
            return {"error": str(e)}

    def get_severity_distribution(self) -> Dict[str, Any]:
        """Get distribution of incidents by severity level"""
        query = f"""
        FROM {self.index_name}
        | STATS count = COUNT(*) BY severity
        | SORT count DESC
        """
        try:
            result = self.elastic_client.esql_query(query)
            return {
                "distribution": result.get("values", []),
                "columns": result.get("columns", [])
            }
        except Exception as e:
            logger.error(f"Severity distribution query failed: {e}")
            return {"error": str(e)}

    def get_mttr_by_type(self) -> Dict[str, Any]:
        """Get Mean Time To Resolution (MTTR) by incident type"""
        query = f"""
        FROM {self.index_name}
        | WHERE status == "RESOLVED"
        | STATS
            mttr_hours = AVG(resolution_time_hours),
            min_resolution = MIN(resolution_time_hours),
            max_resolution = MAX(resolution_time_hours),
            total_resolved = COUNT(*)
          BY incident_type
        | SORT mttr_hours ASC
        """
        try:
            result = self.elastic_client.esql_query(query)
            return {
                "mttr_data": result.get("values", []),
                "columns": result.get("columns", [])
            }
        except Exception as e:
            logger.error(f"MTTR query failed: {e}")
            return {"error": str(e)}

    def get_high_risk_locations(self, top_n: int = 10) -> Dict[str, Any]:
        """Identify locations with highest incident frequency and severity"""
        query = f"""
        FROM {self.index_name}
        | STATS
            incident_count = COUNT(*),
            avg_severity = AVG(severity_score),
            critical_count = COUNT_IF(severity == "CRITICAL"),
            total_injuries = SUM(injuries)
          BY location.field_name
        | EVAL risk_score = (avg_severity * incident_count) + (critical_count * 20)
        | SORT risk_score DESC
        | LIMIT {top_n}
        """
        try:
            result = self.elastic_client.esql_query(query)
            return {
                "high_risk_locations": result.get("values", []),
                "columns": result.get("columns", [])
            }
        except Exception as e:
            logger.error(f"High risk locations query failed: {e}")
            return {"error": str(e)}

    def get_monthly_summary(self, year: int = None) -> Dict[str, Any]:
        """Get monthly incident summary for a given year"""
        if not year:
            year = datetime.now().year

        query = f"""
        FROM {self.index_name}
        | WHERE timestamp >= "{year}-01-01" AND timestamp < "{year + 1}-01-01"
        | EVAL month = DATE_TRUNC(1 month, timestamp)
        | STATS
            total = COUNT(*),
            critical = COUNT_IF(severity == "CRITICAL"),
            high = COUNT_IF(severity == "HIGH"),
            injuries = SUM(injuries),
            financial_impact = SUM(financial_impact)
          BY month
        | SORT month ASC
        """
        try:
            result = self.elastic_client.esql_query(query)
            return {
                "year": year,
                "monthly_data": result.get("values", []),
                "columns": result.get("columns", [])
            }
        except Exception as e:
            logger.error(f"Monthly summary query failed: {e}")
            return {"error": str(e)}

    def get_equipment_failure_analysis(self) -> Dict[str, Any]:
        """Analyze equipment failures to identify patterns"""
        query = f"""
        FROM {self.index_name}
        | WHERE incident_type == "EQUIPMENT_FAILURE"
        | STATS
            failure_count = COUNT(*),
            avg_financial_impact = AVG(financial_impact),
            total_downtime_hours = SUM(resolution_time_hours)
          BY equipment_involved
        | SORT failure_count DESC
        | LIMIT 20
        """
        try:
            result = self.elastic_client.esql_query(query)
            return {
                "equipment_analysis": result.get("values", []),
                "columns": result.get("columns", [])
            }
        except Exception as e:
            logger.error(f"Equipment analysis query failed: {e}")
            return {"error": str(e)}

    def generate_executive_summary(self) -> Dict[str, Any]:
        """Generate an executive-level summary of incident data"""
        queries = {
            "overview": f"""
                FROM {self.index_name}
                | STATS
                    total_incidents = COUNT(*),
                    open_incidents = COUNT_IF(status == "OPEN"),
                    critical_open = COUNT_IF(status == "OPEN" AND severity == "CRITICAL"),
                    total_injuries = SUM(injuries),
                    total_fatalities = SUM(fatalities),
                    total_financial_impact = SUM(financial_impact),
                    avg_resolution_hours = AVG(resolution_time_hours)
            """,
            "last_30_days": f"""
                FROM {self.index_name}
                | WHERE timestamp >= NOW() - 30 days
                | STATS
                    incidents_30d = COUNT(*),
                    critical_30d = COUNT_IF(severity == "CRITICAL")
            """
        }

        summary = {}
        for key, query in queries.items():
            try:
                result = self.elastic_client.esql_query(query)
                values = result.get("values", [[]])
                columns = result.get("columns", [])
                if values and columns:
                    summary[key] = dict(zip(
                        [c["name"] for c in columns],
                        values[0]
                    ))
            except Exception as e:
                logger.error(f"Executive summary query '{key}' failed: {e}")
                summary[key] = {"error": str(e)}

        summary["generated_at"] = datetime.now().isoformat()
        return summary
