"""
Elasticsearch client wrapper for Oilfield Incident Intelligence
Handles connections, index management, and search operations
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ElasticClient:
    """Wrapper around Elasticsearch client with oilfield-specific methods"""

    def __init__(self, url: str, api_key: str):
        self.client = Elasticsearch(
            url,
            api_key=api_key,
            request_timeout=30,
        )
        self._verify_connection()

    def _verify_connection(self):
        """Verify connection to Elasticsearch"""
        try:
            info = self.client.info()
            logger.info(f"Connected to Elasticsearch {info['version']['number']}")
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise

    def create_incident_index(self, index_name: str) -> bool:
        """Create the incidents index with proper mappings"""
        mapping = {
            "mappings": {
                "properties": {
                    "incident_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "location": {
                        "properties": {
                            "field_name": {"type": "keyword"},
                            "well_id": {"type": "keyword"},
                            "coordinates": {"type": "geo_point"}
                        }
                    },
                    "incident_type": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                    "severity_score": {"type": "float"},
                    "description": {
                        "type": "text",
                        "analyzer": "english"
                    },
                    "description_embedding": {
                        "type": "dense_vector",
                        "dims": 384,
                        "index": True,
                        "similarity": "cosine"
                    },
                    "equipment_involved": {"type": "keyword"},
                    "personnel_count": {"type": "integer"},
                    "injuries": {"type": "integer"},
                    "fatalities": {"type": "integer"},
                    "financial_impact": {"type": "float"},
                    "root_cause": {"type": "text"},
                    "corrective_actions": {"type": "text"},
                    "status": {"type": "keyword"},
                    "assigned_team": {"type": "keyword"},
                    "resolution_time_hours": {"type": "float"},
                    "tags": {"type": "keyword"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }

        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created index: {index_name}")
            return True
        logger.info(f"Index {index_name} already exists")
        return False

    def index_incident(self, index_name: str, incident: Dict[str, Any]) -> str:
        """Index a single incident document"""
        response = self.client.index(
            index=index_name,
            id=incident.get("incident_id"),
            document=incident
        )
        return response["_id"]

    def bulk_index_incidents(self, index_name: str, incidents: List[Dict]) -> int:
        """Bulk index multiple incidents"""
        actions = [
            {
                "_index": index_name,
                "_id": inc.get("incident_id"),
                "_source": inc
            }
            for inc in incidents
        ]
        success, errors = bulk(self.client, actions)
        logger.info(f"Indexed {success} incidents, {len(errors)} errors")
        return success

    def semantic_search(self, index_name: str, query_vector: List[float],
                        k: int = 10, filters: Optional[Dict] = None) -> List[Dict]:
        """Perform semantic/KNN search on incident descriptions"""
        knn_query = {
            "field": "description_embedding",
            "query_vector": query_vector,
            "k": k,
            "num_candidates": k * 10
        }

        if filters:
            knn_query["filter"] = filters

        response = self.client.search(
            index=index_name,
            knn=knn_query,
            source=["incident_id", "timestamp", "incident_type", "severity",
                    "description", "location", "status"]
        )
        return [hit["_source"] for hit in response["hits"]["hits"]]

    def esql_query(self, query: str) -> Dict[str, Any]:
        """Execute an ES|QL query"""
        response = self.client.esql.query(query=query)
        return response

    def get_incident_stats(self, index_name: str) -> Dict[str, Any]:
        """Get aggregate statistics for incidents"""
        agg_query = {
            "aggs": {
                "by_severity": {
                    "terms": {"field": "severity"}
                },
                "by_type": {
                    "terms": {"field": "incident_type"}
                },
                "avg_resolution_time": {
                    "avg": {"field": "resolution_time_hours"}
                },
                "total_financial_impact": {
                    "sum": {"field": "financial_impact"}
                }
            },
            "size": 0
        }
        response = self.client.search(index=index_name, body=agg_query)
        return response["aggregations"]
