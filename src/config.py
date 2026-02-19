"""
Configuration management for Oilfield Incident Intelligence System
Uses environment variables for secure credential management
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ElasticConfig:
    """Elasticsearch connection configuration"""
    url: str
    api_key: str
    index_name: str = "oilfield-incidents"
    kb_index: str = "oilfield-kb"
    embedding_model: str = "multilingual-e5-small"


@dataclass
class AgentConfig:
    """Elastic Agent Builder configuration"""
    agent_id: str
    conversation_endpoint: str
    api_key: str


class Config:
    """Main configuration class"""

    def __init__(self):
        self.elastic = ElasticConfig(
            url=os.getenv("ELASTICSEARCH_URL", ""),
            api_key=os.getenv("ELASTICSEARCH_API_KEY", ""),
            index_name=os.getenv("INCIDENT_INDEX", "oilfield-incidents"),
            kb_index=os.getenv("KB_INDEX", "oilfield-kb"),
        )

        self.agent = AgentConfig(
            agent_id=os.getenv("ELASTIC_AGENT_ID", ""),
            conversation_endpoint=os.getenv("ELASTIC_CONVERSATION_ENDPOINT", ""),
            api_key=os.getenv("ELASTIC_AGENT_API_KEY", ""),
        )

        self.debug = os.getenv("DEBUG", "false").lower() == "true"
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    def validate(self) -> bool:
        """Validate that required configuration is present"""
        required = [
            self.elastic.url,
            self.elastic.api_key,
        ]
        return all(required)


# Global config instance
config = Config()
