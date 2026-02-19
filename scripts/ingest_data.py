#!/usr/bin/env python3
"""
Data ingestion script - loads demo incidents into Elasticsearch
Sets up indices, mappings, and bulk-indexes the generated demo data
"""

import json
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.elastic_client import ElasticClient
from src.config import config
from scripts.generate_demo_data import generate_demo_dataset


def setup_and_ingest():
    """Main function to set up indices and ingest demo data"""
    print("Oilfield Incident Intelligence - Data Ingestion")
    print("=" * 50)

    # Validate configuration
    if not config.validate():
        print("ERROR: Missing required configuration. Check your .env file.")
        print("Required: ELASTICSEARCH_URL, ELASTICSEARCH_API_KEY")
        sys.exit(1)

    # Connect to Elasticsearch
    print(f"Connecting to Elasticsearch at {config.elastic.url[:50]}...")
    try:
        client = ElasticClient(
            url=config.elastic.url,
            api_key=config.elastic.api_key
        )
        print("Connected successfully!")
    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        sys.exit(1)

    # Create incident index
    index_name = config.elastic.index_name
    print(f"\nSetting up index: {index_name}")
    client.create_incident_index(index_name)

    # Generate demo data
    print("\nGenerating demo incident data...")
    incidents = generate_demo_dataset(200)
    print(f"Generated {len(incidents)} incidents")

    # Save to file for reference
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    data_file = data_dir / "demo_incidents.json"

    with open(data_file, "w") as f:
        json.dump(incidents, f, indent=2, default=str)
    print(f"Saved demo data to {data_file}")

    # Bulk index incidents
    print(f"\nIndexing {len(incidents)} incidents into Elasticsearch...")
    indexed = client.bulk_index_incidents(index_name, incidents)
    print(f"Successfully indexed {indexed} incidents")

    # Verify indexing
    print("\nVerifying data...")
    stats = client.get_incident_stats(index_name)

    by_severity = stats.get("by_severity", {}).get("buckets", [])
    print("\nSeverity Distribution:")
    for bucket in by_severity:
        print(f"  {bucket['key']}: {bucket['doc_count']}")

    by_type = stats.get("by_type", {}).get("buckets", [])[:5]
    print("\nTop Incident Types:")
    for bucket in by_type:
        print(f"  {bucket['key']}: {bucket['doc_count']}")

    avg_resolution = stats.get("avg_resolution_time", {}).get("value", 0)
    print(f"\nAverage Resolution Time: {avg_resolution:.1f} hours")

    total_impact = stats.get("total_financial_impact", {}).get("value", 0)
    print(f"Total Financial Impact: ${total_impact:,.2f}")

    print("\n" + "=" * 50)
    print("Data ingestion complete!")
    print(f"Index: {index_name}")
    print(f"Total incidents: {indexed}")
    print("\nNext steps:")
    print("  1. Run the demo app: streamlit run app/demo_app.py")
    print("  2. Open Kibana to explore the data")
    print("  3. Use the triage agent to classify new incidents")


if __name__ == "__main__":
    setup_and_ingest()
