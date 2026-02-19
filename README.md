# Oilfield Incident Intelligence

An AI-powered system designed to analyze and retrieve oilfield equipment failure patterns, specifically focusing on Electrical Submersible Pumps (ESPs) and common failure modes like sand ingress and high vibration.

## Overview
This project leverages the **Elasticsearch Agent Builder** to create an intelligent assistant that helps field engineers and maintenance teams diagnose equipment failures by comparing real-time telemetry (vibration, temperature, pressure) against historical incident reports.

## Features
- **Intelligent Root Cause Analysis**: Matches current symptoms (e.g., 150 Hz vibration) with historical patterns (e.g., sand ingress).
- **Automated Remediation**: Recommends proven steps from past successful repairs.
- **Risk Assessment**: Calculates recurrence probability and potential financial impact.
- **Team Coordination**: Identifies which specialized teams (e.g., Mechanical-Team-A) have handled similar issues previously.

## Tech Stack
- **Elasticsearch**: Vector search and data storage for historical incident reports.
- **Elastic Agent Builder**: Framework for the RAG (Retrieval-Augmented Generation) agent.
- **LLM**: Anthropic Claude 4.5.

## Project Structure
- `app/`: Streamlit demo application.
- `scripts/`: Data ingestion and preprocessing scripts.
- `src/`: Core logic for the analytics agent and tool definitions.
- `requirements.txt`: Python dependencies.

## How it Works
1. **Data Ingestion**: Historical incident reports (INC-001, INC-005, etc.) are indexed in Elasticsearch.
2. **Natural Language Querying**: Users describe current equipment symptoms.
3. **Retrieval**: The agent searches the index for similar vibration frequencies, equipment types, and locations.
4. **Synthesis**: The LLM synthesizes the findings into a structured Incident Analysis Report.
