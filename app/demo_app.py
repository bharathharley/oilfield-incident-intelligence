"""
Streamlit Demo Application - Oilfield Incident Intelligence
Provides a dashboard and interactive triage interface for the hackathon project
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.elastic_client import ElasticClient
from src.config import config
from src.agents.triage_agent import TriageAgent
from src.agents.analytics_agent import AnalyticsAgent

st.set_page_config(
    page_title="Oilfield Incident Intelligence",
    page_icon="🛢️",
    layout="wide"
)

# Initialize clients
@st.cache_resource
def get_clients():
    elastic_client = ElasticClient(config.elastic.url, config.elastic.api_key)
    triage_agent = TriageAgent(
        config.agent.conversation_endpoint,
        config.agent.api_key,
        elastic_client
    )
    analytics_agent = AnalyticsAgent(elastic_client, config.elastic.index_name)
    return elastic_client, triage_agent, analytics_agent

try:
    ec, ta, aa = get_clients()
except Exception as e:
    st.error(f"Failed to connect to services: {e}")
    st.stop()

# Sidebar
st.sidebar.title("🛢️ Oilfield Intelligence")
menu = st.sidebar.radio("Navigation", ["Dashboard", "Incident Triage", "Historical Analysis", "Settings"])

if menu == "Dashboard":
    st.title("Operations Command Center")

    # Get executive summary
    summary = aa.generate_executive_summary()
    ov = summary.get("overview", {})

    # Top-level metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Incidents", ov.get("total_incidents", 0))
    col2.metric("Critical Open", ov.get("critical_open", 0), delta_color="inverse")
    col3.metric("Total Injuries", ov.get("total_injuries", 0))
    col4.metric("Financial Impact", f"${ov.get('total_financial_impact', 0)/1e6:.1f}M")

    # Trend Charts
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("Incidents by Severity")
        sev_dist = aa.get_severity_distribution()
        if "distribution" in sev_dist:
            df_sev = pd.DataFrame(sev_dist["distribution"], columns=[c["name"] for c in sev_dist["columns"]])
            fig = px.pie(df_sev, values='count', names='severity', color='severity',
                         color_discrete_map={'CRITICAL': 'red', 'HIGH': 'orange', 'MEDIUM': 'yellow', 'LOW': 'green'})
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Incident Types Trend (30 Days)")
        trends = aa.get_incident_trends(30)
        if "data" in trends:
            df_trends = pd.DataFrame(trends["data"], columns=[c["name"] for c in trends["columns"]])
            fig = px.bar(df_trends, x='incident_type', y='total_incidents', color='avg_severity_score')
            st.plotly_chart(fig, use_container_width=True)

elif menu == "Incident Triage":
    st.title("AI-Powered Incident Triage")
    st.write("Report a new incident for AI analysis and prioritization.")

    with st.form("triage_form"):
        description = st.text_area("Incident Description", placeholder="Describe what happened in detail...")
        location = st.text_input("Location / Field Name")
        equipment = st.text_input("Equipment Involved")
        submitted = st.form_submit_button("Analyze Incident")

        if submitted and description:
            with st.spinner("Analyzing incident with Elastic Agent Builder..."):
                ta.start_conversation()
                context = {"location": location, "equipment": equipment}
                result = ta.classify_incident(description, context)

                st.success("Analysis Complete")

                # Results Layout
                c1, c2 = st.columns([1, 2])

                with c1:
                    st.info(f"**Severity:** {result.get('severity')}")
                    st.warning(f"**Incident Type:** {result.get('incident_type')}")
                    st.metric("Severity Score", result.get("severity_score", 0))

                with c2:
                    st.subheader("Immediate Actions")
                    for action in result.get("immediate_actions", []):
                        st.write(f"- {action}")

                st.subheader("Similar Historical Incidents")
                keywords = result.get("similar_incidents_keywords", [])
                similar = ta.get_similar_incidents(keywords)
                if similar:
                    st.table(similar)
                else:
                    st.write("No direct matches found in historical database.")

elif menu == "Historical Analysis":
    st.title("ES|QL Data Exploration")
    st.write("Explore historical patterns and risk factors.")

    risk_data = aa.get_high_risk_locations()
    if "high_risk_locations" in risk_data:
        st.subheader("High Risk Field Locations")
        df_risk = pd.DataFrame(risk_data["high_risk_locations"], columns=[c["name"] for c in risk_data["columns"]])
        st.dataframe(df_risk, use_container_width=True)

    st.subheader("Mean Time to Resolution (MTTR) by Type")
    mttr = aa.get_mttr_by_type()
    if "mttr_data" in mttr:
        df_mttr = pd.DataFrame(mttr["mttr_data"], columns=[c["name"] for c in mttr["columns"]])
        fig = px.line(df_mttr, x='incident_type', y='mttr_hours', markers=True)
        st.plotly_chart(fig, use_container_width=True)

elif menu == "Settings":
    st.title("System Configuration")
    st.write("Project: Oilfield Incident Intelligence")
    st.write(f"Elasticsearch URL: `{config.elastic.url}`")
    st.write(f"Index: `{config.elastic.index_name}`")
    st.write(f"Agent ID: `{config.agent.agent_id}`")
    st.button("Reload System")
