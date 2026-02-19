#!/usr/bin/env python3
"""
Generate synthetic oilfield incident data for demo purposes.
Creates realistic incident scenarios for the Elasticsearch hackathon demo.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict

random.seed(42)

# Demo oilfield locations
FIELDS = [
    {"field_name": "Permian Basin Alpha", "region": "West Texas", "lat": 31.9, "lon": -102.3},
    {"field_name": "Gulf Coast Platform B7", "region": "Gulf of Mexico", "lat": 28.5, "lon": -90.1},
    {"field_name": "Eagle Ford Shale", "region": "South Texas", "lat": 28.8, "lon": -98.5},
    {"field_name": "Bakken North", "region": "North Dakota", "lat": 47.9, "lon": -103.1},
    {"field_name": "Marcellus Appalachian", "region": "Pennsylvania", "lat": 41.2, "lon": -77.8},
]

INCIDENT_SCENARIOS = [
    {
        "type": "PIPELINE_LEAK",
        "descriptions": [
            "8-inch crude oil pipeline developed a pinhole leak at weld joint. Approximately 10 barrels of crude oil released before isolation. Containment booms deployed.",
            "Corrosion-induced pipeline failure detected by pressure monitoring system. 6-inch gas pipeline section isolated. No injuries reported.",
            "Pipeline pigging operation revealed internal corrosion. Precautionary shutdown of 2km section. Inspection team dispatched."
        ],
        "equipment": ["Pipeline", "Pig Launcher", "Control Valve", "Pressure Transmitter"],
        "severity_weights": ["HIGH", "HIGH", "MEDIUM"]
    },
    {
        "type": "EQUIPMENT_FAILURE",
        "descriptions": [
            "Centrifugal pump bearing failure causing production shutdown on Well-12. Replacement parts ordered, estimated 48-hour downtime.",
            "Gas compressor unit C3 experienced unplanned shutdown due to high vibration alarm. Production reduced by 15%.",
            "Wellhead control panel malfunctioned during routine operations. Manual override engaged. Root cause investigation initiated.",
            "Blowout preventer (BOP) stack pressure test failed. Well operations suspended pending BOP inspection and certification."
        ],
        "equipment": ["Centrifugal Pump", "Gas Compressor", "Wellhead Control Panel", "BOP Stack"],
        "severity_weights": ["MEDIUM", "MEDIUM", "MEDIUM", "HIGH"]
    },
    {
        "type": "H2S_GAS_RELEASE",
        "descriptions": [
            "H2S gas detector alarmed at 15 ppm in production facility. Non-essential personnel evacuated. Ventilation system activated.",
            "Sour crude oil spill in tank battery area triggered H2S monitors. Emergency response team deployed with SCBA equipment.",
            "H2S concentration reached 50 ppm during workover operations. Operations halted, wind direction assessed, muster point activated."
        ],
        "equipment": ["H2S Monitor", "Gas Detector", "SCBA Equipment", "Ventilation System"],
        "severity_weights": ["HIGH", "HIGH", "CRITICAL"]
    },
    {
        "type": "PERSONNEL_INJURY",
        "descriptions": [
            "Rig floor worker sustained hand laceration while making pipe connection. First aid administered on site. No lost time incident.",
            "Operator slipped on wet deck surface causing ankle sprain. Medical evaluation completed. OSHA recordable incident.",
            "Worker struck by dropped wrench from elevated work platform. Hard hat prevented serious injury. Near-miss investigation initiated."
        ],
        "equipment": ["Drill Pipe", "Slips", "Tongs", "Safety Harness"],
        "severity_weights": ["MEDIUM", "LOW", "HIGH"]
    },
    {
        "type": "WELL_BLOWOUT",
        "descriptions": [
            "Uncontrolled well flow detected during drilling operations at 8500ft depth. BOP closed, well kill operation initiated with kill fluid circulation.",
            "Gas kick encountered during tripping operations. Pit gain observed. Well shut in with annular preventer. Well control team mobilized."
        ],
        "equipment": ["BOP", "Drill Pipe", "Mud Pump", "Kill Line"],
        "severity_weights": ["CRITICAL", "CRITICAL"]
    },
    {
        "type": "FIRE_EXPLOSION",
        "descriptions": [
            "Flash fire occurred in wellbay area during hot work permit activities. Fire extinguished within 3 minutes. Minor burns to 1 worker.",
            "Gas cloud ignited from flare stack malfunction. Explosion heard within 500m radius. Emergency shutdown system activated automatically."
        ],
        "equipment": ["Flare Stack", "Fire Suppression System", "Gas Detector", "ESD System"],
        "severity_weights": ["CRITICAL", "CRITICAL"]
    },
    {
        "type": "NEAR_MISS",
        "descriptions": [
            "Near-miss: truck driver came within 2 meters of open wellbore during routine deliveries. Safety barriers were inadequate.",
            "Near-miss: pressure buildup in separator exceeded 90% of relief valve set point. Operator intervened before relief valve activation.",
            "Near-miss: lifting sling found to be worn beyond inspection limits before crane lift operation. Lift halted, equipment replaced."
        ],
        "equipment": ["Crane", "Separator", "Safety Barriers", "Lifting Equipment"],
        "severity_weights": ["LOW", "MEDIUM", "MEDIUM"]
    },
    {
        "type": "ENVIRONMENTAL_RELEASE",
        "descriptions": [
            "Produced water overflow from storage pit due to valve failure. Estimated 50 barrels released. Soil remediation commenced.",
            "Chemical spill of scale inhibitor during dosing operation. 20 liters released. Secondary containment contained spill.",
            "Crude oil sheen detected on water surface near platform. Source identified as overboard discharge from separator. Operations adjusted."
        ],
        "equipment": ["Storage Tank", "Chemical Injection System", "Separator", "Produced Water System"],
        "severity_weights": ["HIGH", "MEDIUM", "HIGH"]
    }
]

TEAMS = ["Emergency Response", "Well Control", "HSE Team", "Operations", "Maintenance", "Environmental"]
STATUSES = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]


def generate_incident(incident_num: int) -> Dict:
    """Generate a single realistic incident record"""
    scenario = random.choice(INCIDENT_SCENARIOS)
    description = random.choice(scenario["descriptions"])
    severity = random.choice(scenario["severity_weights"])
    location = random.choice(FIELDS)

    severity_scores = {"CRITICAL": random.randint(80, 99), "HIGH": random.randint(60, 79),
                       "MEDIUM": random.randint(40, 59), "LOW": random.randint(10, 39)}
    severity_score = severity_scores[severity]

    days_ago = random.randint(0, 730)
    timestamp = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23))

    injuries = 0
    fatalities = 0
    if severity == "CRITICAL":
        injuries = random.randint(0, 3)
        fatalities = random.randint(0, 1) if random.random() < 0.1 else 0
    elif severity == "HIGH":
        injuries = random.randint(0, 2)

    resolution_hours = {"CRITICAL": random.uniform(2, 48), "HIGH": random.uniform(4, 72),
                        "MEDIUM": random.uniform(8, 168), "LOW": random.uniform(24, 336)}

    status = random.choice(STATUSES)
    if status == "CLOSED":
        status = "RESOLVED"

    financial_impact = {
        "CRITICAL": random.uniform(500000, 10000000),
        "HIGH": random.uniform(50000, 500000),
        "MEDIUM": random.uniform(5000, 50000),
        "LOW": random.uniform(500, 5000)
    }[severity]

    return {
        "incident_id": f"INC-{timestamp.year}-{incident_num:04d}",
        "timestamp": timestamp.isoformat(),
        "location": {
            "field_name": location["field_name"],
            "well_id": f"WELL-{random.randint(1, 50):03d}",
            "region": location["region"],
            "coordinates": {
                "lat": location["lat"] + random.uniform(-0.5, 0.5),
                "lon": location["lon"] + random.uniform(-0.5, 0.5)
            }
        },
        "incident_type": scenario["type"],
        "severity": severity,
        "severity_score": severity_score,
        "description": description,
        "equipment_involved": random.choice(scenario["equipment"]),
        "personnel_count": random.randint(2, 25),
        "injuries": injuries,
        "fatalities": fatalities,
        "financial_impact": round(financial_impact, 2),
        "root_cause": f"Under investigation - preliminary assessment indicates {random.choice(['mechanical failure', 'human error', 'process deviation', 'equipment degradation', 'design deficiency'])}",
        "corrective_actions": f"Immediate isolation and assessment. {random.choice(['Maintenance order raised', 'Equipment replaced', 'Procedure reviewed', 'Training initiated', 'Design modification planned'])}.",
        "status": status,
        "assigned_team": random.choice(TEAMS),
        "resolution_time_hours": round(resolution_hours[severity], 1) if status == "RESOLVED" else None,
        "tags": [scenario["type"].lower(), severity.lower(), location["region"].lower().replace(" ", "-")]
    }


def generate_demo_dataset(num_incidents: int = 200) -> List[Dict]:
    """Generate a complete demo dataset"""
    incidents = [generate_incident(i + 1) for i in range(num_incidents)]
    print(f"Generated {len(incidents)} incidents")
    return incidents


if __name__ == "__main__":
    incidents = generate_demo_dataset(200)

    with open("data/demo_incidents.json", "w") as f:
        json.dump(incidents, f, indent=2, default=str)

    print(f"Saved {len(incidents)} incidents to data/demo_incidents.json")

    # Print summary
    from collections import Counter
    severity_counts = Counter(inc["severity"] for inc in incidents)
    type_counts = Counter(inc["incident_type"] for inc in incidents)

    print("\nSeverity Distribution:")
    for sev, count in severity_counts.items():
        print(f"  {sev}: {count}")

    print("\nIncident Type Distribution:")
    for itype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {itype}: {count}")
