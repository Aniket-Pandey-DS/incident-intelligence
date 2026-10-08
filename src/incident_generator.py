from datetime import datetime, timedelta
from pathlib import Path
import csv
import random


SERVICES = [
    "payments",
    "checkout",
    "search",
    "authentication",
    "inventory",
]

REGIONS = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "ap-south-1",
]

SEVERITIES = ["SEV1", "SEV2", "SEV3", "SEV4"]

CATEGORIES = [
    "availability",
    "performance",
    "security",
    "deployment",
    "dependency",
    "data",
    "infrastructure",
]

ENVIRONMENTS = ["prod", "staging", "dev"]

SOURCE_SYSTEMS = [
    "pagerduty",
    "jira",
    "monitoring",
]

RESOLVER_TEAMS = [
    "platform",
    "payments",
    "sre",
    "infrastructure",
    "security",
]


def generate_incidents(
    output_path: str | Path,
    num_records: int = 100,
    seed: int = 42,
) -> None:
    """Generate deterministic synthetic incident data."""

    random.seed(seed)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = datetime(2026, 1, 1, 0, 0, 0)

    fieldnames = [
        "incident_id",
        "created_at",
        "resolved_at",
        "service",
        "region",
        "severity",
        "category",
        "status",
        "description",
        "affected_users",
        "error_rate",
        "latency_ms",
        "request_volume",
        "cpu_utilization",
        "memory_utilization",
        "root_cause",
        "resolution",
        "resolver_team",
        "source_system",
        "environment",
        "created_by",
        "ingested_at",
    ]

    records = []

    for i in range(1, num_records + 1):
        created_at = start_time + timedelta(
            minutes=random.randint(0, 60 * 24 * 30)
        )

        severity = random.choice(SEVERITIES)

        duration_minutes = random.randint(5, 360)
        resolved_at = created_at + timedelta(minutes=duration_minutes)

        service = random.choice(SERVICES)

        record = {
            "incident_id": f"INC-{i:05d}",
            "created_at": created_at.isoformat(),
            "resolved_at": resolved_at.isoformat(),
            "service": service,
            "region": random.choice(REGIONS),
            "severity": severity,
            "category": random.choice(CATEGORIES),
            "status": "RESOLVED",
            "description": (
                f"{severity} incident affecting {service}"
            ),
            "affected_users": random.randint(10, 100000),
            "error_rate": round(random.uniform(0.1, 25.0), 2),
            "latency_ms": round(random.uniform(20, 2000), 2),
            "request_volume": random.randint(1000, 5000000),
            "cpu_utilization": round(random.uniform(10, 100), 2),
            "memory_utilization": round(random.uniform(10, 100), 2),
            "root_cause": f"{random.choice(['deployment', 'dependency', 'configuration', 'infrastructure'])} issue",
            "resolution": f"Rolled back or repaired {service}",
            "resolver_team": random.choice(RESOLVER_TEAMS),
            "source_system": random.choice(SOURCE_SYSTEMS),
            "environment": random.choice(ENVIRONMENTS),
            "created_by": "incident-management-system",
            "ingested_at": (
                resolved_at + timedelta(minutes=random.randint(1, 30))
            ).isoformat(),
        }

        # Introduce controlled data-quality issues for testing.
        if i == 10:
            record["severity"] = "SEV99"

        elif i == 20:
            record["affected_users"] = -500

        elif i == 30:
            record["error_rate"] = 135.5

        elif i == 40:
            record["cpu_utilization"] = 125.0

        elif i == 50:
            record["memory_utilization"] = -10.0

        elif i == 60:
            record["resolved_at"] = (
                created_at - timedelta(minutes=30)
            ).isoformat()

        elif i == 70:
            record["incident_id"] = None

        elif i == 80:
            record["status"] = "UNKNOWN"

        records.append(record)

    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


if __name__ == "__main__":
    generate_incidents(
        "data/raw/incidents/incidents.csv",
        num_records=100,
    )