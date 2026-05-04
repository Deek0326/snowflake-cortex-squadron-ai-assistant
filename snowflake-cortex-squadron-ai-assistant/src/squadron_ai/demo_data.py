from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd


@dataclass(frozen=True)
class DemoDataset:
    missions: pd.DataFrame
    readiness: pd.DataFrame
    personnel: pd.DataFrame
    maintenance_logs: pd.DataFrame
    incident_reports: pd.DataFrame
    parts_inventory: pd.DataFrame


def load_demo_dataset() -> DemoDataset:
    today = date.today()
    start = today - timedelta(days=42)

    missions = pd.DataFrame(
        [
            [1, "Viper", "Recon", "VX-101", start + timedelta(days=1), "Success", True, 92, 0, None, "Nominal sortie"],
            [2, "Viper", "Training", "VX-102", start + timedelta(days=4), "Success", True, 88, 12, "Runway", "Brief runway hold"],
            [3, "Viper", "Air Patrol", "VX-102", start + timedelta(days=8), "Delayed", False, 74, 43, "Weather", "Weather cell over route"],
            [4, "Falcon", "Recon", "FX-201", start + timedelta(days=2), "Success", True, 90, 0, None, "Nominal sortie"],
            [5, "Falcon", "Logistics", "FX-201", start + timedelta(days=6), "Delayed", False, 69, 58, "Maintenance", "Hydraulic inspection extended"],
            [6, "Falcon", "Training", "FX-202", start + timedelta(days=9), "Failed", False, 51, 0, "Avionics", "Navigation system fault"],
            [7, "Raptor", "Air Patrol", "RX-301", start + timedelta(days=3), "Success", True, 94, 0, None, "Nominal sortie"],
            [8, "Raptor", "Recon", "RX-301", start + timedelta(days=7), "Success", True, 91, 0, None, "Nominal sortie"],
            [9, "Raptor", "Training", "RX-302", start + timedelta(days=11), "Success", True, 87, 18, "Personnel", "Minor crew availability delay"],
            [10, "Viper", "Recon", "VX-101", today - timedelta(days=18), "Success", True, 93, 0, None, "Nominal sortie"],
            [11, "Falcon", "Air Patrol", "FX-202", today - timedelta(days=16), "Delayed", False, 66, 77, "Maintenance", "Avionics replacement queue"],
            [12, "Raptor", "Logistics", "RX-301", today - timedelta(days=15), "Success", True, 89, 0, None, "Nominal sortie"],
            [13, "Viper", "Training", "VX-102", today - timedelta(days=10), "Success", True, 90, 0, None, "Nominal sortie"],
            [14, "Falcon", "Recon", "FX-201", today - timedelta(days=8), "Success", True, 84, 21, "Fuel", "Late refuel window"],
            [15, "Raptor", "Air Patrol", "RX-302", today - timedelta(days=6), "Delayed", False, 73, 64, "Maintenance", "Unexpected engine borescope"],
            [16, "Viper", "Air Patrol", "VX-101", today - timedelta(days=5), "Success", True, 91, 0, None, "Nominal sortie"],
            [17, "Falcon", "Training", "FX-202", today - timedelta(days=3), "Failed", False, 49, 0, "Flight Controls", "Flight control warning"],
            [18, "Raptor", "Recon", "RX-301", today - timedelta(days=2), "Success", True, 96, 0, None, "Nominal sortie"],
            [19, "Viper", "Logistics", "VX-102", today - timedelta(days=1), "Delayed", False, 82, 24, "Parts", "Awaited spare sensor delivery"],
            [20, "Falcon", "Air Patrol", "FX-201", today - timedelta(days=1), "Delayed", False, 59, 88, "Maintenance", "Hydraulic leak recurred during preflight"],
            [21, "Raptor", "Training", "RX-302", today - timedelta(days=1), "Success", True, 76, 11, "Personnel", "Late crew swap after fatigue review"],
        ],
        columns=[
            "mission_id",
            "squadron",
            "mission_type",
            "aircraft_id",
            "mission_date",
            "status",
            "success_flag",
            "readiness_score",
            "delay_minutes",
            "delay_reason",
            "mission_notes",
        ],
    )
    missions["mission_date"] = pd.to_datetime(missions["mission_date"])

    readiness = pd.DataFrame(
        [
            ["VX-101", "Viper", "F-35A", "Ready", 93, 0, today - timedelta(days=1), today + timedelta(days=18)],
            ["VX-102", "Viper", "F-35A", "Ready", 82, 1, today - timedelta(days=1), today + timedelta(days=9)],
            ["FX-201", "Falcon", "F-16C", "Limited", 59, 4, today - timedelta(days=1), today + timedelta(days=2)],
            ["FX-202", "Falcon", "F-16C", "Down", 38, 5, today - timedelta(days=1), today + timedelta(days=1)],
            ["RX-301", "Raptor", "F-22", "Ready", 91, 0, today - timedelta(days=1), today + timedelta(days=14)],
            ["RX-302", "Raptor", "F-22", "Limited", 71, 2, today - timedelta(days=1), today + timedelta(days=4)],
        ],
        columns=[
            "aircraft_id",
            "squadron",
            "aircraft_type",
            "readiness_state",
            "readiness_score",
            "open_maintenance_items",
            "snapshot_date",
            "next_inspection_date",
        ],
    )
    readiness["snapshot_date"] = pd.to_datetime(readiness["snapshot_date"])
    readiness["next_inspection_date"] = pd.to_datetime(readiness["next_inspection_date"])

    personnel = pd.DataFrame(
        [
            ["Viper", 42, 37, 0.88],
            ["Falcon", 39, 29, 0.74],
            ["Raptor", 44, 39, 0.89],
        ],
        columns=["squadron", "assigned_personnel", "available_personnel", "availability_rate"],
    )

    maintenance_logs = pd.DataFrame(
        [
            [101, "FX-202", "Falcon", today - timedelta(days=2), "High", "Flight control warning returned after reset. Parts request open. Aircraft remains down."],
            [102, "FX-201", "Falcon", today - timedelta(days=4), "Medium", "Hydraulic inspection exceeded planned window due to valve replacement."],
            [103, "RX-302", "Raptor", today - timedelta(days=5), "Medium", "Engine borescope found wear trend. Monitor next two sorties."],
            [104, "VX-102", "Viper", today - timedelta(days=8), "Low", "Runway hold caused minor delay. Aircraft cleared with no defect."],
            [105, "RX-301", "Raptor", today - timedelta(days=9), "Low", "Preventive maintenance complete. No open discrepancies."],
            [106, "FX-201", "Falcon", today - timedelta(days=1), "High", "Hydraulic leak recurred during preflight. Replacement seal kit required."],
            [107, "VX-102", "Viper", today - timedelta(days=1), "Medium", "Sensor replacement delayed by low stock at supply cage."],
            [108, "RX-302", "Raptor", today - timedelta(days=1), "Medium", "Engine trend remains within limits but inspection interval shortened."],
        ],
        columns=["log_id", "aircraft_id", "squadron", "log_date", "severity", "log_text"],
    )
    maintenance_logs["log_date"] = pd.to_datetime(maintenance_logs["log_date"])

    incident_reports = pd.DataFrame(
        [
            [201, "Falcon", "FX-202", today - timedelta(days=3), "High", "Repeated flight control warning caused mission abort during training profile."],
            [202, "Falcon", "FX-201", today - timedelta(days=1), "Medium", "Hydraulic leak discovered during preflight checks; mission delayed 88 minutes."],
            [203, "Raptor", "RX-302", today - timedelta(days=6), "Medium", "Engine borescope follow-up added after unexpected maintenance delay."],
            [204, "Viper", "VX-102", today - timedelta(days=1), "Low", "Supply delay for sensor created a 24 minute logistics sortie delay."],
        ],
        columns=["incident_id", "squadron", "aircraft_id", "incident_date", "severity", "report_text"],
    )
    incident_reports["incident_date"] = pd.to_datetime(incident_reports["incident_date"])

    parts_inventory = pd.DataFrame(
        [
            ["Flight control actuator", "FX-202", 0, 2, today + timedelta(days=3), "Critical"],
            ["Hydraulic seal kit", "FX-201", 1, 4, today + timedelta(days=1), "High"],
            ["Engine inspection kit", "RX-302", 2, 2, today + timedelta(days=5), "Medium"],
            ["Avionics sensor", "VX-102", 1, 3, today + timedelta(days=2), "Medium"],
        ],
        columns=["part_name", "aircraft_id", "quantity_on_hand", "reorder_point", "expected_restock_date", "priority"],
    )
    parts_inventory["expected_restock_date"] = pd.to_datetime(parts_inventory["expected_restock_date"])

    return DemoDataset(missions, readiness, personnel, maintenance_logs, incident_reports, parts_inventory)


def export_flat_sample(path: str) -> None:
    dataset = load_demo_dataset()
    dataset.missions.to_csv(path, index=False)
