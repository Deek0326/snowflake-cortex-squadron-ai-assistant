USE DATABASE SQUADRON_AI_DB;
USE SCHEMA OPERATIONS;

INSERT OVERWRITE INTO SQUADRONS (squadron_name, base_location, commander, primary_mission) VALUES
('Viper', 'North Field', 'Col. Avery Stone', 'Recon and patrol'),
('Falcon', 'East Field', 'Col. Mira Chen', 'Training and logistics'),
('Raptor', 'West Field', 'Col. Elias Ward', 'Air patrol and rapid response');

INSERT OVERWRITE INTO MISSIONS
(squadron_name, mission_type, aircraft_id, mission_date, status, success_flag, readiness_score, delay_minutes, delay_reason, mission_notes)
VALUES
('Viper', 'Recon', 'VX-101', DATEADD(day, -31, CURRENT_DATE), 'Success', TRUE, 92, 0, NULL, 'Nominal sortie'),
('Viper', 'Training', 'VX-102', DATEADD(day, -28, CURRENT_DATE), 'Success', TRUE, 88, 12, 'Runway', 'Brief runway hold'),
('Viper', 'Air Patrol', 'VX-102', DATEADD(day, -24, CURRENT_DATE), 'Delayed', FALSE, 74, 43, 'Weather', 'Weather cell over route'),
('Falcon', 'Recon', 'FX-201', DATEADD(day, -30, CURRENT_DATE), 'Success', TRUE, 90, 0, NULL, 'Nominal sortie'),
('Falcon', 'Logistics', 'FX-201', DATEADD(day, -25, CURRENT_DATE), 'Delayed', FALSE, 69, 58, 'Maintenance', 'Hydraulic inspection extended'),
('Falcon', 'Training', 'FX-202', DATEADD(day, -20, CURRENT_DATE), 'Failed', FALSE, 51, 0, 'Avionics', 'Navigation system fault'),
('Raptor', 'Air Patrol', 'RX-301', DATEADD(day, -29, CURRENT_DATE), 'Success', TRUE, 94, 0, NULL, 'Nominal sortie'),
('Raptor', 'Recon', 'RX-301', DATEADD(day, -22, CURRENT_DATE), 'Success', TRUE, 91, 0, NULL, 'Nominal sortie'),
('Raptor', 'Training', 'RX-302', DATEADD(day, -17, CURRENT_DATE), 'Success', TRUE, 87, 18, 'Personnel', 'Minor crew availability delay'),
('Viper', 'Recon', 'VX-101', DATEADD(day, -14, CURRENT_DATE), 'Success', TRUE, 93, 0, NULL, 'Nominal sortie'),
('Falcon', 'Air Patrol', 'FX-202', DATEADD(day, -12, CURRENT_DATE), 'Delayed', FALSE, 66, 77, 'Maintenance', 'Avionics replacement queue'),
('Raptor', 'Logistics', 'RX-301', DATEADD(day, -10, CURRENT_DATE), 'Success', TRUE, 89, 0, NULL, 'Nominal sortie'),
('Viper', 'Training', 'VX-102', DATEADD(day, -7, CURRENT_DATE), 'Success', TRUE, 90, 0, NULL, 'Nominal sortie'),
('Falcon', 'Recon', 'FX-201', DATEADD(day, -5, CURRENT_DATE), 'Success', TRUE, 84, 21, 'Fuel', 'Late refuel window'),
('Raptor', 'Air Patrol', 'RX-302', DATEADD(day, -3, CURRENT_DATE), 'Delayed', FALSE, 73, 64, 'Maintenance', 'Unexpected engine borescope'),
('Viper', 'Logistics', 'VX-102', DATEADD(day, -1, CURRENT_DATE), 'Delayed', FALSE, 82, 24, 'Parts', 'Awaited spare sensor delivery'),
('Falcon', 'Air Patrol', 'FX-201', DATEADD(day, -1, CURRENT_DATE), 'Delayed', FALSE, 59, 88, 'Maintenance', 'Hydraulic leak recurred during preflight'),
('Raptor', 'Training', 'RX-302', DATEADD(day, -1, CURRENT_DATE), 'Success', TRUE, 76, 11, 'Personnel', 'Late crew swap after fatigue review');

INSERT OVERWRITE INTO AIRCRAFT_READINESS
(aircraft_id, squadron_name, aircraft_type, snapshot_date, readiness_state, readiness_score, open_maintenance_items, next_inspection_date)
VALUES
('VX-101', 'Viper', 'F-35A', CURRENT_DATE, 'READY', 93, 0, DATEADD(day, 18, CURRENT_DATE)),
('VX-102', 'Viper', 'F-35A', CURRENT_DATE, 'READY', 82, 1, DATEADD(day, 9, CURRENT_DATE)),
('FX-201', 'Falcon', 'F-16C', CURRENT_DATE, 'LIMITED', 59, 4, DATEADD(day, 2, CURRENT_DATE)),
('FX-202', 'Falcon', 'F-16C', CURRENT_DATE, 'DOWN', 38, 5, DATEADD(day, 1, CURRENT_DATE)),
('RX-301', 'Raptor', 'F-22', CURRENT_DATE, 'READY', 91, 0, DATEADD(day, 14, CURRENT_DATE)),
('RX-302', 'Raptor', 'F-22', CURRENT_DATE, 'LIMITED', 71, 2, DATEADD(day, 4, CURRENT_DATE));

INSERT OVERWRITE INTO PERSONNEL_AVAILABILITY
(squadron_name, snapshot_date, assigned_personnel, available_personnel, pilots_available, maintenance_crew_available)
VALUES
('Viper', CURRENT_DATE, 42, 37, 12, 18),
('Falcon', CURRENT_DATE, 39, 29, 9, 13),
('Raptor', CURRENT_DATE, 44, 39, 13, 19);

INSERT OVERWRITE INTO MAINTENANCE_LOGS
(aircraft_id, squadron_name, log_date, severity, log_text)
VALUES
('FX-202', 'Falcon', DATEADD(day, -2, CURRENT_DATE), 'High', 'Flight control warning returned after reset. Parts request open. Aircraft remains down.'),
('FX-201', 'Falcon', DATEADD(day, -4, CURRENT_DATE), 'Medium', 'Hydraulic inspection exceeded planned window due to valve replacement.'),
('RX-302', 'Raptor', DATEADD(day, -5, CURRENT_DATE), 'Medium', 'Engine borescope found wear trend. Monitor next two sorties.'),
('VX-102', 'Viper', DATEADD(day, -8, CURRENT_DATE), 'Low', 'Runway hold caused minor delay. Aircraft cleared with no defect.'),
('RX-301', 'Raptor', DATEADD(day, -9, CURRENT_DATE), 'Low', 'Preventive maintenance complete. No open discrepancies.'),
('FX-201', 'Falcon', DATEADD(day, -1, CURRENT_DATE), 'High', 'Hydraulic leak recurred during preflight. Replacement seal kit required.'),
('VX-102', 'Viper', DATEADD(day, -1, CURRENT_DATE), 'Medium', 'Sensor replacement delayed by low stock at supply cage.'),
('RX-302', 'Raptor', DATEADD(day, -1, CURRENT_DATE), 'Medium', 'Engine trend remains within limits but inspection interval shortened.');

INSERT OVERWRITE INTO INCIDENT_REPORTS
(squadron_name, aircraft_id, incident_date, severity, report_text)
VALUES
('Falcon', 'FX-202', DATEADD(day, -3, CURRENT_DATE), 'High', 'Repeated flight control warning caused mission abort during training profile.'),
('Falcon', 'FX-201', DATEADD(day, -1, CURRENT_DATE), 'Medium', 'Hydraulic leak discovered during preflight checks; mission delayed 88 minutes.'),
('Raptor', 'RX-302', DATEADD(day, -6, CURRENT_DATE), 'Medium', 'Engine borescope follow-up added after unexpected maintenance delay.'),
('Viper', 'VX-102', DATEADD(day, -1, CURRENT_DATE), 'Low', 'Supply delay for sensor created a 24 minute logistics sortie delay.');

INSERT OVERWRITE INTO PARTS_INVENTORY
(part_name, aircraft_id, quantity_on_hand, reorder_point, expected_restock_date, priority)
VALUES
('Flight control actuator', 'FX-202', 0, 2, DATEADD(day, 3, CURRENT_DATE), 'Critical'),
('Hydraulic seal kit', 'FX-201', 1, 4, DATEADD(day, 1, CURRENT_DATE), 'High'),
('Engine inspection kit', 'RX-302', 2, 2, DATEADD(day, 5, CURRENT_DATE), 'Medium'),
('Avionics sensor', 'VX-102', 1, 3, DATEADD(day, 2, CURRENT_DATE), 'Medium');
