-- DEV / DEMO DATA ONLY - not part of the schema.
-- Operators must pre-exist in CENTRAL (the PDF gives FO only an Operator ID).
-- Stations are NOT seeded: they enroll themselves via REGISTER_WS (ID + location).
INSERT OR IGNORE INTO operators (operator_id, name, active) VALUES
    ('OP_001', 'Operator One',   1),
    ('OP_002', 'Operator Two',   1),
    ('OP_003', 'Operator Three', 1);
