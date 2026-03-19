-- =======================================================
-- Mock Data for Hack2026 Database (PostgreSQL)
-- =======================================================

-- 1. Insert Mock Data into tbCaseData
INSERT INTO tbCaseData (
    coE2I222, coPatientId, coE2I223, coE2I228, coLastname, coFirstname,
    coGender, coDateOfBirth, coAgeYears, coTypeOfStay, coIcd, coDrgName,
    coRecliningType, coState
) VALUES
(1001, 5001, '2026-02-13 08:00:00', '2026-02-18 12:00:00', 'Smith', 'John', 'M', '1980-05-15 00:00:00', 45, 'Inpatient', 'I10', 'Heart Failure', 'Standard', 'Discharged'),
(1002, 5002, '2026-02-14 09:30:00', NULL, 'Doe', 'Jane', 'F', '1992-11-20 00:00:00', 33, 'Outpatient', 'E11.9', 'Type 2 Diabetes', 'Standard', 'Admitted'),
(1003, 5003, '2026-02-10 14:15:00', '2026-02-15 10:00:00', 'Miller', 'David', 'M', '1975-03-08 00:00:00', 50, 'Inpatient', 'J45.9', 'Asthma', 'Intensive Care', 'Discharged');

-- 2. Insert Mock Data into tbImportAcData
INSERT INTO tbImportAcData (
    coCaseId, coE0I001, coE0I005, coE0I0004, coE2I225, coE2I230
) VALUES
(1, 10, 98.600, 'Routine Checkup', '2026-02-13 08:30:00', 'General Ward'),
(2, 20, 99.100, 'Blood Sugar Monitoring', '2026-02-14 10:00:00', 'Outpatient Clinic'),
(3, 30, 101.200, 'Severe Asthma Attack', '2026-02-10 14:45:00', 'ICU');

-- 3. Insert Mock Data into tbImportLabsData
INSERT INTO tbImportLabsData (
    coCaseId, coSpecimen_datetime, coSodium_mmol_L, coSodium_ref_low, coSodium_ref_high,
    coGlucose_mg_dL, coGlucose_ref_low, coGlucose_ref_high
) VALUES
(1, '2026-02-13T09:00:00', '140', '135', '145', '95', '70', '100'),
(2, '2026-02-14T10:15:00', '138', '135', '145', '150', '70', '100'),
(3, '2026-02-10T15:00:00', '142', '135', '145', '110', '70', '100');

-- 4. Insert Mock Data into tbImportIcd10Data
INSERT INTO tbImportIcd10Data (
    coCaseId, coWard, coAdmission_date, coDischarge_date, coLength_of_stay_days,
    coPrimary_icd10_code, coPrimary_icd10_description_en
) VALUES
(1, 'Cardiology', '2026-02-13', '2026-02-18', '5', 'I10', 'Essential (primary) hypertension'),
(2, 'Endocrinology', '2026-02-14', NULL, NULL, 'E11.9', 'Type 2 diabetes mellitus without complications'),
(3, 'Pulmonology', '2026-02-10', '2026-02-15', '5', 'J45.9', 'Asthma, unspecified');

-- 5. Insert Mock Data into tbImportDeviceMotionData
INSERT INTO tbImportDeviceMotionData (
    coCaseId, coTimestamp, coPatient_id, coMovement_index_0_100, coBed_exit_detected_0_1, coFall_event_0_1
) VALUES
(1, '2026-02-14 02:00:00', '5001', '15', '0', '0'),
(3, '2026-02-12 03:30:00', '5003', '85', '1', '1');

-- 6. Insert Mock Data into tbImportMedicationInpatientData
INSERT INTO tbImportMedicationInpatientData (
    coCaseId, coPatient_id, coWard, coMedication_name, coDose, coDose_unit,
    coFrequency, coOrder_start_datetime, order_status
) VALUES
(1, '5001', 'Cardiology', 'Lisinopril', '10', 'mg', '1x daily', '2026-02-13T10:00:00', 'Completed'),
(2, '5002', 'Endocrinology', 'Metformin', '500', 'mg', '2x daily', '2026-02-14T11:00:00', 'Active'),
(3, '5003', 'ICU', 'Albuterol', '2.5', 'mg', 'As needed', '2026-02-10T16:00:00', 'Completed');

-- 7. Insert Mock Data into tbImportNursingDailyReportsData
INSERT INTO tbImportNursingDailyReportsData (
    coCaseId, coPatient_id, coWard, coReport_date, coShift, coNursing_note_free_text
) VALUES
(1, '5001', 'Cardiology', '2026-02-14', 'Night', 'Patient slept well. No complaints of chest pain. BP stable at 120/80.'),
(2, '5002', 'Endocrinology', '2026-02-14', 'Day', 'Patient admitted. Blood sugar slightly elevated. Educated on diet.'),
(3, '5003', 'ICU', '2026-02-11', 'Night', 'Patient experienced mild shortness of breath. Albuterol administered. Breathing improved.');
