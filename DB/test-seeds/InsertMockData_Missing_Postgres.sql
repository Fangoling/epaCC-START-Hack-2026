-- =======================================================
-- Mock Data with MISSING/NULL VALUES for Database (PostgreSQL)
-- Useful for testing missing data pipelines or error handling
-- =======================================================

-- 1. Insert Mock Data into tbCaseData
INSERT INTO tbCaseData (
    coE2I222, coPatientId, coE2I223, coE2I228, coLastname, coFirstname,
    coGender, coDateOfBirth, coAgeYears, coTypeOfStay, coIcd, coDrgName,
    coRecliningType, coState
) VALUES
(1004, 5004, '2026-02-13 08:00:00', NULL, 'Williams', 'Sarah', NULL, '1985-05-15 00:00:00', NULL, 'Inpatient', 'I10', NULL, 'Standard', 'Admitted'),
(1005, NULL, '2026-02-14 09:30:00', NULL, 'Brown', NULL, 'F', NULL, 33, 'Outpatient', NULL, 'Type 2 Diabetes', 'Standard', 'Admitted'),
(NULL, 5006, NULL, '2026-02-15 10:00:00', NULL, 'David', 'M', '1975-03-08 00:00:00', 50, NULL, 'J45.9', 'Asthma', NULL, 'Discharged');

-- 2. Insert Mock Data into tbImportAcData
INSERT INTO tbImportAcData (
    coCaseId, coE0I001, coE0I005, coE0I0004, coE2I225, coE2I230
) VALUES
(4, NULL, 98.600, NULL, '2026-02-13 08:30:00', 'General Ward'),
(5, 20, NULL, 'Blood Sugar Monitoring', NULL, 'Outpatient Clinic'),
(NULL, 30, 101.200, 'Severe Asthma Attack', '2026-02-10 14:45:00', NULL);

-- 3. Insert Mock Data into tbImportLabsData
INSERT INTO tbImportLabsData (
    coCaseId, coSpecimen_datetime, coSodium_mmol_L, coSodium_ref_low, coSodium_ref_high,
    coGlucose_mg_dL, coGlucose_ref_low, coGlucose_ref_high
) VALUES
(4, NULL, '140', '135', '145', NULL, '70', '100'),
(5, '2026-02-14T10:15:00', NULL, NULL, NULL, '150', '70', '100'),
(6, '2026-02-10T15:00:00', '142', '135', '145', '110', NULL, NULL);

-- 4. Insert Mock Data into tbImportIcd10Data
INSERT INTO tbImportIcd10Data (
    coCaseId, coWard, coAdmission_date, coDischarge_date, coLength_of_stay_days,
    coPrimary_icd10_code, coPrimary_icd10_description_en
) VALUES
(4, NULL, '2026-02-13', NULL, '5', 'I10', NULL),
(5, 'Endocrinology', NULL, NULL, NULL, NULL, 'Type 2 diabetes mellitus without complications'),
(NULL, 'Pulmonology', '2026-02-10', '2026-02-15', NULL, 'J45.9', 'Asthma, unspecified');

-- 5. Insert Mock Data into tbImportDeviceMotionData
INSERT INTO tbImportDeviceMotionData (
    coCaseId, coTimestamp, coPatient_id, coMovement_index_0_100, coBed_exit_detected_0_1, coFall_event_0_1
) VALUES
(4, '2026-02-14 02:00:00', NULL, '15', NULL, '0'),
(5, NULL, '5003', NULL, '1', NULL);

-- 6. Insert Mock Data into tbImportMedicationInpatientData
INSERT INTO tbImportMedicationInpatientData (
    coCaseId, coPatient_id, coWard, coMedication_name, coDose, coDose_unit,
    coFrequency, coOrder_start_datetime, order_status
) VALUES
(4, '5004', NULL, 'Lisinopril', NULL, 'mg', '1x daily', '2026-02-13T10:00:00', NULL),
(5, NULL, 'Endocrinology', NULL, '500', NULL, NULL, '2026-02-14T11:00:00', 'Active'),
(6, '5006', 'ICU', 'Albuterol', '2.5', 'mg', 'As needed', NULL, 'Completed');

-- 7. Insert Mock Data into tbImportNursingDailyReportsData
INSERT INTO tbImportNursingDailyReportsData (
    coCaseId, coPatient_id, coWard, coReport_date, coShift, coNursing_note_free_text
) VALUES
(4, '5004', NULL, '2026-02-14', 'Night', NULL),
(5, NULL, 'Endocrinology', NULL, 'Day', 'Patient admitted. Blood sugar slightly elevated. Educated on diet.'),
(6, '5006', 'ICU', '2026-02-11', NULL, 'Patient experienced mild shortness of breath. Albuterol administered. Breathing improved.');