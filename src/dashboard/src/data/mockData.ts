// ===== tbCaseData =====
export interface CaseDataRecord {
  coId: number;
  coE2I222: number | null;
  coPatientId: number | null;
  coE2I223: string | null;
  coE2I228: string | null;
  coLastname: string | null;
  coFirstname: string | null;
  coGender: string | null;
  coDateOfBirth: string | null;
  coAgeYears: number | null;
  coTypeOfStay: string | null;
  coIcd: string | null;
  coDrgName: string | null;
  coRecliningType: string | null;
  coState: string | null;
}

// ===== tblImportAcData (assessment/care - many coded fields) =====
export interface AcData {
  coId: number;
  coCaseId: number;
  // Key assessment fields (showing representative subset)
  coE0I001: number | null;
  coE0I002: number | null;
  coE0I003: number | null;
  coE0I004: number | null;
  coE0I005: number | null; // numeric(6,3)
  coE0I007: number | null;
  coE0I008: number | null;
  coE0I009: number | null;
  coE0I010: number | null;
  coE0I011: number | null;
  coE0I012: number | null;
  coE0I013: number | null;
  coE0I014: number | null;
  coE0I015: number | null;
  coE0I021: number | null;
  coE0I043: number | null;
  coE0I070: number | null;
  coE0I0004: string | null; // nvarchar
  coMaxDekuGrad: number | null;
  coDekubitusWertTotal: number | null;
  coLastAssessment: number | null;
  coE3I0889: string | null;
  // ... 200+ more coE2Ixxx fields omitted for brevity
  [key: string]: number | string | null; // allow dynamic access
}

// ===== tblImportLabsData =====
export interface LabData {
  coId: number;
  coCaseId: number;
  coSpecimen_datetime: string | null;
  coSodium_mmol_L: string | null;
  coSodium_flag: string | null;
  cosodium_ref_low: string | null;
  cosodium_ref_high: string | null;
  coPotassium_mmol_L: string | null;
  coPotassium_flag: string | null;
  coCreatinine_mg_dL: string | null;
  coCreatinine_flag: string | null;
  coEgfr_mL_min_1_73m2: string | null;
  coEgfr_flag: string | null;
  coGlucose_mg_dL: string | null;
  coGlucose_flag: string | null;
  coHemoglobin_g_dL: string | null;
  coHb_flag: string | null;
  coWbc_10e9_L: string | null;
  coWbc_flag: string | null;
  coPlatelets_10e9_L: string | null;
  coPlatelets_flag: string | null;
  coCrp_mg_L: string | null;
  coCrp_flag: string | null;
  coAlt_U_L: string | null;
  coAlt_flag: string | null;
  coAst_U_L: string | null;
  coAst_flag: string | null;
  coBilirubin_mg_dL: string | null;
  coBilirubin_flag: string | null;
  coAlbumin_g_dL: string | null;
  coAlbumin_flag: string | null;
  coInr: string | null;
  coInr_flag: string | null;
  coLactate_mmol_L: string | null;
  coLactate_flag: string | null;
  [key: string]: number | string | null;
}

// ===== tblImportIcd10Data =====
export interface Icd10Data {
  coId: number;
  coCaseId: number;
  coWard: string | null;
  coAdmission_date: string | null;
  coDischarge_date: string | null;
  coLength_of_stay_days: string | null;
  coPrimary_icd10_code: string | null;
  coPrimary_icd10_description_en: string | null;
  coSecondary_icd10_codes: string | null;
  cpSecondary_icd10_descriptions_en: string | null;
  coOps_codes: string | null;
  ops_descriptions_en: string | null;
  [key: string]: number | string | null;
}

// ===== tblImportDeviceMotionData =====
export interface DeviceMotionData {
  coId: number;
  coCaseId: number;
  coTimestamp: string | null;
  coPatient_id: string | null;
  coMovement_index_0_100: string | null;
  coMicro_movements_count: string | null;
  coBed_exit_detected_0_1: string | null;
  coFall_event_0_1: string | null;
  coImpact_magnitude_g: string | null;
  coPost_fall_immobility_minutes: string | null;
  [key: string]: number | string | null;
}

// ===== tblImportDevice1HzMotionData =====
export interface Device1HzMotionData {
  coId: number;
  coCaseId: number;
  coTimestamp: string | null;
  coPatient_id: string | null;
  coDevice_id: string | null;
  coBed_occupied_0_1: string | null;
  coMovement_score_0_100: string | null;
  coAccel_x_m_s2: string | null;
  coAccel_y_m_s2: string | null;
  coAccel_z_m_s2: string | null;
  coAccel_magnitude_g: string | null;
  coPressure_zone1_0_100: string | null;
  coPressure_zone2_0_100: string | null;
  coPressure_zone3_0_100: string | null;
  coPressure_zone4_0_100: string | null;
  coBed_exit_event_0_1: string | null;
  coBed_return_event_0_1: string | null;
  coFall_event_0_1: string | null;
  coImpact_magnitude_g: string | null;
  coEvent_id: string | null;
  [key: string]: number | string | null;
}

// ===== tblImportMedicationInpatientData =====
export interface MedicationData {
  coId: number;
  coCaseId: number;
  coPatient_id: string | null;
  coRecord_type: string | null;
  coEncounter_id: string | null;
  coWard: string | null;
  coAdmission_datetime: string | null;
  coDischarge_datetime: string | null;
  coOrder_id: string | null;
  coMedication_code_atc: string | null;
  coMedication_name: string | null;
  coRoute: string | null;
  coDose: string | null;
  coDose_unit: string | null;
  coFrequency: string | null;
  coOrder_start_datetime: string | null;
  coOrder_stop_datetime: string | null;
  coIs_prn_0_1: string | null;
  coIndication: string | null;
  prescriber_role: string | null;
  order_status: string | null;
  administration_datetime: string | null;
  administered_dose: string | null;
  administered_unit: string | null;
  administration_status: string | null;
  note: string | null;
  [key: string]: number | string | null;
}

// ===== tblImportNursingDailyReportsData =====
export interface NursingDailyReport {
  coId: number;
  coCaseId: number;
  coPatient_id: string | null;
  coWard: string | null;
  coReport_date: string | null;
  coShift: string | null;
  coNursing_note_free_text: string | null;
  [key: string]: number | string | null;
}

// ===== Aggregated case with linked tables =====
export interface CaseRecord {
  caseData: CaseDataRecord;
  tables: {
    acData?: Partial<AcData>[];
    labsData?: Partial<LabData>[];
    icd10Data?: Partial<Icd10Data>[];
    deviceMotion?: Partial<DeviceMotionData>[];
    device1HzMotion?: Partial<Device1HzMotionData>[];
    medication?: Partial<MedicationData>[];
    nursingReports?: Partial<NursingDailyReport>[];
  };
}

// ===== Field labels for human-readable display =====
export const fieldLabels: Record<string, string> = {
  // tbCaseData
  coE2I222: 'Fall-Kennung',
  coPatientId: 'Patienten-ID',
  coE2I223: 'Aufnahmedatum',
  coE2I228: 'Entlassdatum',
  coLastname: 'Nachname',
  coFirstname: 'Vorname',
  coGender: 'Geschlecht',
  coDateOfBirth: 'Geburtsdatum',
  coAgeYears: 'Alter (Jahre)',
  coTypeOfStay: 'Aufenthaltsart',
  coIcd: 'ICD-10',
  coDrgName: 'DRG-Name',
  coRecliningType: 'Aufnahmeart',
  coState: 'Status',

  // tblImportAcData - key fields
  coE0I001: 'Pflegegrad',
  coE0I002: 'Mobilität',
  coE0I003: 'Kognitive Fähigkeiten',
  coE0I004: 'Verhaltensweisen',
  coE0I005: 'Braden-Score',
  coE0I007: 'Sturzrisiko',
  coE0I008: 'Ernährungsstatus',
  coE0I009: 'Kontinenz Harn',
  coE0I010: 'Kontinenz Stuhl',
  coE0I011: 'Schmerz',
  coE0I012: 'Atmung',
  coE0I013: 'Bewusstsein',
  coE0I014: 'Kommunikation',
  coE0I015: 'Orientierung',
  coE0I021: 'Selbstpflege',
  coE0I043: 'Dekubitus-Risiko',
  coE0I070: 'Barthel-Index',
  coE0I0004: 'Freitext Bewertung',
  coMaxDekuGrad: 'Max. Dekubitus-Grad',
  coDekubitusWertTotal: 'Dekubitus Gesamtwert',
  coLastAssessment: 'Letztes Assessment',
  coE3I0889: 'Zusatzinformation',

  // tblImportLabsData
  coSpecimen_datetime: 'Probeentnahme-Datum',
  coSodium_mmol_L: 'Natrium (mmol/L)',
  coSodium_flag: 'Natrium Flag',
  cosodium_ref_low: 'Natrium Ref. niedrig',
  cosodium_ref_high: 'Natrium Ref. hoch',
  coPotassium_mmol_L: 'Kalium (mmol/L)',
  coPotassium_flag: 'Kalium Flag',
  coCreatinine_mg_dL: 'Kreatinin (mg/dL)',
  coCreatinine_flag: 'Kreatinin Flag',
  coEgfr_mL_min_1_73m2: 'eGFR (mL/min)',
  coEgfr_flag: 'eGFR Flag',
  coGlucose_mg_dL: 'Glukose (mg/dL)',
  coGlucose_flag: 'Glukose Flag',
  coHemoglobin_g_dL: 'Hämoglobin (g/dL)',
  coHb_flag: 'Hb Flag',
  coWbc_10e9_L: 'Leukozyten (10⁹/L)',
  coWbc_flag: 'WBC Flag',
  coPlatelets_10e9_L: 'Thrombozyten (10⁹/L)',
  coPlatelets_flag: 'PLT Flag',
  coCrp_mg_L: 'CRP (mg/L)',
  coCrp_flag: 'CRP Flag',
  coAlt_U_L: 'ALT/GPT (U/L)',
  coAlt_flag: 'ALT Flag',
  coAst_U_L: 'AST/GOT (U/L)',
  coAst_flag: 'AST Flag',
  coBilirubin_mg_dL: 'Bilirubin (mg/dL)',
  coBilirubin_flag: 'Bilirubin Flag',
  coAlbumin_g_dL: 'Albumin (g/dL)',
  coAlbumin_flag: 'Albumin Flag',
  coInr: 'INR',
  coInr_flag: 'INR Flag',
  coLactate_mmol_L: 'Laktat (mmol/L)',
  coLactate_flag: 'Laktat Flag',

  // tblImportIcd10Data
  coWard: 'Station',
  coAdmission_date: 'Aufnahmedatum',
  coDischarge_date: 'Entlassdatum',
  coLength_of_stay_days: 'Aufenthaltsdauer (Tage)',
  coPrimary_icd10_code: 'Primärer ICD-10',
  coPrimary_icd10_description_en: 'Primärdiagnose (EN)',
  coSecondary_icd10_codes: 'Nebendiagnosen ICD-10',
  cpSecondary_icd10_descriptions_en: 'Nebendiagnosen (EN)',
  coOps_codes: 'OPS-Codes',
  ops_descriptions_en: 'OPS-Beschreibungen (EN)',

  // tblImportDeviceMotionData
  coTimestamp: 'Zeitstempel',
  coPatient_id: 'Patienten-ID',
  coMovement_index_0_100: 'Bewegungsindex (0-100)',
  coMicro_movements_count: 'Mikrobewegungen',
  coBed_exit_detected_0_1: 'Bettaustritt erkannt',
  coFall_event_0_1: 'Sturzereignis',
  coImpact_magnitude_g: 'Aufprallstärke (g)',
  coPost_fall_immobility_minutes: 'Immobilität nach Sturz (min)',

  // tblImportDevice1HzMotionData
  coDevice_id: 'Geräte-ID',
  coBed_occupied_0_1: 'Bett belegt',
  coMovement_score_0_100: 'Bewegungsscore (0-100)',
  coAccel_x_m_s2: 'Beschleunigung X (m/s²)',
  coAccel_y_m_s2: 'Beschleunigung Y (m/s²)',
  coAccel_z_m_s2: 'Beschleunigung Z (m/s²)',
  coAccel_magnitude_g: 'Beschleunigung Betrag (g)',
  coPressure_zone1_0_100: 'Druckzone 1 (0-100)',
  coPressure_zone2_0_100: 'Druckzone 2 (0-100)',
  coPressure_zone3_0_100: 'Druckzone 3 (0-100)',
  coPressure_zone4_0_100: 'Druckzone 4 (0-100)',
  coBed_exit_event_0_1: 'Bettaustritt-Ereignis',
  coBed_return_event_0_1: 'Bettrückkehr-Ereignis',
  coEvent_id: 'Ereignis-ID',

  // tblImportMedicationInpatientData
  coRecord_type: 'Datensatztyp',
  coEncounter_id: 'Kontakt-ID',
  coAdmission_datetime: 'Aufnahme-Datum/Zeit',
  coDischarge_datetime: 'Entlass-Datum/Zeit',
  coOrder_id: 'Verordnungs-ID',
  coMedication_code_atc: 'ATC-Code',
  coMedication_name: 'Medikament',
  coRoute: 'Verabreichungsweg',
  coDose: 'Dosis',
  coDose_unit: 'Dosiseinheit',
  coFrequency: 'Häufigkeit',
  coOrder_start_datetime: 'Verordnung Start',
  coOrder_stop_datetime: 'Verordnung Ende',
  coIs_prn_0_1: 'Bedarfsmedikation',
  coIndication: 'Indikation',
  prescriber_role: 'Verordner-Rolle',
  order_status: 'Verordnungsstatus',
  administration_datetime: 'Verabreichung Datum/Zeit',
  administered_dose: 'Verabreichte Dosis',
  administered_unit: 'Verabreichte Einheit',
  administration_status: 'Verabreichungsstatus',
  note: 'Bemerkung',

  // tblImportNursingDailyReportsData
  coReport_date: 'Berichtsdatum',
  coShift: 'Schicht',
  coNursing_note_free_text: 'Pflegebericht (Freitext)',
};

export const tableLabels: Record<string, { full: string; short: string }> = {
  acData: { full: 'tbImportAcData', short: 'Pflegebewertungen (Assessment/Care)' },
  labsData: { full: 'tbImportLabsData', short: 'Laborwerte' },
  icd10Data: { full: 'tbImportIcd10Data', short: 'ICD-10 Diagnosen & OPS' },
  deviceMotion: { full: 'tbImportDeviceMotionData', short: 'Geräte-Bewegungsdaten' },
  device1HzMotion: { full: 'tbImportDevice1HzMotionData', short: '1Hz Hochfrequenz-Sensordaten' },
  medication: { full: 'tbImportMedicationInpatientData', short: 'Medikation (stationär)' },
  nursingReports: { full: 'tbImportNursingDailyReportsData', short: 'Pflegetagesberichte' },
};

// ===== Mock Data =====

export const caseRecords: CaseRecord[] = [
  {
    caseData: { coId: 100201, coE2I222: 550001, coPatientId: 40001, coE2I223: '2024-05-10T08:00:00', coE2I228: null, coLastname: 'Müller', coFirstname: 'Hans', coGender: 'M', coDateOfBirth: '1946-03-12T00:00:00', coAgeYears: 78, coTypeOfStay: 'vollstationär', coIcd: 'E11.9', coDrgName: 'K60Z', coRecliningType: 'Notfall', coState: 'aktiv' },
    tables: {
      acData: [{
        coId: 1, coCaseId: 100201,
        coE0I001: 3, coE0I002: 2, coE0I003: 1, coE0I004: 1, coE0I005: 15.000, coE0I007: 3,
        coE0I008: 2, coE0I009: 2, coE0I010: 1, coE0I011: 2, coE0I012: 1, coE0I013: 1,
        coE0I014: 1, coE0I015: 1, coE0I021: 3, coE0I043: 2, coE0I070: 65,
        coE0I0004: 'Patient kooperativ, eingeschränkte Mobilität', coMaxDekuGrad: 1, coDekubitusWertTotal: 18, coLastAssessment: 1, coE3I0889: null,
      }],
      labsData: [{
        coId: 1, coCaseId: 100201, coSpecimen_datetime: '2024-05-15T10:30:00',
        coSodium_mmol_L: '141', coSodium_flag: 'N', cosodium_ref_low: '136', cosodium_ref_high: '145',
        coPotassium_mmol_L: '4.2', coPotassium_flag: 'N',
        coCreatinine_mg_dL: '1.1', coCreatinine_flag: 'N',
        coEgfr_mL_min_1_73m2: '68', coEgfr_flag: 'L',
        coGlucose_mg_dL: '185', coGlucose_flag: 'H',
        coHemoglobin_g_dL: '13.2', coHb_flag: 'N',
        coWbc_10e9_L: '7.8', coWbc_flag: 'N',
        coPlatelets_10e9_L: '245', coPlatelets_flag: 'N',
        coCrp_mg_L: '3.2', coCrp_flag: 'N',
        coAlt_U_L: '28', coAlt_flag: 'N',
        coAst_U_L: '25', coAst_flag: 'N',
        coBilirubin_mg_dL: '0.8', coBilirubin_flag: 'N',
        coAlbumin_g_dL: '3.8', coAlbumin_flag: 'N',
        coInr: '1.0', coInr_flag: 'N',
        coLactate_mmol_L: '1.1', coLactate_flag: 'N',
      }],
      icd10Data: [{
        coId: 1, coCaseId: 100201, coWard: 'Station A', coAdmission_date: '2024-05-10', coDischarge_date: null,
        coLength_of_stay_days: '36', coPrimary_icd10_code: 'E11.9', coPrimary_icd10_description_en: 'Type 2 diabetes mellitus without complications',
        coSecondary_icd10_codes: 'I10;M19.9', cpSecondary_icd10_descriptions_en: 'Essential hypertension;Arthrosis, unspecified',
        coOps_codes: null, ops_descriptions_en: null,
      }],
      deviceMotion: [{
        coId: 1, coCaseId: 100201, coTimestamp: '2024-06-14T22:15:00', coPatient_id: '40001',
        coMovement_index_0_100: '42', coMicro_movements_count: '156',
        coBed_exit_detected_0_1: '0', coFall_event_0_1: '0',
        coImpact_magnitude_g: null, coPost_fall_immobility_minutes: null,
      }],
      medication: [
        { coId: 1, coCaseId: 100201, coPatient_id: '40001', coRecord_type: 'order', coEncounter_id: 'ENC-100201', coWard: 'Station A', coAdmission_datetime: '2024-05-10T08:00:00', coDischarge_datetime: null, coOrder_id: 'ORD-001', coMedication_code_atc: 'A10BA02', coMedication_name: 'Metformin', coRoute: 'PO', coDose: '500', coDose_unit: 'mg', coFrequency: 'BID', coOrder_start_datetime: '2024-05-10T09:00:00', coOrder_stop_datetime: null, coIs_prn_0_1: '0', coIndication: 'Diabetes Typ 2', prescriber_role: 'Arzt', order_status: 'active', administration_datetime: '2024-06-15T08:00:00', administered_dose: '500', administered_unit: 'mg', administration_status: 'given', note: null },
        { coId: 2, coCaseId: 100201, coPatient_id: '40001', coRecord_type: 'order', coEncounter_id: 'ENC-100201', coWard: 'Station A', coAdmission_datetime: '2024-05-10T08:00:00', coDischarge_datetime: null, coOrder_id: 'ORD-002', coMedication_code_atc: 'C09AA05', coMedication_name: 'Ramipril', coRoute: 'PO', coDose: '5', coDose_unit: 'mg', coFrequency: 'QD', coOrder_start_datetime: '2024-05-10T09:00:00', coOrder_stop_datetime: null, coIs_prn_0_1: '0', coIndication: 'Hypertonie', prescriber_role: 'Arzt', order_status: 'active', administration_datetime: '2024-06-15T08:00:00', administered_dose: '5', administered_unit: 'mg', administration_status: 'given', note: null },
      ],
      nursingReports: [
        { coId: 1, coCaseId: 100201, coPatient_id: '40001', coWard: 'Station A', coReport_date: '2024-06-15', coShift: 'Früh', coNursing_note_free_text: 'Patient kooperativ. Mobilisierung mit Rollator durchgeführt. BZ-Werte erhöht, Arzt informiert.' },
        { coId: 2, coCaseId: 100201, coPatient_id: '40001', coWard: 'Station A', coReport_date: '2024-06-14', coShift: 'Spät', coNursing_note_free_text: 'Vitalzeichen stabil. Leichte Unruhe am Abend. Schmerzmedikation verabreicht.' },
      ],
    },
  },
  {
    caseData: { coId: 100202, coE2I222: 550002, coPatientId: 40001, coE2I223: '2024-03-01T10:00:00', coE2I228: '2024-03-18T14:00:00', coLastname: 'Müller', coFirstname: 'Hans', coGender: 'M', coDateOfBirth: '1946-03-12T00:00:00', coAgeYears: 78, coTypeOfStay: 'vollstationär', coIcd: 'I10', coDrgName: 'F67C', coRecliningType: 'Elektiv', coState: 'entlassen' },
    tables: {
      labsData: [{
        coId: 2, coCaseId: 100202, coSpecimen_datetime: '2024-03-05T09:00:00',
        coSodium_mmol_L: '139', coSodium_flag: 'N', cosodium_ref_low: '136', cosodium_ref_high: '145',
        coPotassium_mmol_L: '4.5', coPotassium_flag: 'N',
        coCreatinine_mg_dL: '1.3', coCreatinine_flag: 'H',
        coEgfr_mL_min_1_73m2: '55', coEgfr_flag: 'L',
        coGlucose_mg_dL: '142', coGlucose_flag: 'H',
        coHemoglobin_g_dL: '12.8', coHb_flag: 'N',
        coWbc_10e9_L: null, coWbc_flag: null,
        coPlatelets_10e9_L: null, coPlatelets_flag: null,
        coCrp_mg_L: '8.5', coCrp_flag: 'H',
        coAlt_U_L: null, coAlt_flag: null,
        coAst_U_L: null, coAst_flag: null,
        coBilirubin_mg_dL: null, coBilirubin_flag: null,
        coAlbumin_g_dL: null, coAlbumin_flag: null,
        coInr: null, coInr_flag: null,
        coLactate_mmol_L: null, coLactate_flag: null,
      }],
      icd10Data: [{
        coId: 2, coCaseId: 100202, coWard: 'Station A', coAdmission_date: '2024-03-01', coDischarge_date: '2024-03-18',
        coLength_of_stay_days: '17', coPrimary_icd10_code: 'I10', coPrimary_icd10_description_en: 'Essential (primary) hypertension',
        coSecondary_icd10_codes: 'E11.9', cpSecondary_icd10_descriptions_en: 'Type 2 diabetes mellitus without complications',
        coOps_codes: null, ops_descriptions_en: null,
      }],
    },
  },
  {
    caseData: { coId: 100301, coE2I222: 550010, coPatientId: 40002, coE2I223: '2024-05-18T14:00:00', coE2I228: null, coLastname: 'Schmidt', coFirstname: 'Anna', coGender: 'F', coDateOfBirth: '1959-08-22T00:00:00', coAgeYears: 65, coTypeOfStay: 'vollstationär', coIcd: 'I10', coDrgName: 'F67C', coRecliningType: 'Elektiv', coState: 'aktiv' },
    tables: {
      acData: [{
        coId: 3, coCaseId: 100301,
        coE0I001: 1, coE0I002: 1, coE0I003: 1, coE0I004: 0, coE0I005: 21.000, coE0I007: 1,
        coE0I008: 1, coE0I009: 1, coE0I010: 1, coE0I011: 1, coE0I012: 1, coE0I013: 1,
        coE0I014: 1, coE0I015: 1, coE0I021: 1, coE0I043: 0, coE0I070: 95,
        coE0I0004: 'Patientin vollständig selbstständig', coMaxDekuGrad: 0, coDekubitusWertTotal: 23, coLastAssessment: 1, coE3I0889: null,
      }],
      deviceMotion: [{
        coId: 2, coCaseId: 100301, coTimestamp: '2024-06-13T08:30:00', coPatient_id: '40002',
        coMovement_index_0_100: '78', coMicro_movements_count: '342',
        coBed_exit_detected_0_1: '1', coFall_event_0_1: '0',
        coImpact_magnitude_g: null, coPost_fall_immobility_minutes: null,
      }],
      nursingReports: [
        { coId: 3, coCaseId: 100301, coPatient_id: '40002', coWard: 'Station B', coReport_date: '2024-06-14', coShift: 'Früh', coNursing_note_free_text: 'Patientin selbstständig, gute Kooperation. Entlassung geplant.' },
      ],
    },
  },
  {
    caseData: { coId: 100401, coE2I222: 550020, coPatientId: 40003, coE2I223: '2024-04-05T11:00:00', coE2I228: null, coLastname: 'Weber', coFirstname: 'Karl', coGender: 'M', coDateOfBirth: '1942-01-05T00:00:00', coAgeYears: 82, coTypeOfStay: 'vollstationär', coIcd: 'F03', coDrgName: 'B76Z', coRecliningType: 'Notfall', coState: 'aktiv' },
    tables: {
      acData: [{
        coId: 4, coCaseId: 100401,
        coE0I001: 4, coE0I002: 4, coE0I003: 4, coE0I004: 3, coE0I005: 10.000, coE0I007: 4,
        coE0I008: 3, coE0I009: 3, coE0I010: 2, coE0I011: 2, coE0I012: 1, coE0I013: 3,
        coE0I014: 3, coE0I015: 4, coE0I021: 4, coE0I043: 3, coE0I070: 30,
        coE0I0004: 'Patient desorientiert, erhöhte Sturzgefahr, Fixierung notwendig', coMaxDekuGrad: 2, coDekubitusWertTotal: 12, coLastAssessment: 1, coE3I0889: 'Dekubitus Kategorie II sakral',
      }],
      deviceMotion: [
        { coId: 3, coCaseId: 100401, coTimestamp: '2024-06-12T23:45:00', coPatient_id: '40003', coMovement_index_0_100: '15', coMicro_movements_count: '23', coBed_exit_detected_0_1: '1', coFall_event_0_1: '1', coImpact_magnitude_g: '2.8', coPost_fall_immobility_minutes: '4' },
      ],
      device1HzMotion: [
        { coId: 1, coCaseId: 100401, coTimestamp: '2024-06-12T23:45:12', coPatient_id: '40003', coDevice_id: 'DEV-003', coBed_occupied_0_1: '0', coMovement_score_0_100: '85', coAccel_x_m_s2: '-0.12', coAccel_y_m_s2: '9.78', coAccel_z_m_s2: '0.45', coAccel_magnitude_g: '2.8', coPressure_zone1_0_100: '0', coPressure_zone2_0_100: '0', coPressure_zone3_0_100: '0', coPressure_zone4_0_100: '0', coBed_exit_event_0_1: '1', coBed_return_event_0_1: '0', coFall_event_0_1: '1', coImpact_magnitude_g: '2.8', coEvent_id: 'EVT-FALL-001' },
      ],
      medication: [
        { coId: 6, coCaseId: 100401, coPatient_id: '40003', coRecord_type: 'order', coEncounter_id: 'ENC-100401', coWard: 'Station A', coAdmission_datetime: '2024-04-05T11:00:00', coDischarge_datetime: null, coOrder_id: 'ORD-006', coMedication_code_atc: 'N06DA02', coMedication_name: 'Donepezil', coRoute: 'PO', coDose: '10', coDose_unit: 'mg', coFrequency: 'QD', coOrder_start_datetime: '2024-04-05T12:00:00', coOrder_stop_datetime: null, coIs_prn_0_1: '0', coIndication: 'Demenz', prescriber_role: 'Arzt', order_status: 'active', administration_datetime: '2024-06-15T08:00:00', administered_dose: '10', administered_unit: 'mg', administration_status: 'given', note: null },
      ],
      nursingReports: [
        { coId: 4, coCaseId: 100401, coPatient_id: '40003', coWard: 'Station A', coReport_date: '2024-06-13', coShift: 'Nacht', coNursing_note_free_text: 'Patient um 23:45 gestürzt. Sturzereignis vom Sensor erkannt. Keine sichtbaren Verletzungen. Arzt informiert. Überwachung verstärkt.' },
      ],
    },
  },
  {
    caseData: { coId: 100501, coE2I222: 550030, coPatientId: 40004, coE2I223: '2024-06-01T09:00:00', coE2I228: null, coLastname: 'Fischer', coFirstname: 'Maria', coGender: 'F', coDateOfBirth: '1953-11-30T00:00:00', coAgeYears: 71, coTypeOfStay: 'vollstationär', coIcd: 'J44.1', coDrgName: 'E65A', coRecliningType: 'Elektiv', coState: 'aktiv' },
    tables: {
      device1HzMotion: [
        { coId: 3, coCaseId: 100501, coTimestamp: '2024-06-12T06:00:00', coPatient_id: '40004', coDevice_id: 'DEV-004', coBed_occupied_0_1: '1', coMovement_score_0_100: '22', coAccel_x_m_s2: '0.01', coAccel_y_m_s2: '9.81', coAccel_z_m_s2: '0.02', coAccel_magnitude_g: '1.0', coPressure_zone1_0_100: '45', coPressure_zone2_0_100: '60', coPressure_zone3_0_100: '35', coPressure_zone4_0_100: '50', coBed_exit_event_0_1: '0', coBed_return_event_0_1: '0', coFall_event_0_1: '0', coImpact_magnitude_g: null, coEvent_id: null },
      ],
      medication: [
        { coId: 8, coCaseId: 100501, coPatient_id: '40004', coRecord_type: 'order', coEncounter_id: 'ENC-100501', coWard: 'Station C', coAdmission_datetime: '2024-06-01T09:00:00', coDischarge_datetime: null, coOrder_id: 'ORD-008', coMedication_code_atc: 'R03AC02', coMedication_name: 'Salbutamol', coRoute: 'INH', coDose: '100', coDose_unit: 'µg', coFrequency: 'PRN', coOrder_start_datetime: '2024-06-01T10:00:00', coOrder_stop_datetime: null, coIs_prn_0_1: '1', coIndication: 'COPD', prescriber_role: 'Arzt', order_status: 'active', administration_datetime: null, administered_dose: null, administered_unit: null, administration_status: null, note: 'Bei Dyspnoe' },
      ],
    },
  },
  {
    caseData: { coId: 100601, coE2I222: 550040, coPatientId: 40005, coE2I223: '2024-02-15T16:00:00', coE2I228: null, coLastname: 'Wagner', coFirstname: 'Fritz', coGender: 'M', coDateOfBirth: '1935-07-18T00:00:00', coAgeYears: 89, coTypeOfStay: 'vollstationär', coIcd: 'G20', coDrgName: 'B67D', coRecliningType: 'Notfall', coState: 'aktiv' },
    tables: {
      acData: [{
        coId: 5, coCaseId: 100601,
        coE0I001: 4, coE0I002: 4, coE0I003: 3, coE0I004: 2, coE0I005: 11.000, coE0I007: 5,
        coE0I008: 3, coE0I009: 3, coE0I010: 3, coE0I011: 3, coE0I012: 2, coE0I013: 2,
        coE0I014: 2, coE0I015: 3, coE0I021: 4, coE0I043: 4, coE0I070: 20,
        coE0I0004: 'Vollständige Unterstützung bei allen ADLs, Tremor erschwert Nahrungsaufnahme', coMaxDekuGrad: 2, coDekubitusWertTotal: 11, coLastAssessment: 1, coE3I0889: 'Dekubitus Kategorie II Ferse links',
      }],
      nursingReports: [
        { coId: 5, coCaseId: 100601, coPatient_id: '40005', coWard: 'Station B', coReport_date: '2024-06-11', coShift: 'Früh', coNursing_note_free_text: 'Patient benötigt vollständige Unterstützung. Tremor verstärkt. Lagerung alle 2h durchgeführt.' },
      ],
    },
  },
];

// Helper to get patient list from caseRecords
export interface PatientSummary {
  patientId: number;
  displayId: string;
  lastname: string;
  firstname: string;
  age: number;
  gender: string;
  icd: string;
  state: string;
  department: string;
  latestActivity: string;
  dischargeDate: string;
  lengthOfStay: number | null;
  caseCount: number;
}

export const getPatientSummaries = (): PatientSummary[] => {
  const map = new Map<number, PatientSummary>();
  caseRecords.forEach(cr => {
    const cd = cr.caseData;
    const pid = cd.coPatientId!;
    const existing = map.get(pid);
    if (!existing || cd.coE2I223! > existing.latestActivity) {
      map.set(pid, {
        patientId: pid,
        displayId: `P-${pid}`,
        lastname: cd.coLastname || '',
        firstname: cd.coFirstname || '',
        age: cd.coAgeYears || 0,
        gender: cd.coGender || '',
        icd: cd.coIcd || '',
        state: cd.coState || '',
        department: cr.tables.nursingReports?.[0]?.coWard || cr.tables.medication?.[0]?.coWard || cr.tables.icd10Data?.[0]?.coWard || '',
        latestActivity: cd.coE2I223 || '',
        dischargeDate: cd.coE2I228 || '',
        lengthOfStay: cd.coE2I223 && cd.coE2I228 ? Math.round((new Date(cd.coE2I228).getTime() - new Date(cd.coE2I223).getTime()) / (1000 * 60 * 60 * 24)) : null,
        caseCount: (existing?.caseCount || 0) + 1,
      });
    } else {
      existing.caseCount++;
    }
  });
  return Array.from(map.values());
};

// ===== Data Errors =====
export interface DataError {
  id: string;
  sourceInstitution: string;
  patientId: string;
  dataField: string;
  tableName: string;
  columnName: string;
  errorDescription: string;
  errorType: 'missing' | 'invalid' | 'range' | 'inconsistent';
  priority: 'high' | 'medium' | 'low';
  status: 'new' | 'pending' | 'corrected';
  category: string;
  correctedBy?: string;
  correctedAt?: string;
  comment?: string;
  // Fields populated when data comes from the real API
  rowId?: number;
  allMissingColumns?: string[];
}

export const dataErrors: DataError[] = [
  { id: 'E-001', sourceInstitution: 'Station 3A – Innere Medizin', patientId: 'P-40001', dataField: 'Sturzrisiko', tableName: 'tblImportAcData', columnName: 'coE0I007', errorDescription: 'Sturzrisiko fehlt für Fall 100202', errorType: 'missing', priority: 'high', status: 'new', category: 'tblImportAcData' },
  { id: 'E-002', sourceInstitution: 'Station 5B – Geriatrie', patientId: 'P-40003', dataField: 'Bewegungsindex', tableName: 'tblImportDeviceMotionData', columnName: 'coMovement_index_0_100', errorDescription: 'Bewegungsindex fehlt', errorType: 'missing', priority: 'high', status: 'new', category: 'tblImportDeviceMotionData' },
  { id: 'E-003', sourceInstitution: 'Station 3A – Innere Medizin', patientId: 'P-40002', dataField: 'Leukozyten', tableName: 'tblImportLabsData', columnName: 'coWbc_10e9_L', errorDescription: 'Laborwert Leukozyten fehlt', errorType: 'missing', priority: 'medium', status: 'pending', category: 'tblImportLabsData' },
  { id: 'E-004', sourceInstitution: 'Station 2C – Chirurgie', patientId: 'P-40004', dataField: 'Nebendiagnosen', tableName: 'tblImportIcd10Data', columnName: 'coSecondary_icd10_codes', errorDescription: 'Nebendiagnosen fehlen', errorType: 'missing', priority: 'medium', status: 'new', category: 'tblImportIcd10Data' },
  { id: 'E-005', sourceInstitution: 'Station 4A – Neurologie', patientId: 'P-40005', dataField: 'Geburtsdatum', tableName: 'tbCaseData', columnName: 'coDateOfBirth', errorDescription: 'Geburtsdatum fehlt', errorType: 'missing', priority: 'low', status: 'corrected', category: 'tbCaseData', correctedBy: 'Dr. Meier', correctedAt: '2024-06-10 14:30' },
  { id: 'E-006', sourceInstitution: 'Station 5B – Geriatrie', patientId: 'P-40003', dataField: 'Barthel-Index', tableName: 'tblImportAcData', columnName: 'coE0I070', errorDescription: 'Barthel-Index fehlt', errorType: 'missing', priority: 'high', status: 'new', category: 'tblImportAcData' },
  { id: 'E-007', sourceInstitution: 'Station 3A – Innere Medizin', patientId: 'P-40004', dataField: 'Beschleunigung', tableName: 'tblImportDevice1HzMotionData', columnName: 'coAccel_magnitude_g', errorDescription: 'Sensorwert Beschleunigung fehlt', errorType: 'missing', priority: 'medium', status: 'pending', category: 'tblImportDevice1HzMotionData' },
  { id: 'E-008', sourceInstitution: 'Station 2C – Chirurgie', patientId: 'P-40005', dataField: 'Pflegegrad', tableName: 'tblImportAcData', columnName: 'coE0I001', errorDescription: 'Pflegegrad fehlt', errorType: 'missing', priority: 'low', status: 'new', category: 'tblImportAcData' },
  { id: 'E-009', sourceInstitution: 'Station 4A – Neurologie', patientId: 'P-40001', dataField: 'ATC-Code', tableName: 'tblImportMedicationInpatientData', columnName: 'coMedication_code_atc', errorDescription: 'ATC-Code der Medikation fehlt', errorType: 'missing', priority: 'high', status: 'new', category: 'tblImportMedicationInpatientData' },
  { id: 'E-010', sourceInstitution: 'Station 5B – Geriatrie', patientId: 'P-40003', dataField: 'Pflegebericht', tableName: 'tblImportNursingDailyReportsData', columnName: 'coNursing_note_free_text', errorDescription: 'Pflegebericht-Freitext fehlt', errorType: 'missing', priority: 'high', status: 'new', category: 'tblImportNursingDailyReportsData' },
];

export interface SimilarCase {
  patientId: number;
  name: string;
  age: number;
  similarity: number;
  mainDiagnosis: string;
}

export const similarCases: SimilarCase[] = [
  { patientId: 40003, name: 'Weber, Karl', age: 82, similarity: 87, mainDiagnosis: 'Demenz (F03)' },
  { patientId: 40005, name: 'Wagner, Fritz', age: 89, similarity: 74, mainDiagnosis: 'Parkinson (G20)' },
  { patientId: 40002, name: 'Schmidt, Anna', age: 65, similarity: 61, mainDiagnosis: 'Hypertonie (I10)' },
  { patientId: 40004, name: 'Fischer, Maria', age: 71, similarity: 55, mainDiagnosis: 'COPD (J44.1)' },
];

export const dataQualityMetrics = {
  overallCompleteness: 82,
  anomaliesFound: 10,
  mappingSuccessRate: 94,
  missingByCategory: [
    { name: 'tblImportAcData', missing: 15, total: 100 },
    { name: 'tblImportLabsData', missing: 8, total: 100 },
    { name: 'tblImportIcd10Data', missing: 4, total: 100 },
    { name: 'tblImportDeviceMotionData', missing: 6, total: 100 },
    { name: 'tblImportDevice1HzMotionData', missing: 12, total: 100 },
    { name: 'tblImportMedicationInpatientData', missing: 3, total: 100 },
    { name: 'tblImportNursingDailyReportsData', missing: 5, total: 100 },
  ],
};

export const mappingRules = [
  { input: 'tblImportLabsData', rule: 'coCaseId → Fall-Zuordnung', target: 'tbCaseData', status: 'active' },
  { input: 'tblImportAcData', rule: 'coCaseId → Fall-Verknüpfung', target: 'tbCaseData', status: 'active' },
  { input: 'tblImportIcd10Data', rule: 'coCaseId → Diagnosen-Mapping', target: 'tbCaseData', status: 'active' },
  { input: 'tblImportDeviceMotionData', rule: 'coCaseId → Bewegungsanalyse', target: 'tbCaseData', status: 'active' },
  { input: 'tblImportDevice1HzMotionData', rule: 'coCaseId → 1Hz Sensordaten', target: 'tbCaseData', status: 'active' },
  { input: 'tblImportNursingDailyReportsData', rule: 'coCaseId → Pflegeberichte', target: 'tbCaseData', status: 'active' },
  { input: 'tblImportMedicationInpatientData', rule: 'coCaseId → Medikation', target: 'tbCaseData', status: 'warning' },
];

export const anomalyAlerts = [
  { id: 'A-01', message: 'Sturzereignis bei P-40003 (coFall_event_0_1=1, Impact 2.8g)', severity: 'high', timestamp: '2024-06-12 23:45', read: false },
  { id: 'A-02', message: 'Barthel-Index Wert 150 bei P-40003 – außerhalb gültiger Range (0-100)', severity: 'high', timestamp: '2024-06-15 08:15', read: false },
  { id: 'A-03', message: '3 neue NULL-Felder in tblImportLabsData (WBC, PLT, ALT) erkannt', severity: 'medium', timestamp: '2024-06-14 16:00', read: true },
  { id: 'A-04', message: 'Mapping-Regel für tblImportMedicationInpatientData: ungültiger ATC-Code', severity: 'medium', timestamp: '2024-06-14 11:45', read: true },
];
