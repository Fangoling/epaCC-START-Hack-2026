import type { CaseRecord, PatientSummary } from "@/data/mockData";

const API_BASE = "http://localhost:8000";

// --- Raw types from the API ---

export interface BrokenEntry {
  id: string;
  table: string;
  row_id: number;
  missing_columns: string[];
  context: Record<string, any>;
  patient_id: number | null;
}

export interface MissingDataResponse {
  totalCases: number;
  brokenEntries: BrokenEntry[];
}

export interface TableQualityMetrics {
  tableName: string;
  total: number;
  missing: number;
  completeness: number;
  missingPct: number;
}

export interface QualityMetricsResponse {
  overallCompleteness: number;
  totalRecords: number;
  missingRecords: number;
  byTable: Record<string, TableQualityMetrics>;
}

// --- API functions ---

export async function fetchPatients(): Promise<PatientSummary[]> {
  const res = await fetch(`${API_BASE}/api/patients`);
  if (!res.ok) throw new Error(`Failed to fetch patients: ${res.status}`);
  const rows: any[] = await res.json();

  return rows.map((r) => ({
    patientId: r.coPatientId,
    displayId: r.displayId,
    lastname: r.coLastname || "",
    firstname: r.coFirstname || "",
    age: r.coAgeYears || 0,
    gender: r.coGender || "",
    icd: r.coIcd || "",
    state: r.coState || "",
    department: "",
    latestActivity: r.latestAdmission || "",
    dischargeDate: r.latestDischarge || "",
    lengthOfStay:
      r.latestAdmission && r.latestDischarge
        ? Math.round(
            (new Date(r.latestDischarge).getTime() -
              new Date(r.latestAdmission).getTime()) /
              (1000 * 60 * 60 * 24)
          )
        : null,
    caseCount: r.caseCount,
  }));
}

export async function fetchCasesForPatient(patientId: number): Promise<CaseRecord[]> {
  const res = await fetch(`${API_BASE}/api/cases/${patientId}`);
  if (!res.ok) throw new Error(`Failed to fetch cases: ${res.status}`);
  return res.json();
}

export async function fetchMissingData(): Promise<MissingDataResponse> {
  const res = await fetch(`${API_BASE}/api/missing-data`);
  if (!res.ok) throw new Error(`Failed to fetch missing data: ${res.status}`);
  return res.json();
}

export async function fetchQualityMetrics(): Promise<QualityMetricsResponse> {
  const res = await fetch(`${API_BASE}/api/quality-metrics`);
  if (!res.ok) throw new Error(`Failed to fetch quality metrics: ${res.status}`);
  return res.json();
}

export async function fixRecord(
  table: string,
  rowId: number,
  columnName: string,
  newValue: string
): Promise<{ status: string; message: string }> {
  const res = await fetch(`${API_BASE}/api/missing-data/fix`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      table,
      row_id: rowId,
      column_name: columnName,
      new_value: newValue,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `Fix failed: ${res.status}`);
  }
  return res.json();
}
