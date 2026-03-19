import { useState } from "react";
import { ChevronDown, ChevronRight, Activity, FlaskConical, HeartPulse, Smartphone, FileText, Pill, AlertCircle, Database, Stethoscope } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { caseRecords, fieldLabels, tableLabels, type PatientSummary, type CaseRecord } from "@/data/mockData";

const tableConfig: Record<string, { label: string; icon: React.ElementType; color: string }> = {
  acData: { label: 'Pflegebewertungen', icon: Activity, color: 'text-epa-blue' },
  labsData: { label: 'Laborwerte', icon: FlaskConical, color: 'text-epa-success' },
  icd10Data: { label: 'ICD-10 / OPS', icon: Stethoscope, color: 'text-epa-purple' },
  deviceMotion: { label: 'Bewegungsdaten', icon: Smartphone, color: 'text-epa-info' },
  device1HzMotion: { label: '1Hz Sensordaten', icon: HeartPulse, color: 'text-epa-danger' },
  medication: { label: 'Medikation', icon: Pill, color: 'text-epa-warning' },
  nursingReports: { label: 'Pflegeberichte', icon: FileText, color: 'text-muted-foreground' },
};

// Fields to show prominently per table (avoid showing every coded field)
const keyFields: Record<string, string[]> = {
  acData: ['coE0I001', 'coE0I002', 'coE0I003', 'coE0I005', 'coE0I007', 'coE0I070', 'coE0I043', 'coMaxDekuGrad', 'coDekubitusWertTotal', 'coE0I0004', 'coE3I0889'],
  labsData: ['coSpecimen_datetime', 'coSodium_mmol_L', 'coSodium_flag', 'coPotassium_mmol_L', 'coPotassium_flag', 'coCreatinine_mg_dL', 'coCreatinine_flag', 'coEgfr_mL_min_1_73m2', 'coEgfr_flag', 'coGlucose_mg_dL', 'coGlucose_flag', 'coHemoglobin_g_dL', 'coHb_flag', 'coWbc_10e9_L', 'coWbc_flag', 'coPlatelets_10e9_L', 'coPlatelets_flag', 'coCrp_mg_L', 'coCrp_flag', 'coAlt_U_L', 'coAst_U_L', 'coBilirubin_mg_dL', 'coAlbumin_g_dL', 'coInr', 'coLactate_mmol_L'],
  icd10Data: ['coWard', 'coAdmission_date', 'coDischarge_date', 'coLength_of_stay_days', 'coPrimary_icd10_code', 'coPrimary_icd10_description_en', 'coSecondary_icd10_codes', 'cpSecondary_icd10_descriptions_en', 'coOps_codes', 'ops_descriptions_en'],
  deviceMotion: ['coTimestamp', 'coPatient_id', 'coMovement_index_0_100', 'coMicro_movements_count', 'coBed_exit_detected_0_1', 'coFall_event_0_1', 'coImpact_magnitude_g', 'coPost_fall_immobility_minutes'],
  device1HzMotion: ['coTimestamp', 'coDevice_id', 'coBed_occupied_0_1', 'coMovement_score_0_100', 'coAccel_x_m_s2', 'coAccel_y_m_s2', 'coAccel_z_m_s2', 'coAccel_magnitude_g', 'coPressure_zone1_0_100', 'coPressure_zone2_0_100', 'coPressure_zone3_0_100', 'coPressure_zone4_0_100', 'coBed_exit_event_0_1', 'coFall_event_0_1', 'coImpact_magnitude_g', 'coEvent_id'],
  medication: ['coMedication_name', 'coMedication_code_atc', 'coRoute', 'coDose', 'coDose_unit', 'coFrequency', 'coIndication', 'coIs_prn_0_1', 'order_status', 'administration_status', 'note'],
  nursingReports: ['coWard', 'coReport_date', 'coShift', 'coNursing_note_free_text'],
};

interface Props {
  patient: PatientSummary;
}

const NullIndicator = () => (
  <span className="inline-flex items-center gap-1 text-xs text-epa-warning"><AlertCircle className="h-3 w-3" />NULL</span>
);

const FlagBadge = ({ flag }: { flag: string | null }) => {
  if (!flag || flag === 'N') return null;
  return <span className={`ml-1 inline-block rounded px-1 text-[10px] font-bold ${flag === 'H' ? 'bg-epa-danger/10 text-epa-danger' : 'bg-epa-info/10 text-epa-info'}`}>{flag === 'H' ? '↑' : '↓'}</span>;
};

const renderTableData = (tableKey: string, data: any[]) => {
  if (!data || data.length === 0) return null;
  const fields = keyFields[tableKey] || Object.keys(data[0]).filter(k => k !== 'coId' && k !== 'coCaseId');

  // For nursing reports, show as cards instead of table
  if (tableKey === 'nursingReports') {
    return (
      <div className="space-y-2 p-3">
        {data.map((row: any) => (
          <div key={row.coId} className="rounded-lg border border-border bg-card p-3">
            <div className="flex items-center gap-3 text-xs text-muted-foreground mb-2">
              <span className="font-medium">{row.coWard}</span>
              <span>{row.coReport_date}</span>
              <Badge variant="secondary" className="text-[10px]">{row.coShift}</Badge>
            </div>
            <p className="text-sm leading-relaxed">{row.coNursing_note_free_text || <NullIndicator />}</p>
          </div>
        ))}
      </div>
    );
  }

  // For medication, use a cleaner layout
  if (tableKey === 'medication') {
    return (
      <div className="space-y-2 p-3">
        {data.map((row: any) => (
          <div key={row.coId} className="rounded-lg border border-border bg-card p-3">
            <div className="flex items-center gap-2 mb-1">
              <span className="font-semibold text-sm">{row.coMedication_name || <NullIndicator />}</span>
              <span className="font-mono text-[10px] text-muted-foreground">{row.coMedication_code_atc}</span>
              {row.coIs_prn_0_1 === '1' && <Badge variant="secondary" className="text-[10px]">PRN</Badge>}
            </div>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground mt-1">
              <span>Dosis: <strong className="text-foreground">{row.coDose} {row.coDose_unit}</strong></span>
              <span>Route: <strong className="text-foreground">{row.coRoute}</strong></span>
              <span>Frequenz: <strong className="text-foreground">{row.coFrequency}</strong></span>
              <span>Indikation: <strong className="text-foreground">{row.coIndication || '–'}</strong></span>
              <span>Status: <strong className="text-foreground">{row.order_status || '–'}</strong></span>
              <span>Verabreichung: <strong className="text-foreground">{row.administration_status || '–'}</strong></span>
            </div>
            {row.note && <p className="mt-2 text-xs text-muted-foreground italic">📝 {row.note}</p>}
          </div>
        ))}
      </div>
    );
  }

  // Generic table for other types
  const presentFields = fields.filter(f => data.some((row: any) => row[f] !== undefined));

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead><tr className="border-b border-border">
          {presentFields.map(f => (
            <th key={f} className="px-3 py-2 text-left text-xs font-semibold text-muted-foreground whitespace-nowrap">
              <div>{fieldLabels[f] || f}</div>
              <div className="font-mono text-[10px] text-muted-foreground/50">{f}</div>
            </th>
          ))}
        </tr></thead>
        <tbody>
          {data.map((row: any, ri: number) => (
            <tr key={ri} className="border-b border-border/50">
              {presentFields.map(f => {
                const val = row[f];
                const isFlag = f.toLowerCase().includes('flag');
                return (
                  <td key={f} className="px-3 py-2 whitespace-nowrap">
                    {val === null || val === undefined ? (
                      <NullIndicator />
                    ) : isFlag ? (
                      <FlagBadge flag={String(val)} />
                    ) : (
                      <span className="font-medium">{String(val)}</span>
                    )}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const PatientCaseView = ({ patient }: Props) => {
  const [expandedCase, setExpandedCase] = useState<number | null>(null);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

  const patientCases = caseRecords.filter(c => c.caseData.coPatientId === patient.patientId);

  const getAvailableTables = (c: CaseRecord) =>
    Object.entries(c.tables).filter(([, data]) => data && (data as any[]).length > 0);

  const getCompleteness = (c: CaseRecord) => {
    const allTables = Object.keys(tableConfig);
    const present = getAvailableTables(c).length;
    return Math.round((present / allTables.length) * 100);
  };

  return (
    <div className="rounded-xl border border-border bg-card shadow-epa animate-fade-in">
      <div className="border-b border-border p-4">
        <div className="flex items-center gap-3">
          <Database className="h-5 w-5 text-primary" />
          <div>
            <h2 className="text-lg font-semibold">Fallübersicht: {patient.lastname}, {patient.firstname}</h2>
            <p className="text-sm text-muted-foreground">
              coPatientId: <span className="font-mono font-medium">{patient.patientId}</span> · {patient.age}J ·
              ICD: <span className="font-mono font-medium text-primary">{patient.icd}</span> ·
              {patientCases.length} Fall/Fälle in tbCaseData
            </p>
          </div>
        </div>
      </div>

      <div className="divide-y divide-border">
        {patientCases.length === 0 && (
          <div className="p-8 text-center text-muted-foreground">Keine Fälle in tbCaseData gefunden.</div>
        )}
        {patientCases.map(cr => {
          const cd = cr.caseData;
          const isExpanded = expandedCase === cd.coId;
          const tables = getAvailableTables(cr);
          const completeness = getCompleteness(cr);

          return (
            <div key={cd.coId}>
              <button
                onClick={() => { setExpandedCase(isExpanded ? null : cd.coId); setExpandedTable(null); }}
                className="flex w-full items-center gap-3 px-4 py-3 text-left transition-colors hover:bg-secondary/50"
              >
                {isExpanded ? <ChevronDown className="h-4 w-4 text-muted-foreground" /> : <ChevronRight className="h-4 w-4 text-muted-foreground" />}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-sm font-medium">Fall {cd.coE2I222}</span>
                    <span className={`inline-block rounded-full px-2 py-0.5 text-[10px] font-medium ${cd.coState === 'aktiv' ? 'bg-epa-success/10 text-epa-success' : 'bg-muted text-muted-foreground'}`}>{cd.coState}</span>
                  </div>
                  <div className="text-xs text-muted-foreground mt-0.5">
                    {cd.coRecliningType} · {cd.coTypeOfStay} · DRG: {cd.coDrgName} · Aufnahme: {cd.coE2I223?.split('T')[0]}
                    {cd.coE2I228 && ` · Entlassung: ${cd.coE2I228.split('T')[0]}`}
                  </div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  {tables.map(([key]) => {
                    const cfg = tableConfig[key];
                    if (!cfg) return null;
                    const Icon = cfg.icon;
                    return <Icon key={key} className={`h-4 w-4 ${cfg.color}`} title={cfg.label} />;
                  })}
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <div className="h-2 w-16 rounded-full bg-secondary">
                    <div className={`h-2 rounded-full transition-all ${completeness >= 70 ? 'bg-epa-success' : completeness >= 40 ? 'bg-epa-warning' : 'bg-epa-danger'}`} style={{ width: `${completeness}%` }} />
                  </div>
                  <span className="text-xs text-muted-foreground w-8">{completeness}%</span>
                </div>
              </button>

              {isExpanded && (
                <div className="border-t border-border bg-secondary/20 animate-fade-in">
                  {tables.map(([key, data]) => {
                    const cfg = tableConfig[key];
                    if (!cfg) return null;
                    const Icon = cfg.icon;
                    const isTableExpanded = expandedTable === `${cd.coId}-${key}`;
                    const tl = tableLabels[key];
                    return (
                      <div key={key} className="border-b border-border/50 last:border-0">
                        <button
                          onClick={() => setExpandedTable(isTableExpanded ? null : `${cd.coId}-${key}`)}
                          className="flex w-full items-center gap-3 px-6 py-2.5 text-left transition-colors hover:bg-secondary/50"
                        >
                          {isTableExpanded ? <ChevronDown className="h-3.5 w-3.5 text-muted-foreground" /> : <ChevronRight className="h-3.5 w-3.5 text-muted-foreground" />}
                          <Icon className={`h-4 w-4 ${cfg.color}`} />
                          <span className="text-sm font-medium">{tl?.short || cfg.label}</span>
                          <Badge variant="secondary" className="ml-auto text-[10px]">{(data as any[]).length} Einträge</Badge>
                        </button>
                        {isTableExpanded && (
                          <div className="bg-card/50 px-6 pb-3 animate-fade-in">
                            <div className="rounded-lg border border-border bg-card overflow-hidden">
                              {renderTableData(key, data as any[])}
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default PatientCaseView;
