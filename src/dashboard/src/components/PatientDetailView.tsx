import { useState } from "react";
import { Activity, FlaskConical, HeartPulse, Smartphone, FileText, Pill, AlertCircle, Database, Stethoscope, Pencil, Check, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { caseRecords, fieldLabels, tableLabels, type PatientSummary, type CaseRecord } from "@/data/mockData";
import { toast } from "sonner";

const tableConfig: Record<string, { label: string; icon: React.ElementType; color: string }> = {
  acData: { label: 'Pflegebewertungen', icon: Activity, color: 'text-epa-blue' },
  labsData: { label: 'Laborwerte', icon: FlaskConical, color: 'text-epa-success' },
  icd10Data: { label: 'ICD-10 / OPS', icon: Stethoscope, color: 'text-epa-purple' },
  deviceMotion: { label: 'Bewegungsdaten', icon: Smartphone, color: 'text-epa-info' },
  device1HzMotion: { label: '1Hz Sensordaten', icon: HeartPulse, color: 'text-epa-danger' },
  medication: { label: 'Medikation', icon: Pill, color: 'text-epa-warning' },
  nursingReports: { label: 'Pflegeberichte', icon: FileText, color: 'text-muted-foreground' },
};

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

const NullIndicator = ({ onEdit }: { onEdit?: () => void }) => (
  <button
    onClick={onEdit}
    className="inline-flex items-center gap-1 text-xs text-epa-warning hover:text-epa-danger transition-colors group cursor-pointer"
    title="Fehlender Wert — klicken zum Ergänzen"
  >
    <AlertCircle className="h-3 w-3" />
    <span>Fehlend</span>
    <Pencil className="h-2.5 w-2.5 opacity-0 group-hover:opacity-100 transition-opacity" />
  </button>
);

// Reference range display
const RefRange = ({ text }: { text: string }) => (
  <p className="mt-1 text-[9px] text-muted-foreground truncate">Norm: {text}</p>
);

// Lab value+flag pairs with clinical reference ranges for anomaly detection
const labValuePairs = [
  { value: 'coSodium_mmol_L', flag: 'coSodium_flag', label: 'Natrium', unit: 'mmol/L', refLow: 136, refHigh: 145 },
  { value: 'coPotassium_mmol_L', flag: 'coPotassium_flag', label: 'Kalium', unit: 'mmol/L', refLow: 3.5, refHigh: 5.1 },
  { value: 'coCreatinine_mg_dL', flag: 'coCreatinine_flag', label: 'Kreatinin', unit: 'mg/dL', refLow: 0.6, refHigh: 1.2 },
  { value: 'coEgfr_mL_min_1_73m2', flag: 'coEgfr_flag', label: 'eGFR', unit: 'mL/min', refLow: 90, refHigh: 999 },
  { value: 'coGlucose_mg_dL', flag: 'coGlucose_flag', label: 'Glukose', unit: 'mg/dL', refLow: 70, refHigh: 110 },
  { value: 'coHemoglobin_g_dL', flag: 'coHb_flag', label: 'Hämoglobin', unit: 'g/dL', refLow: 12.0, refHigh: 17.5 },
  { value: 'coWbc_10e9_L', flag: 'coWbc_flag', label: 'Leukozyten', unit: '10⁹/L', refLow: 4.0, refHigh: 11.0 },
  { value: 'coPlatelets_10e9_L', flag: 'coPlatelets_flag', label: 'Thrombozyten', unit: '10⁹/L', refLow: 150, refHigh: 400 },
  { value: 'coCrp_mg_L', flag: 'coCrp_flag', label: 'CRP', unit: 'mg/L', refLow: 0, refHigh: 5 },
  { value: 'coAlt_U_L', flag: 'coAlt_flag', label: 'ALT/GPT', unit: 'U/L', refLow: 0, refHigh: 45 },
  { value: 'coAst_U_L', flag: 'coAst_flag', label: 'AST/GOT', unit: 'U/L', refLow: 0, refHigh: 35 },
  { value: 'coBilirubin_mg_dL', flag: 'coBilirubin_flag', label: 'Bilirubin', unit: 'mg/dL', refLow: 0, refHigh: 1.2 },
  { value: 'coAlbumin_g_dL', flag: 'coAlbumin_flag', label: 'Albumin', unit: 'g/dL', refLow: 3.5, refHigh: 5.5 },
  { value: 'coInr', flag: 'coInr_flag', label: 'INR', unit: '', refLow: 0.8, refHigh: 1.2 },
  { value: 'coLactate_mmol_L', flag: 'coLactate_flag', label: 'Laktat', unit: 'mmol/L', refLow: 0.5, refHigh: 2.2 },
];

// Clinical thresholds for assessment fields
const assessmentThresholds: Record<string, { direction: 'high' | 'low'; threshold: number; warnLabel: string; refText: string }> = {
  coE0I005: { direction: 'low', threshold: 18, warnLabel: 'Dekubitusrisiko', refText: '> 18' },
  coE0I007: { direction: 'high', threshold: 3, warnLabel: 'Sturzrisiko', refText: '< 3' },
  coE0I070: { direction: 'low', threshold: 60, warnLabel: 'Eingeschränkte Selbstständigkeit', refText: '> 60' },
  coMaxDekuGrad: { direction: 'high', threshold: 2, warnLabel: 'Dekubitus', refText: '< 2' },
};

// Assessment fields grouped for card display
const acDataGroups = [
  { title: 'Pflegegrad & Mobilität', fields: ['coE0I001', 'coE0I002', 'coE0I003', 'coE0I004', 'coE0I070'] },
  { title: 'Risikobewertung', fields: ['coE0I005', 'coE0I007', 'coE0I043', 'coMaxDekuGrad', 'coDekubitusWertTotal'] },
  { title: 'Funktionsstatus', fields: ['coE0I008', 'coE0I009', 'coE0I010', 'coE0I011', 'coE0I012', 'coE0I013', 'coE0I014', 'coE0I015', 'coE0I021'] },
];

const deviceMotionFields = [
  { key: 'coMovement_index_0_100', label: 'Bewegungsindex', unit: '/ 100' },
  { key: 'coMicro_movements_count', label: 'Mikrobewegungen', unit: '' },
  { key: 'coBed_exit_detected_0_1', label: 'Bettaustritt', unit: '', isBool: true },
  { key: 'coFall_event_0_1', label: 'Sturzereignis', unit: '', isBool: true, danger: true },
  { key: 'coImpact_magnitude_g', label: 'Aufprallstärke', unit: 'g' },
  { key: 'coPost_fall_immobility_minutes', label: 'Immobilität nach Sturz', unit: 'min' },
];

const device1HzFields = [
  { key: 'coBed_occupied_0_1', label: 'Bett belegt', isBool: true },
  { key: 'coMovement_score_0_100', label: 'Bewegungsscore', unit: '/ 100' },
  { key: 'coAccel_magnitude_g', label: 'Beschleunigung', unit: 'g' },
  { key: 'coPressure_zone1_0_100', label: 'Druckzone 1', unit: '/ 100' },
  { key: 'coPressure_zone2_0_100', label: 'Druckzone 2', unit: '/ 100' },
  { key: 'coPressure_zone3_0_100', label: 'Druckzone 3', unit: '/ 100' },
  { key: 'coPressure_zone4_0_100', label: 'Druckzone 4', unit: '/ 100' },
  { key: 'coBed_exit_event_0_1', label: 'Bettaustritt', isBool: true },
  { key: 'coFall_event_0_1', label: 'Sturzereignis', isBool: true, danger: true },
  { key: 'coImpact_magnitude_g', label: 'Aufprallstärke', unit: 'g' },
];

const ValueCard = ({ label, value, unit, isBool, danger, mono, fieldKey, anomaly, refText }: { label: string; value: any; unit?: string; isBool?: boolean; danger?: boolean; mono?: boolean; fieldKey?: string; anomaly?: string; refText?: string }) => {
  const isNull = value === null || value === undefined;
  const boolActive = isBool && value === '1';
  const isDanger = danger && boolActive;
  const isAnomaly = !!anomaly;
  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState('');

  const handleSave = () => {
    toast.success(`"${label}" korrigiert: ${editValue}`, { description: 'Korrektur wurde gespeichert.' });
    setEditing(false);
    setEditValue('');
  };

  const borderClass = isDanger
    ? 'border-epa-danger/40 bg-epa-danger/5'
    : isAnomaly
    ? 'border-epa-danger/50 bg-epa-danger/5 ring-1 ring-epa-danger/20'
    : isNull
    ? 'border-epa-warning/30 bg-epa-warning/5'
    : 'border-border bg-card';

  return (
    <div className={`rounded-lg border p-2.5 ${borderClass} relative`} title={anomaly || undefined}>
      <p className="text-[11px] text-muted-foreground truncate">{label}</p>
      {isAnomaly && (
        <div className="absolute -top-1.5 -right-1.5 flex h-4 w-4 items-center justify-center rounded-full bg-epa-danger text-[8px] font-bold text-primary-foreground" title={anomaly}>
          ⚠
        </div>
      )}
      <div className="mt-1 flex items-baseline gap-1.5">
        {editing ? (
          <div className="flex items-center gap-1 w-full" onClick={e => e.stopPropagation()}>
            <Input
              autoFocus
              value={editValue}
              onChange={e => setEditValue(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter' && editValue) handleSave(); if (e.key === 'Escape') setEditing(false); }}
              className="h-7 text-xs px-2"
              placeholder={`${label}...`}
            />
            <button onClick={handleSave} disabled={!editValue} className="p-1 text-epa-success hover:bg-epa-success/10 rounded disabled:opacity-30"><Check className="h-3.5 w-3.5" /></button>
            <button onClick={() => setEditing(false)} className="p-1 text-muted-foreground hover:bg-secondary rounded"><X className="h-3.5 w-3.5" /></button>
          </div>
        ) : isNull ? (
          <NullIndicator onEdit={() => setEditing(true)} />
        ) : isBool ? (
          <span className={`text-sm font-bold ${boolActive ? (danger ? 'text-epa-danger' : 'text-epa-success') : 'text-muted-foreground'}`}>
            {boolActive ? (danger ? '⚠ Ja' : '✓ Ja') : 'Nein'}
          </span>
        ) : (
          <>
            <span className={`text-lg font-bold ${mono ? 'font-mono text-base' : ''} ${isAnomaly ? 'text-epa-danger' : ''}`}>{String(value)}</span>
            {unit && <span className="text-[11px] text-muted-foreground">{unit}</span>}
          </>
        )}
      </div>
      {refText && !editing && <RefRange text={refText} />}
    </div>
  );
};

const renderTableData = (tableKey: string, data: any[]) => {
  if (!data || data.length === 0) return null;

  // Lab data as card grid
  if (tableKey === 'labsData') {
    return (
      <div className="space-y-3">
        {data.map((row: any, i: number) => (
          <div key={i}>
            {row.coSpecimen_datetime && (
              <p className="mb-2 text-xs text-muted-foreground">Probeentnahme: <strong className="text-foreground">{row.coSpecimen_datetime}</strong></p>
            )}
            <div className="grid grid-cols-3 gap-2 sm:grid-cols-4 lg:grid-cols-5">
              {labValuePairs.map(lp => {
                const val = row[lp.value];
                const flag = row[lp.flag];
                if (val === undefined && flag === undefined) return null;
                // Anomaly detection: check if value is outside reference range
                let anomaly: string | undefined;
                if (val !== null && val !== undefined && lp.refLow !== undefined && lp.refHigh !== undefined) {
                  const num = Number(val);
                  if (!isNaN(num)) {
                    if (num < lp.refLow) anomaly = `Unter Referenz (${lp.refLow}–${lp.refHigh} ${lp.unit})`;
                    else if (num > lp.refHigh) anomaly = `Über Referenz (${lp.refLow}–${lp.refHigh} ${lp.unit})`;
                  }
                }
                const refText = lp.refHigh === 999 ? `≥ ${lp.refLow} ${lp.unit}` : lp.refLow === 0 ? `≤ ${lp.refHigh} ${lp.unit}` : `${lp.refLow}–${lp.refHigh} ${lp.unit}`;
                return <ValueCard key={lp.value} label={lp.label} value={val} unit={lp.unit} anomaly={anomaly} refText={refText} />;
              })}
            </div>
          </div>
        ))}
      </div>
    );
  }

  // Assessment data as grouped cards
  if (tableKey === 'acData') {
    return (
      <div className="space-y-3">
        {data.map((row: any, i: number) => (
          <div key={i} className="space-y-3">
            {acDataGroups.map(group => {
              const hasData = group.fields.some(f => row[f] !== undefined);
              if (!hasData) return null;
              return (
                <div key={group.title}>
                  <p className="mb-1.5 text-xs font-semibold text-muted-foreground uppercase tracking-wider">{group.title}</p>
                  <div className="grid grid-cols-3 gap-2 sm:grid-cols-4 lg:grid-cols-5">
                    {group.fields.map(f => {
                      if (row[f] === undefined) return null;
                      let anomaly: string | undefined;
                      let refText: string | undefined;
                      const threshold = assessmentThresholds[f];
                      if (threshold) {
                        refText = threshold.refText;
                        if (row[f] !== null) {
                          const num = Number(row[f]);
                          if (!isNaN(num)) {
                            if (threshold.direction === 'low' && num <= threshold.threshold) {
                              anomaly = threshold.warnLabel;
                            } else if (threshold.direction === 'high' && num >= threshold.threshold) {
                              anomaly = threshold.warnLabel;
                            }
                          }
                        }
                      }
                      return <ValueCard key={f} label={fieldLabels[f] || f} value={row[f]} anomaly={anomaly} refText={refText} />;
                    })}
                  </div>
                </div>
              );
            })}
            {/* Free text fields */}
            {row.coE0I0004 && (
              <div className="rounded-lg border border-border bg-card p-3">
                <p className="text-[11px] text-muted-foreground mb-1">Freitext Bewertung</p>
                <p className="text-sm">{row.coE0I0004}</p>
              </div>
            )}
            {row.coE3I0889 && (
              <div className="rounded-lg border border-border bg-secondary/50 p-3">
                <p className="text-[11px] text-muted-foreground mb-1">Zusatzinformation</p>
                <p className="text-sm font-medium">{row.coE3I0889}</p>
              </div>
            )}
          </div>
        ))}
      </div>
    );
  }

  // Device motion as card grid
  if (tableKey === 'deviceMotion') {
    return (
      <div className="space-y-3">
        {data.map((row: any, i: number) => (
          <div key={i}>
            <p className="mb-2 text-xs text-muted-foreground">Zeitstempel: <strong className="text-foreground">{row.coTimestamp}</strong></p>
            <div className="grid grid-cols-3 gap-2 sm:grid-cols-4 lg:grid-cols-6">
              {deviceMotionFields.map(f => (
                <ValueCard key={f.key} label={f.label} value={row[f.key]} unit={f.unit} isBool={f.isBool} danger={f.danger} />
              ))}
            </div>
          </div>
        ))}
      </div>
    );
  }

  // 1Hz sensor data as card grid
  if (tableKey === 'device1HzMotion') {
    return (
      <div className="space-y-3">
        {data.map((row: any, i: number) => (
          <div key={i}>
            <div className="flex items-center gap-4 mb-2 text-xs text-muted-foreground">
              <span>Zeitstempel: <strong className="text-foreground">{row.coTimestamp}</strong></span>
              <span>Gerät: <strong className="font-mono text-foreground">{row.coDevice_id}</strong></span>
              {row.coEvent_id && <Badge variant="secondary" className="text-[10px]">{row.coEvent_id}</Badge>}
            </div>
            <div className="grid grid-cols-3 gap-2 sm:grid-cols-4 lg:grid-cols-5">
              {device1HzFields.map(f => (
                <ValueCard key={f.key} label={f.label} value={row[f.key]} unit={f.unit} isBool={f.isBool} danger={f.danger} />
              ))}
            </div>
          </div>
        ))}
      </div>
    );
  }

  // Nursing reports as cards
  if (tableKey === 'nursingReports') {
    return (
      <div className="space-y-2">
        {data.map((row: any) => (
          <div key={row.coId} className="rounded-lg border border-border bg-card p-3">
            <div className="flex items-center gap-3 text-xs text-muted-foreground mb-1.5">
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

  // Medication as cards
  if (tableKey === 'medication') {
    return (
      <div className="space-y-2">
        {data.map((row: any) => (
          <div key={row.coId} className="rounded-lg border border-border bg-card p-3">
            <div className="flex items-center gap-2 mb-1">
              <span className="font-semibold text-sm">{row.coMedication_name || <NullIndicator />}</span>
              <span className="font-mono text-[10px] text-muted-foreground">{row.coMedication_code_atc}</span>
              {row.coIs_prn_0_1 === '1' && <Badge variant="secondary" className="text-[10px]">PRN</Badge>}
            </div>
            <div className="grid grid-cols-3 gap-x-4 gap-y-1 text-xs text-muted-foreground mt-1">
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

  // ICD-10 and generic fallback as key-value cards
  if (tableKey === 'icd10Data') {
    const fields = keyFields[tableKey];
    return (
      <div className="space-y-2">
        {data.map((row: any, i: number) => (
          <div key={i} className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
            {fields.map(f => {
              const val = row[f];
              if (val === undefined) return null;
              return <ValueCard key={f} label={fieldLabels[f] || f} value={val} mono={f.includes('icd10') || f.includes('ops')} />;
            })}
          </div>
        ))}
      </div>
    );
  }

  // Fallback: generic card grid
  const fields = Object.keys(data[0]).filter(k => k !== 'coId' && k !== 'coCaseId');
  return (
    <div className="space-y-2">
      {data.map((row: any, i: number) => (
        <div key={i} className="grid grid-cols-3 gap-2 sm:grid-cols-4">
          {fields.map(f => {
            const val = row[f];
            if (val === undefined) return null;
            return <ValueCard key={f} label={fieldLabels[f] || f} value={val} />;
          })}
        </div>
      ))}
    </div>
  );
};
const getAvailableTables = (c: CaseRecord) =>
  Object.entries(c.tables).filter(([, data]) => data && (data as any[]).length > 0);

const getCompleteness = (c: CaseRecord) => {
  const allTables = Object.keys(tableConfig);
  const present = getAvailableTables(c).length;
  return Math.round((present / allTables.length) * 100);
};

const PatientDetailView = ({ patient }: Props) => {
  const patientCases = caseRecords.filter(c => c.caseData.coPatientId === patient.patientId);
  const [expandedCase, setExpandedCase] = useState<number | null>(patientCases.length === 1 ? patientCases[0].caseData.coId : null);

  if (patientCases.length === 0) {
    return (
      <div className="flex h-full items-center justify-center rounded-xl border border-border bg-card p-8 text-center shadow-epa">
        <div>
          <Database className="mx-auto h-10 w-10 text-muted-foreground/30" />
          <p className="mt-3 text-sm text-muted-foreground">Keine Fälle in tbCaseData gefunden.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in overflow-y-auto">
      {/* Patient header */}
      <div className="rounded-xl border border-border bg-card p-4 shadow-epa">
        <div className="flex items-center gap-3">
          <div className="flex h-12 w-12 items-center justify-center rounded-xl gradient-epa">
            <Database className="h-5 w-5 text-primary-foreground" />
          </div>
          <div>
            <h2 className="text-xl font-bold">{patient.lastname}, {patient.firstname}</h2>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
              <span>Patienten-ID: <span className="font-mono font-medium text-primary">{patient.patientId}</span></span>
              <span>{patient.age} Jahre · {patient.gender}</span>
              <span>ICD: <span className="font-mono font-medium text-primary">{patient.icd}</span></span>
              {(() => {
                const ward = patientCases[0]?.tables.icd10Data?.[0]?.coWard;
                return ward ? <span>Station: <span className="font-medium text-foreground">{ward}</span></span> : null;
              })()}
              <span>{patientCases.length} Fälle</span>
            </div>
          </div>
        </div>
      </div>

      {/* All cases, each fully expanded */}
      {patientCases.map(cr => {
        const cd = cr.caseData;
        const tables = getAvailableTables(cr);
        const completeness = getCompleteness(cr);

        return (
          <div key={cd.coId} className="rounded-xl border border-border bg-card shadow-epa overflow-hidden">
            {/* Case header */}
            <div
              className="border-b border-border bg-secondary/30 px-4 py-3 cursor-pointer transition-colors hover:bg-secondary/50"
              onClick={() => setExpandedCase(expandedCase === cd.coId ? null : cd.coId)}
            >
              <div className="flex items-center justify-between">
                <div>
                  <div className="flex items-center gap-2 flex-wrap">
                    <span className="text-base font-semibold">Fall {cd.coE2I222}</span>
                  </div>
                  <div className="mt-1 flex flex-wrap items-center gap-x-3 text-xs text-muted-foreground">
                    <span>{cd.coRecliningType} · {cd.coTypeOfStay}</span>
                    <span>DRG: <strong>{cd.coDrgName}</strong></span>
                    <span>Aufnahme: {cd.coE2I223?.split('T')[0]}</span>
                    {cd.coE2I228 && <span>Entlassung: {cd.coE2I228.split('T')[0]}</span>}
                  </div>
                </div>
                <div className="flex items-center gap-3 shrink-0">
                  <div className="flex items-center gap-1.5">
                    {tables.map(([key]) => {
                      const cfg = tableConfig[key];
                      if (!cfg) return null;
                      const Icon = cfg.icon;
                      return <Icon key={key} className={`h-4 w-4 ${cfg.color}`} title={cfg.label} />;
                    })}
                  </div>
                  <svg className={`h-4 w-4 text-muted-foreground transition-transform ${expandedCase === cd.coId ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" /></svg>
                </div>
              </div>
            </div>

            {/* Tables shown only when expanded */}
            {expandedCase === cd.coId && (
            <div className="divide-y divide-border">
              {tables.map(([key, data]) => {
                const cfg = tableConfig[key];
                if (!cfg) return null;
                const Icon = cfg.icon;
                const tl = tableLabels[key];

                return (
                  <div key={key}>
                    <div className="flex items-center gap-2 px-4 py-2 bg-secondary/10 border-b border-border/50">
                      <Icon className={`h-4 w-4 ${cfg.color}`} />
                      <span className="text-sm font-semibold">{tl?.short || cfg.label}</span>
                      <Badge variant="secondary" className="ml-auto text-[10px]">{(data as any[]).length} Einträge</Badge>
                    </div>
                    <div className="p-3">
                      {renderTableData(key, data as any[])}
                    </div>
                  </div>
                );
              })}
            </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

export default PatientDetailView;
