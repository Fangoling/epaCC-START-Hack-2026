import { useState, useMemo } from "react";
import { Search, User, ChevronRight, AlertTriangle, AlertCircle, ArrowUpDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { getPatientSummaries, caseRecords, dataErrors, type PatientSummary } from "@/data/mockData";

// Clinical thresholds (same as PatientDetailView)
const labRanges: Record<string, { low: number; high: number }> = {
  coGlucose_mg_dL: { low: 70, high: 110 },
  coCreatinine_mg_dL: { low: 0.6, high: 1.2 },
  coEgfr_mL_min_1_73m2: { low: 90, high: 999 },
  coCrp_mg_L: { low: 0, high: 5 },
  coLactate_mmol_L: { low: 0.5, high: 2.2 },
};

export const getPatientIssues = (patientId: number) => {
  let anomalies = 0;
  let errors = 0;

  errors = dataErrors.filter(e => e.patientId === `P-${patientId}` && e.status !== 'corrected').length;

  caseRecords.filter(cr => cr.caseData.coPatientId === patientId).forEach(cr => {
    cr.tables.labsData?.forEach(lab => {
      Object.entries(labRanges).forEach(([key, range]) => {
        const val = (lab as any)[key];
        if (val !== null && val !== undefined) {
          const num = Number(val);
          if (!isNaN(num) && (num < range.low || num > range.high)) anomalies++;
        }
      });
    });
    cr.tables.deviceMotion?.forEach(dm => {
      if (dm.coFall_event_0_1 === '1') anomalies++;
    });
    cr.tables.acData?.forEach(ac => {
      if (ac.coE0I005 != null && Number(ac.coE0I005) <= 18) anomalies++;
      if (ac.coE0I070 != null && Number(ac.coE0I070) <= 60) anomalies++;
      if (ac.coE0I007 != null && Number(ac.coE0I007) >= 3) anomalies++;
    });
  });

  return { anomalies, errors, total: anomalies + errors };
};

interface Props {
  selectedPatient: PatientSummary | null;
  onSelectPatient: (patient: PatientSummary) => void;
}

const PatientList = ({ selectedPatient, onSelectPatient }: Props) => {
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<string>("name");
  const patients = useMemo(() => getPatientSummaries(), []);

  const patientIssues = useMemo(() => {
    const map = new Map<number, { anomalies: number; errors: number; total: number }>();
    patients.forEach(p => map.set(p.patientId, getPatientIssues(p.patientId)));
    return map;
  }, [patients]);

  const filtered = useMemo(() => {
    let list = patients.filter(p =>
      p.displayId.toLowerCase().includes(search.toLowerCase()) ||
      `${p.lastname}, ${p.firstname}`.toLowerCase().includes(search.toLowerCase()) ||
      p.icd.toLowerCase().includes(search.toLowerCase())
    );

    if (sort === 'issues') {
      list = [...list].sort((a, b) => {
        const ia = patientIssues.get(a.patientId)?.total || 0;
        const ib = patientIssues.get(b.patientId)?.total || 0;
        return ib - ia;
      });
    } else if (sort === 'anomalies') {
      list = [...list].sort((a, b) => {
        const ia = patientIssues.get(a.patientId)?.anomalies || 0;
        const ib = patientIssues.get(b.patientId)?.anomalies || 0;
        return ib - ia;
      });
    }

    return list;
  }, [search, patients, sort, patientIssues]);

  return (
    <div className="flex h-full flex-col rounded-xl border border-border bg-card shadow-epa">
      <div className="border-b border-border p-3">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-sm font-semibold">Patienten</h2>
          <span className="text-[10px] text-muted-foreground">{filtered.length} Einträge</span>
        </div>
        <div className="relative mb-2">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input placeholder="Suchen..." value={search} onChange={(e) => setSearch(e.target.value)} className="h-8 pl-8 text-sm" />
        </div>
        <Select value={sort} onValueChange={setSort}>
          <SelectTrigger className="h-7 text-[11px]">
            <ArrowUpDown className="h-3 w-3 mr-1" />
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="name">Name (A–Z)</SelectItem>
            <SelectItem value="issues">Meiste Issues zuerst</SelectItem>
            <SelectItem value="anomalies">Meiste Anomalien zuerst</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="flex-1 overflow-y-auto">
        {filtered.map(patient => {
          const isSelected = selectedPatient?.patientId === patient.patientId;
          const issues = patientIssues.get(patient.patientId);
          const hasAnomalies = issues && issues.anomalies > 0;
          const hasErrors = issues && issues.errors > 0;

          return (
            <button
              key={patient.patientId}
              onClick={() => onSelectPatient(patient)}
              className={`flex w-full items-center gap-3 border-b border-border/50 px-3 py-2.5 text-left transition-colors hover:bg-secondary/50 ${isSelected ? 'bg-primary/5 border-l-2 border-l-primary' : ''}`}
            >
              <div className={`relative flex h-8 w-8 shrink-0 items-center justify-center rounded-full ${isSelected ? 'gradient-epa' : 'bg-secondary'}`}>
                <User className={`h-3.5 w-3.5 ${isSelected ? 'text-primary-foreground' : 'text-muted-foreground'}`} />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium">{patient.lastname}, {patient.firstname}</p>
                <div className="flex items-center gap-2 mt-0.5">
                  <span className="font-mono text-[11px] text-muted-foreground">{patient.displayId}</span>
                  {hasAnomalies && (
                    <span className="inline-flex items-center gap-0.5 rounded-full bg-epa-danger/10 px-1.5 py-0.5 text-[10px] font-medium text-epa-danger" title={`${issues.anomalies} Anomalien erkannt`}>
                      <AlertTriangle className="h-3 w-3" />{issues.anomalies} Anomalien
                    </span>
                  )}
                  {hasErrors && (
                    <span className="inline-flex items-center gap-0.5 rounded-full bg-epa-warning/10 px-1.5 py-0.5 text-[10px] font-medium text-epa-warning" title={`${issues.errors} fehlende Daten`}>
                      <AlertCircle className="h-3 w-3" />{issues.errors} Fehlend
                    </span>
                  )}
                </div>
              </div>
              {isSelected && <ChevronRight className="h-3.5 w-3.5 shrink-0 text-primary" />}
            </button>
          );
        })}
      </div>
    </div>
  );
};

export default PatientList;
