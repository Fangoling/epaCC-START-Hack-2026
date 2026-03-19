import { useMemo } from "react";
import { Activity, AlertTriangle, AlertCircle, Users, FlaskConical, FileText } from "lucide-react";
import { getPatientSummaries, caseRecords } from "@/data/mockData";
import { getPatientIssues } from "./PatientList";

const ExplorerEmptyState = () => {
  const patients = useMemo(() => getPatientSummaries(), []);

  const stats = useMemo(() => {
    let totalAnomalies = 0;
    let totalErrors = 0;
    let totalRecords = 0;
    let totalCases = caseRecords.length;

    patients.forEach(p => {
      const issues = getPatientIssues(p.patientId);
      totalAnomalies += issues.anomalies;
      totalErrors += issues.errors;
    });

    caseRecords.forEach(cr => {
      Object.values(cr.tables).forEach(data => {
        if (data) totalRecords += (data as any[]).length;
      });
    });

    return { totalAnomalies, totalErrors, totalRecords, totalCases, totalPatients: patients.length };
  }, [patients]);

  const cards = [
    { label: 'Patienten', value: stats.totalPatients, icon: Users, color: 'text-primary', bg: 'bg-primary/10' },
    { label: 'Fälle', value: stats.totalCases, icon: FileText, color: 'text-epa-info', bg: 'bg-epa-info/10' },
    { label: 'Datensätze', value: stats.totalRecords, icon: FlaskConical, color: 'text-epa-success', bg: 'bg-epa-success/10' },
    { label: 'Anomalien', value: stats.totalAnomalies, icon: AlertTriangle, color: 'text-epa-danger', bg: 'bg-epa-danger/10' },
    { label: 'Fehlende Daten', value: stats.totalErrors, icon: AlertCircle, color: 'text-epa-warning', bg: 'bg-epa-warning/10' },
  ];

  return (
    <div className="flex h-full flex-col items-center justify-center rounded-xl border border-border bg-card shadow-epa p-8">
      <Activity className="h-10 w-10 text-primary/20 mb-4" />
      <h3 className="text-lg font-semibold mb-1">Smart Health Explorer</h3>
      <p className="text-sm text-muted-foreground mb-6">Wählen Sie einen Patienten aus der Liste, um alle Falldaten zu sehen.</p>

      <div className="grid grid-cols-2 gap-3 w-full max-w-md sm:grid-cols-3">
        {cards.map(c => {
          const Icon = c.icon;
          return (
            <div key={c.label} className="rounded-lg border border-border bg-background p-3 text-center">
              <div className={`mx-auto mb-1.5 flex h-8 w-8 items-center justify-center rounded-lg ${c.bg}`}>
                <Icon className={`h-4 w-4 ${c.color}`} />
              </div>
              <p className="text-lg font-bold">{c.value}</p>
              <p className="text-[10px] text-muted-foreground">{c.label}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default ExplorerEmptyState;
