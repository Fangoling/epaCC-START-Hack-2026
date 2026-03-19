import { useState, useMemo } from "react";
import { AlertTriangle, CheckCircle2, Search, ShieldCheck, FileText, BarChart3, Activity, FlaskConical, Stethoscope, Smartphone, HeartPulse, Pill, CircleSlash, ChevronRight } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { dataErrors, dataQualityMetrics, caseRecords, tableLabels, type DataError } from "@/data/mockData";
import CorrectionDialog from "./CorrectionDialog";
import { toast } from "sonner";

// ── Data source stats ──

const getSourceStats = () => {
  const counts: Record<string, number> = {};
  caseRecords.forEach(cr => {
    Object.entries(cr.tables).forEach(([key, data]) => {
      if (data && (data as any[]).length > 0) counts[key] = (counts[key] || 0) + (data as any[]).length;
    });
  });
  const sourceConfig: Record<string, { bg: string; icon: React.ElementType; color: string }> = {
    acData: { bg: 'bg-epa-blue', icon: Activity, color: 'text-epa-blue' },
    labsData: { bg: 'bg-epa-success', icon: FlaskConical, color: 'text-epa-success' },
    icd10Data: { bg: 'bg-epa-purple', icon: Stethoscope, color: 'text-epa-purple' },
    deviceMotion: { bg: 'bg-epa-info', icon: Smartphone, color: 'text-epa-info' },
    device1HzMotion: { bg: 'bg-epa-danger', icon: HeartPulse, color: 'text-epa-danger' },
    medication: { bg: 'bg-epa-warning', icon: Pill, color: 'text-epa-warning' },
    nursingReports: { bg: 'bg-muted-foreground', icon: FileText, color: 'text-muted-foreground' },
  };
  const total = Object.values(counts).reduce((s, c) => s + c, 0);
  return { counts, total, sourceConfig };
};

// ── Quality bars data ──

const qualityBySource: Record<string, { clean: number; incorrect: number; missing: number }> = {
  icd10Data: { clean: 85, incorrect: 5, missing: 10 },
  acData: { clean: 78, incorrect: 7, missing: 15 },
  labsData: { clean: 88, incorrect: 4, missing: 8 },
  medication: { clean: 82, incorrect: 8, missing: 10 },
  nursingReports: { clean: 65, incorrect: 10, missing: 25 },
  deviceMotion: { clean: 72, incorrect: 6, missing: 22 },
  device1HzMotion: { clean: 70, incorrect: 8, missing: 22 },
};

// (error types and severity removed — all errors are "missing data")

// ── Component ──

const DataQualityDashboard = () => {
  const [search, setSearch] = useState('');
  const [sourceFilter, setSourceFilter] = useState<string>('all');
  
  const [selectedError, setSelectedError] = useState<DataError | null>(null);
  const [correctedIds, setCorrectedIds] = useState<Set<string>>(new Set());

  // Patient name lookup
  const patientNames = useMemo(() => {
    const map: Record<string, string> = {};
    caseRecords.forEach(cr => {
      const pid = `P-${cr.caseData.coPatientId}`;
      if (!map[pid]) map[pid] = `${cr.caseData.coFirstname || ''} ${cr.caseData.coLastname || ''}`.trim();
    });
    return map;
  }, []);

  // Reverse lookup: tblImportAcData -> short label
  const tableNameToShort = useMemo(() => {
    const map: Record<string, string> = {};
    Object.entries(tableLabels).forEach(([key, val]) => {
      map[val.full] = val.short;
    });
    return map;
  }, []);

  const errorIssues = dataErrors.map(e => ({
    id: e.id,
    patientId: e.patientId,
    patientName: patientNames[e.patientId] || '',
    source: tableNameToShort[e.tableName] || e.sourceInstitution,
    station: e.sourceInstitution,
    field: e.dataField,
    description: e.errorDescription,
    errorType: e.errorType || 'missing',
    severity: e.priority === 'high' ? 'high' : e.priority === 'medium' ? 'medium' : 'low',
    status: correctedIds.has(e.id) ? 'resolved' : e.status === 'corrected' ? 'resolved' : 'open',
    originalError: e,
  }));

  const filtered = errorIssues
    .filter(i => {
      if (i.status === 'resolved') return false;
      if (sourceFilter !== 'all' && i.source !== sourceFilter) return false;
      if (search && !i.patientName.toLowerCase().includes(search.toLowerCase()) && !i.field.toLowerCase().includes(search.toLowerCase()) && !i.description.toLowerCase().includes(search.toLowerCase())) return false;
      return true;
    });

  const handleCorrection = (errorId: string) => {
    setCorrectedIds(prev => new Set(prev).add(errorId));
    setSelectedError(null);
    toast.success('Korrektur gespeichert');
  };

  const m = dataQualityMetrics;
  const { counts, total, sourceConfig } = getSourceStats();
  const openIssues = errorIssues.filter(i => i.status === 'open').length;

  const allSources = useMemo(() => {
    const labels = Object.values(tableLabels).map(l => l.short).sort();
    return labels;
  }, []);

  return (
    <div className="space-y-5 animate-fade-in">
      {/* KPI Row */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <div className="rounded-xl border border-border bg-card p-4 shadow-epa">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-epa-info/10"><FileText className="h-4 w-4 text-epa-info" /></div>
            <div><p className="text-xl font-bold">{total}</p><p className="text-[11px] text-muted-foreground">Datensätze</p></div>
          </div>
        </div>
        <div className="rounded-xl border border-border bg-card p-4 shadow-epa">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-epa-success/10"><ShieldCheck className="h-4 w-4 text-epa-success" /></div>
            <div><p className="text-xl font-bold">{m.overallCompleteness}%</p><p className="text-[11px] text-muted-foreground">Vollständigkeit</p></div>
          </div>
        </div>
        <div className="rounded-xl border border-border bg-card p-4 shadow-epa">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-epa-danger/10"><AlertTriangle className="h-4 w-4 text-epa-danger" /></div>
            <div><p className="text-xl font-bold">{openIssues}</p><p className="text-[11px] text-muted-foreground">Offene Fehler</p></div>
          </div>
        </div>
        <div className="rounded-xl border border-border bg-card p-4 shadow-epa">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-primary/10"><BarChart3 className="h-4 w-4 text-primary" /></div>
            <div><p className="text-xl font-bold">{m.mappingSuccessRate}%</p><p className="text-[11px] text-muted-foreground">Genauigkeit</p></div>
          </div>
        </div>
      </div>

      {/* Combined Data Sources + Quality */}
      <div className="rounded-xl border border-border bg-card p-4 shadow-epa">
        <div className="grid grid-cols-2 gap-6 mb-4">
          <div className="flex items-center gap-3">
            <h3 className="text-sm font-semibold">Datenquellen</h3>
            <span className="text-xs text-muted-foreground">{total} Datensätze</span>
          </div>
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold">Qualität</h3>
            <div className="flex items-center gap-3 text-[11px] text-muted-foreground">
              <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-epa-success" />Korrekt</span>
              <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-epa-warning" />Fehlerhaft</span>
              <span className="flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-epa-danger" />Fehlend</span>
            </div>
          </div>
        </div>
        <div className="space-y-3">
          {Object.entries(counts)
            .sort(([, a], [, b]) => b - a)
            .map(([key, count]) => {
              const pct = Math.round((count / total) * 100);
              const q = qualityBySource[key];
              const cfg = sourceConfig[key];
              if (!cfg) return null;
              const Icon = cfg.icon;
              const qTotal = q ? q.clean + q.incorrect + q.missing : 0;

              return (
                <div key={key} className="group grid grid-cols-2 gap-6 items-center">
                  {/* Left: source volume */}
                  <div>
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex items-center gap-2 min-w-0">
                        <Icon className={`h-4 w-4 shrink-0 ${cfg.color}`} />
                        <span className="text-xs font-medium truncate">{tableLabels[key]?.short || key}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-bold tabular-nums">{count}</span>
                        <span className="text-[10px] text-muted-foreground w-8 text-right">{pct}%</span>
                      </div>
                    </div>
                    <div className="h-2 w-full rounded-full bg-secondary overflow-hidden">
                      <div
                        className={`h-full rounded-full ${cfg.bg} transition-all duration-500 group-hover:opacity-80`}
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                  </div>

                  {/* Right: quality breakdown */}
                  <div>
                    {q ? (
                      <>
                        <div className="flex items-center justify-end mb-1 gap-2 text-[10px] tabular-nums text-muted-foreground">
                          <span className="text-epa-success font-medium">{q.clean}%</span>
                          <span className="text-epa-warning">{q.incorrect}%</span>
                          <span className="text-epa-danger">{q.missing}%</span>
                        </div>
                        <div className="h-2 w-full rounded-full bg-secondary overflow-hidden flex">
                          <div className="h-full bg-epa-success transition-all duration-500" style={{ width: `${(q.clean / qTotal) * 100}%` }} />
                          <div className="h-full bg-epa-warning transition-all duration-500" style={{ width: `${(q.incorrect / qTotal) * 100}%` }} />
                          <div className="h-full bg-epa-danger rounded-r-full transition-all duration-500" style={{ width: `${(q.missing / qTotal) * 100}%` }} />
                        </div>
                      </>
                    ) : (
                      <div className="h-2 w-full rounded-full bg-secondary" />
                    )}
                  </div>
                </div>
              );
            })}
        </div>
      </div>

      {/* ═══════════ DATENFEHLER — Premium Redesign ═══════════ */}
      <div className="rounded-xl border border-border bg-card shadow-epa overflow-hidden">
        {/* Header */}
        <div className="border-b border-border px-5 py-4">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-epa-danger/10">
                <AlertTriangle className="h-4 w-4 text-epa-danger" />
              </div>
              <div>
                <h3 className="text-sm font-bold">Datenfehler</h3>
                <p className="text-[11px] text-muted-foreground">{openIssues} offene Probleme erfordern Aufmerksamkeit</p>
              </div>
            </div>
          </div>


          {/* Search + filter */}
          <div className="flex flex-wrap items-center gap-3">
            <div className="relative flex-1 min-w-[180px]">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input placeholder="Patient oder Feld suchen..." value={search} onChange={e => setSearch(e.target.value)} className="pl-10 h-9" />
            </div>
            <Select value={sourceFilter} onValueChange={setSourceFilter}>
              <SelectTrigger className="w-[200px] h-9"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Alle Datenquellen</SelectItem>
                {allSources.map(s => (
                  <SelectItem key={s} value={s}>{s}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* Error Cards */}
        <div className="max-h-[480px] overflow-y-auto">
          {filtered.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <CheckCircle2 className="h-10 w-10 mb-3 text-epa-success/50" />
              <p className="text-sm font-medium">Keine offenen Fehler</p>
              <p className="text-xs">Alle Daten sind korrekt</p>
            </div>
          ) : (
            <div className="divide-y divide-border">
              {filtered.map(issue => (
                  <div
                    key={issue.id}
                    className="group flex items-center gap-4 px-5 py-3.5 transition-all hover:bg-secondary/40 cursor-pointer"
                    onClick={() => setSelectedError(issue.originalError)}
                  >
                    {/* Icon */}
                    <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border bg-epa-warning/10 border-epa-warning/20">
                      <CircleSlash className="h-4 w-4 text-epa-warning" />
                    </div>

                    {/* Content */}
                    <div className="min-w-0 flex-1">
                      <span className="text-sm font-semibold truncate block">{issue.field}</span>
                      <p className="text-xs text-muted-foreground truncate">{issue.description}</p>
                      <div className="flex items-center gap-3 mt-1">
                        <span className="text-[11px] text-muted-foreground/80">{issue.patientName || issue.patientId}</span>
                        <span className="text-[10px] text-muted-foreground/50">·</span>
                        <span className="text-[11px] text-muted-foreground/60 truncate">{issue.station}</span>
                      </div>
                    </div>

                    <ChevronRight className="h-4 w-4 text-muted-foreground/40 group-hover:text-foreground/60 transition-colors shrink-0" />
                  </div>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="border-t border-border px-5 py-2.5 flex items-center justify-between">
          <span className="text-[11px] text-muted-foreground">{filtered.length} von {openIssues} angezeigt</span>
          <span className="text-[11px] text-muted-foreground/60">Klicken zum Korrigieren</span>
        </div>
      </div>

      {selectedError && (
        <CorrectionDialog
          error={selectedError}
          onClose={() => setSelectedError(null)}
          onCorrect={(errorId) => handleCorrection(errorId)}
        />
      )}
    </div>
  );
};

export default DataQualityDashboard;