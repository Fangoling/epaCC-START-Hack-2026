import { useState, useMemo } from "react";
import { Search, User, ChevronRight, AlertCircle, ArrowUpDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { type PatientSummary, type DataError } from "@/data/mockData";

interface Props {
  patients: PatientSummary[];
  errors: DataError[];
  selectedPatient: PatientSummary | null;
  onSelectPatient: (patient: PatientSummary) => void;
}

const PatientList = ({ patients, errors, selectedPatient, onSelectPatient }: Props) => {
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<string>("name");

  // Count missing-data errors per patient from the API-provided errors list
  const errorCountByPatient = useMemo(() => {
    const map = new Map<number, number>();
    errors.forEach((e) => {
      if (e.status === "corrected") return;
      const pidNum = parseInt(e.patientId.replace("P-", ""), 10);
      if (!isNaN(pidNum)) map.set(pidNum, (map.get(pidNum) || 0) + 1);
    });
    return map;
  }, [errors]);

  const filtered = useMemo(() => {
    let list = patients.filter(
      (p) =>
        p.displayId.toLowerCase().includes(search.toLowerCase()) ||
        `${p.lastname}, ${p.firstname}`.toLowerCase().includes(search.toLowerCase()) ||
        p.icd.toLowerCase().includes(search.toLowerCase())
    );

    if (sort === "issues") {
      list = [...list].sort(
        (a, b) => (errorCountByPatient.get(b.patientId) || 0) - (errorCountByPatient.get(a.patientId) || 0)
      );
    }

    return list;
  }, [search, patients, sort, errorCountByPatient]);

  return (
    <div className="flex h-full flex-col rounded-xl border border-border bg-card shadow-epa">
      <div className="border-b border-border p-3">
        <div className="flex items-center justify-between mb-2">
          <h2 className="text-sm font-semibold">Patienten</h2>
          <span className="text-[10px] text-muted-foreground">{filtered.length} Einträge</span>
        </div>
        <div className="relative mb-2">
          <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Suchen..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 pl-8 text-sm"
          />
        </div>
        <Select value={sort} onValueChange={setSort}>
          <SelectTrigger className="h-7 text-[11px]">
            <ArrowUpDown className="h-3 w-3 mr-1" />
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="name">Name (A–Z)</SelectItem>
            <SelectItem value="issues">Meiste Fehler zuerst</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div className="flex-1 overflow-y-auto">
        {filtered.map((patient) => {
          const isSelected = selectedPatient?.patientId === patient.patientId;
          const errCount = errorCountByPatient.get(patient.patientId) || 0;

          return (
            <button
              key={patient.patientId}
              onClick={() => onSelectPatient(patient)}
              className={`flex w-full items-center gap-3 border-b border-border/50 px-3 py-2.5 text-left transition-colors hover:bg-secondary/50 ${
                isSelected ? "bg-primary/5 border-l-2 border-l-primary" : ""
              }`}
            >
              <div
                className={`relative flex h-8 w-8 shrink-0 items-center justify-center rounded-full ${
                  isSelected ? "gradient-epa" : "bg-secondary"
                }`}
              >
                <User
                  className={`h-3.5 w-3.5 ${isSelected ? "text-primary-foreground" : "text-muted-foreground"}`}
                />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium">
                  {patient.lastname}, {patient.firstname}
                </p>
                <div className="flex items-center gap-2 mt-0.5">
                  <span className="font-mono text-[11px] text-muted-foreground">{patient.displayId}</span>
                  {errCount > 0 && (
                    <span
                      className="inline-flex items-center gap-0.5 rounded-full bg-epa-warning/10 px-1.5 py-0.5 text-[10px] font-medium text-epa-warning"
                      title={`${errCount} fehlende Daten`}
                    >
                      <AlertCircle className="h-3 w-3" />
                      {errCount} Fehlend
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
