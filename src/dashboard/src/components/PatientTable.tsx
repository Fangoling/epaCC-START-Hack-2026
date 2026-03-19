import { useState, useMemo } from "react";
import { Search, ArrowUpDown } from "lucide-react";
import { Input } from "@/components/ui/input";
import { getPatientSummaries, type PatientSummary } from "@/data/mockData";

interface PatientTableProps {
  selectedPatient: PatientSummary | null;
  onSelectPatient: (patient: PatientSummary) => void;
}

const PatientTable = ({ selectedPatient, onSelectPatient }: PatientTableProps) => {
  const [search, setSearch] = useState("");
  const [sortField, setSortField] = useState<keyof PatientSummary>("latestActivity");
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const patients = useMemo(() => getPatientSummaries(), []);

  const filtered = useMemo(() => {
    let result = patients.filter(p =>
      p.displayId.toLowerCase().includes(search.toLowerCase()) ||
      `${p.lastname}, ${p.firstname}`.toLowerCase().includes(search.toLowerCase())
    );
    result.sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      const cmp = typeof aVal === 'number' ? (aVal as number) - (bVal as number) : String(aVal).localeCompare(String(bVal));
      return sortDir === 'asc' ? cmp : -cmp;
    });
    return result;
  }, [search, sortField, sortDir, patients]);

  const toggleSort = (field: keyof PatientSummary) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortDir('asc'); }
  };

  const SortHeader = ({ field, label }: { field: keyof PatientSummary; label: string }) => (
    <th className="cursor-pointer px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-muted-foreground hover:text-foreground" onClick={() => toggleSort(field)}>
      <span className="flex items-center gap-1">{label}<ArrowUpDown className="h-3 w-3" /></span>
    </th>
  );

  return (
    <div className="rounded-xl border border-border bg-card shadow-epa animate-fade-in">
      <div className="border-b border-border p-4">
        <h2 className="mb-3 text-lg font-semibold">Patientenübersicht</h2>
        <div className="relative">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input placeholder="Patient suchen (ID oder Name)..." value={search} onChange={(e) => setSearch(e.target.value)} className="pl-10" />
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="border-b border-border bg-secondary/50">
            <tr>
              <SortHeader field="displayId" label="Patienten-ID" />
              <SortHeader field="lastname" label="Name" />
              <SortHeader field="age" label="Alter" />
              <SortHeader field="icd" label="ICD-10" />
              <SortHeader field="state" label="Status" />
              <SortHeader field="caseCount" label="Fälle" />
              <SortHeader field="latestActivity" label="Aufnahme" />
              <SortHeader field="dischargeDate" label="Entlassung" />
              <SortHeader field="lengthOfStay" label="Verweildauer" />
            </tr>
          </thead>
          <tbody>
            {filtered.map(patient => (
              <tr
                key={patient.patientId}
                onClick={() => onSelectPatient(patient)}
                className={`cursor-pointer border-b border-border transition-colors hover:bg-secondary/50 ${selectedPatient?.patientId === patient.patientId ? 'bg-primary/5 border-l-2 border-l-primary' : ''}`}
              >
                <td className="px-4 py-3 text-sm font-mono font-medium text-primary">{patient.displayId}</td>
                <td className="px-4 py-3 text-sm font-medium">{patient.lastname}, {patient.firstname}</td>
                <td className="px-4 py-3 text-sm text-muted-foreground">{patient.age}</td>
                <td className="px-4 py-3 text-sm font-mono text-muted-foreground">{patient.icd}</td>
                <td className="px-4 py-3 text-sm">
                  <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${patient.state === 'aktiv' ? 'bg-epa-success/10 text-epa-success' : 'bg-muted text-muted-foreground'}`}>
                    {patient.state}
                  </span>
                </td>
                <td className="px-4 py-3 text-sm text-center text-muted-foreground">{patient.caseCount}</td>
                <td className="px-4 py-3 text-sm text-muted-foreground">{patient.latestActivity?.split('T')[0]}</td>
                <td className="px-4 py-3 text-sm text-muted-foreground">{patient.dischargeDate?.split('T')[0] || '–'}</td>
                <td className="px-4 py-3 text-sm text-muted-foreground">{patient.lengthOfStay != null ? `${patient.lengthOfStay} Tage` : '–'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default PatientTable;
