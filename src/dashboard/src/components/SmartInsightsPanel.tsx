import { TrendingUp, AlertTriangle, Users } from "lucide-react";
import { similarCases, type PatientSummary } from "@/data/mockData";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from "recharts";

const trendData = [
  { group: '60-69', score: 4.2 },
  { group: '70-79', score: 3.5 },
  { group: '80-89', score: 2.1 },
  { group: '90+', score: 1.4 },
];

const riskData = [
  { name: 'Niedrig', value: 45, color: 'hsl(152, 60%, 42%)' },
  { name: 'Mittel', value: 35, color: 'hsl(38, 92%, 50%)' },
  { name: 'Hoch', value: 20, color: 'hsl(0, 72%, 51%)' },
];

interface Props {
  patient: PatientSummary;
  onSelectPatient: (id: number) => void;
}

const SmartInsightsPanel = ({ patient, onSelectPatient }: Props) => {
  return (
    <div className="space-y-4 animate-slide-in">
      {/* Similar Cases */}
      <div className="rounded-xl border border-border bg-card shadow-epa">
        <div className="border-b border-border p-4">
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <Users className="h-4 w-4 text-primary" /> Ähnliche Fälle
          </h3>
        </div>
        <div className="divide-y divide-border">
          {similarCases.filter(c => c.patientId !== patient.patientId).slice(0, 5).map(c => (
            <button key={c.patientId} onClick={() => onSelectPatient(c.patientId)} className="flex w-full items-center gap-3 px-4 py-2.5 text-left transition-colors hover:bg-secondary/50">
              <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 text-xs font-bold text-primary">{c.similarity}%</div>
              <div className="flex-1">
                <p className="text-sm font-medium">{c.name}</p>
                <p className="text-xs text-muted-foreground">{c.age}J · {c.mainDiagnosis}</p>
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* Trends */}
      <div className="rounded-xl border border-border bg-card shadow-epa">
        <div className="border-b border-border p-4">
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <TrendingUp className="h-4 w-4 text-primary" /> Ø Mobilitätsscore nach Altersgruppe
          </h3>
        </div>
        <div className="p-4">
          <ResponsiveContainer width="100%" height={140}>
            <BarChart data={trendData}>
              <XAxis dataKey="group" tick={{ fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11 }} axisLine={false} tickLine={false} domain={[0, 5]} />
              <Tooltip />
              <Bar dataKey="score" fill="hsl(252, 62%, 50%)" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Risk Distribution */}
      <div className="rounded-xl border border-border bg-card shadow-epa">
        <div className="border-b border-border p-4">
          <h3 className="flex items-center gap-2 text-sm font-semibold">
            <AlertTriangle className="h-4 w-4 text-epa-warning" /> Sturzrisiko-Verteilung
          </h3>
        </div>
        <div className="flex items-center gap-4 p-4">
          <ResponsiveContainer width={100} height={100}>
            <PieChart>
              <Pie data={riskData} dataKey="value" cx="50%" cy="50%" innerRadius={25} outerRadius={45} strokeWidth={0}>
                {riskData.map((entry, i) => <Cell key={i} fill={entry.color} />)}
              </Pie>
            </PieChart>
          </ResponsiveContainer>
          <div className="space-y-1.5">
            {riskData.map(r => (
              <div key={r.name} className="flex items-center gap-2 text-xs">
                <div className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: r.color }} />
                <span className="text-muted-foreground">{r.name}: {r.value}%</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Anomaly for current patient */}
      {patient.age > 75 && (
        <div className="rounded-xl border border-epa-warning/40 bg-epa-warning/5 p-4 shadow-epa animate-fade-in">
          <div className="flex items-start gap-2">
            <AlertTriangle className="mt-0.5 h-4 w-4 text-epa-warning" />
            <div>
              <p className="text-sm font-semibold text-epa-warning">Anomalie erkannt</p>
              <p className="mt-1 text-xs text-muted-foreground">Drastische Veränderung der Vitalzeichen im Vergleich zur Vorwoche. Bitte überprüfen.</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default SmartInsightsPanel;
