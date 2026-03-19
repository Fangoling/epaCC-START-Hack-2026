import { ArrowRight, CheckCircle2, AlertTriangle } from "lucide-react";
import { mappingRules } from "@/data/mockData";

const MappingVisualizer = () => {
  return (
    <div className="rounded-xl border border-border bg-card p-5 shadow-epa animate-fade-in">
      <h3 className="mb-4 text-lg font-semibold">Automatische Mapping-Regeln</h3>
      <div className="space-y-3">
        {mappingRules.map((rule, i) => (
          <div key={i} className="flex items-center gap-3 rounded-lg border border-border bg-secondary/30 p-3">
            <div className="rounded-lg border border-border bg-card px-3 py-2 text-sm font-medium shadow-sm">{rule.input}</div>
            <ArrowRight className="h-4 w-4 text-muted-foreground" />
            <div className="rounded-lg border border-primary/20 bg-primary/5 px-3 py-2 text-sm font-medium text-primary">{rule.rule}</div>
            <ArrowRight className="h-4 w-4 text-muted-foreground" />
            <div className="rounded-lg border border-border bg-card px-3 py-2 text-sm font-medium shadow-sm">{rule.target}</div>
            <div className="ml-auto">
              {rule.status === 'active' ? (
                <CheckCircle2 className="h-5 w-5 text-epa-success" />
              ) : (
                <AlertTriangle className="h-5 w-5 text-epa-warning" />
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default MappingVisualizer;
