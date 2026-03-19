import { useState, useMemo } from "react";
import { X, CheckCircle2, Lightbulb, Clock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { type DataError, caseRecords } from "@/data/mockData";

interface Props {
  error: DataError;
  onClose: () => void;
  onCorrect: (errorId: string, value: string, comment: string, action: 'save' | 'na') => void;
}

const smartSuggestions: Record<string, string> = {
  'Sturzrisiko-Score': '3 (Mittel)',
  'Blutdruck': '130/85 mmHg',
  'HbA1c': '6.5%',
  'SpO2': '96%',
  'Barthel-Index': '55',
  'Pflegestufe': '2',
  'Temperatur': '36.8°C',
};

const CorrectionDialog = ({ error, onClose, onCorrect }: Props) => {
  const [value, setValue] = useState('');
  const suggestion = smartSuggestions[error.dataField];

  // Look up patient name from caseRecords
  const patientName = useMemo(() => {
    const pidNum = parseInt(error.patientId.replace('P-', ''), 10);
    for (const cr of caseRecords) {
      if (cr.caseData.coPatientId === pidNum) {
        return `${cr.caseData.coFirstname || ''} ${cr.caseData.coLastname || ''}`.trim();
      }
    }
    return '';
  }, [error.patientId]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/20 backdrop-blur-sm animate-fade-in" onClick={onClose}>
      <div className="w-full max-w-lg rounded-2xl border border-border bg-card p-6 shadow-epa-lg animate-fade-in" onClick={e => e.stopPropagation()}>
        <div className="flex items-start justify-between">
          <div>
            <h2 className="text-lg font-bold">Datenkorrektur</h2>
            <p className="mt-1 text-sm text-muted-foreground">Bitte geben Sie die fehlenden Informationen ein.</p>
          </div>
          <Button variant="ghost" size="icon" onClick={onClose}><X className="h-4 w-4" /></Button>
        </div>

        {/* Context */}
        <div className="mt-5 rounded-lg border border-border bg-secondary/50 p-4">
          <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Kontext</p>
          <div className="mt-2 space-y-1.5 text-sm">
            <p><span className="text-muted-foreground">Quelle:</span> <span className="font-medium">{error.sourceInstitution}</span></p>
            <p>
              <span className="text-muted-foreground">Patient:</span>{' '}
              <span className="font-mono font-medium text-primary">{error.patientId}</span>
              {patientName && <span className="ml-2 font-medium">{patientName}</span>}
            </p>
            <p><span className="text-muted-foreground">Fehlendes Feld:</span> <span className="font-medium">{error.dataField}</span></p>
          </div>
        </div>

        {/* Smart suggestion */}
        {suggestion && (
          <div className="mt-4 flex items-center gap-2 rounded-lg border border-primary/20 bg-primary/5 p-3">
            <Lightbulb className="h-4 w-4 text-primary" />
            <p className="text-sm"><span className="text-muted-foreground">Vorschlag:</span> <button className="font-medium text-primary underline-offset-2 hover:underline" onClick={() => setValue(suggestion)}>{suggestion}</button></p>
          </div>
        )}

        {/* Input */}
        <div className="mt-4">
          <label className="text-sm font-medium">Korrekturwert</label>
          <Input className="mt-1.5" value={value} onChange={e => setValue(e.target.value)} placeholder={`Wert für ${error.dataField} eingeben...`} />
        </div>

        {/* Audit trail */}
        {error.correctedBy && (
          <div className="mt-4 flex items-center gap-2 rounded-lg border border-border bg-secondary/50 p-3 text-xs text-muted-foreground">
            <Clock className="h-3.5 w-3.5" />
            Korrigiert von {error.correctedBy} am {error.correctedAt}
          </div>
        )}

        {/* Actions */}
        <div className="mt-6 flex items-center gap-3">
          <Button onClick={() => onCorrect(error.id, value, '', 'save')} disabled={!value} className="gradient-epa">
            <CheckCircle2 className="mr-2 h-4 w-4" /> Korrektur speichern
          </Button>
          <Button variant="ghost" onClick={onClose} className="ml-auto">Abbrechen</Button>
        </div>
      </div>
    </div>
  );
};

export default CorrectionDialog;
