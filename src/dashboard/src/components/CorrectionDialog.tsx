import { useState } from "react";
import { X, CheckCircle2, Info } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { type DataError, fieldLabels } from "@/data/mockData";
import { getFieldFormat } from "@/lib/fieldFormat";

// Case-insensitive lookup for column names coming from the API (which lowercases them)
const fieldLabelsLower = Object.fromEntries(
  Object.entries(fieldLabels).map(([k, v]) => [k.toLowerCase(), v])
);
function getFieldLabel(col: string): string {
  return fieldLabels[col] || fieldLabelsLower[col.toLowerCase()] || col;
}

interface Props {
  error: DataError;
  onClose: () => void;
  onCorrect: (errorId: string, value: string, comment: string, action: 'save' | 'na') => void;
}

const CorrectionDialog = ({ error, onClose, onCorrect }: Props) => {
  const column = error.columnName || '';
  const format = getFieldFormat(column);

  const [value, setValue] = useState('');
  const [validationError, setValidationError] = useState<string | null>(null);
  const [touched, setTouched] = useState(false);

  const handleChange = (v: string) => {
    setValue(v);
    if (touched) {
      setValidationError(v ? format.validate(v) : null);
    }
  };

  const handleBlur = () => {
    setTouched(true);
    if (value) setValidationError(format.validate(value));
  };

  const handleSubmit = () => {
    setTouched(true);
    const err = value ? format.validate(value) : null;
    setValidationError(err);
    if (!value || err) return;
    onCorrect(error.id, format.transform(value), '', 'save');
  };

  const isInvalid = touched && !!validationError;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-foreground/20 backdrop-blur-sm animate-fade-in"
      onClick={onClose}
    >
      <div
        className="w-full max-w-lg rounded-2xl border border-border bg-card p-6 shadow-epa-lg animate-fade-in"
        onClick={(e) => e.stopPropagation()}
      >
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
            <p><span className="text-muted-foreground">Tabelle:</span> <span className="font-mono font-medium">{error.tableName}</span></p>
            <p><span className="text-muted-foreground">Patient:</span> <span className="font-mono font-medium text-primary">{error.patientId}</span></p>
            {error.rowId != null && (
              <p><span className="text-muted-foreground">Zeile ID:</span> <span className="font-mono font-medium">{error.rowId}</span></p>
            )}
          </div>
        </div>

        {/* Field label */}
        <div className="mt-4">
          <p className="text-sm font-medium">
            Fehlendes Feld: <span className="text-primary font-semibold">{getFieldLabel(column)}</span>
          </p>
        </div>

        {/* Input */}
        <div className="mt-4">
          <label className="text-sm font-medium">Korrekturwert</label>

          {format.hint && (
            <div className="mt-1 flex items-center gap-1.5 text-[11px] text-muted-foreground">
              <Info className="h-3 w-3 shrink-0" />
              <span>{format.hint}</span>
            </div>
          )}

          <Input
            className={`mt-1.5 transition-colors ${isInvalid ? 'border-epa-danger ring-1 ring-epa-danger/50 focus-visible:ring-epa-danger' : ''}`}
            value={value}
            onChange={(e) => handleChange(e.target.value)}
            onBlur={handleBlur}
            placeholder={format.placeholder}
            onKeyDown={(e) => {
              if (e.key === 'Enter') handleSubmit();
            }}
          />

          {isInvalid && (
            <p className="mt-1.5 text-xs text-epa-danger flex items-center gap-1">
              <span className="font-medium">⚠</span> {validationError}
            </p>
          )}
        </div>

        {/* Actions */}
        <div className="mt-6 flex items-center gap-3">
          <Button
            onClick={handleSubmit}
            disabled={!value}
            className="gradient-epa"
          >
            <CheckCircle2 className="mr-2 h-4 w-4" /> Korrektur speichern
          </Button>
          <Button variant="ghost" onClick={onClose} className="ml-auto">Abbrechen</Button>
        </div>
      </div>
    </div>
  );
};

export default CorrectionDialog;
