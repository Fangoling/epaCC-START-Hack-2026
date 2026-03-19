import { useState } from "react";
import { X, CheckCircle2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { type DataError, fieldLabels } from "@/data/mockData";

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
  const allColumns = error.allMissingColumns || [error.columnName];
  const [selectedColumn, setSelectedColumn] = useState(error.columnName || allColumns[0] || '');
  const [value, setValue] = useState('');

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

        {/* Column selector (if multiple missing columns) */}
        {allColumns.length > 1 && (
          <div className="mt-4">
            <label className="text-sm font-medium">Zu korrigierendes Feld</label>
            <Select value={selectedColumn} onValueChange={setSelectedColumn}>
              <SelectTrigger className="mt-1.5 h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {allColumns.map((col) => (
                  <SelectItem key={col} value={col}>
                    {getFieldLabel(col)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        {/* If only one column, show it as info */}
        {allColumns.length === 1 && (
          <div className="mt-4">
            <p className="text-sm font-medium">
              Fehlendes Feld: <span className="text-primary font-semibold">{getFieldLabel(selectedColumn)}</span>
            </p>
          </div>
        )}

        {/* Input */}
        <div className="mt-4">
          <label className="text-sm font-medium">Korrekturwert</label>
          <Input
            className="mt-1.5"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder={`Wert für ${getFieldLabel(selectedColumn)} eingeben...`}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && value) onCorrect(error.id, value, '', 'save');
            }}
          />
        </div>

        {/* Actions */}
        <div className="mt-6 flex items-center gap-3">
          <Button
            onClick={() => onCorrect(error.id, value, '', 'save')}
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
