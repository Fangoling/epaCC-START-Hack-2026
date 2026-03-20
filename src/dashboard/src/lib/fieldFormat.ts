export interface FieldFormat {
  hint: string;
  placeholder: string;
  validate: (v: string) => string | null; // null = valid
  transform: (v: string) => string;       // converts user input to DB value
}

export function getFieldFormat(col: string): FieldFormat {
  const c = col.toLowerCase();

  // Datetime: user enters YYYY-MM-DD, we append T00:00:00 before saving
  if (
    c.includes('_datetime') ||
    c === 'cotimestamp' ||
    c === 'coe2i223' ||
    c === 'coe2i228' ||
    c === 'codateofbirth'
  ) {
    return {
      hint: 'Format: YYYY-MM-DD',
      placeholder: '2024-05-10',
      validate: (v) =>
        /^\d{4}-\d{2}-\d{2}$/.test(v.trim())
          ? null
          : 'Erwartet: YYYY-MM-DD (z.B. 2024-05-10)',
      transform: (v) => `${v.trim()}T00:00:00`,
    };
  }

  // Date only: _date but not _datetime
  if (c.includes('_date') && !c.includes('_datetime')) {
    return {
      hint: 'Format: YYYY-MM-DD',
      placeholder: '2024-05-10',
      validate: (v) =>
        /^\d{4}-\d{2}-\d{2}$/.test(v.trim())
          ? null
          : 'Erwartet: YYYY-MM-DD (z.B. 2024-05-10)',
      transform: (v) => v.trim(),
    };
  }

  // Boolean 0/1
  if (c.endsWith('_0_1')) {
    return {
      hint: 'Erlaubte Werte: 0 (Nein) oder 1 (Ja)',
      placeholder: '0',
      validate: (v) =>
        v.trim() === '0' || v.trim() === '1'
          ? null
          : 'Bitte 0 (Nein) oder 1 (Ja) eingeben',
      transform: (v) => v.trim(),
    };
  }

  // Score / index 0–100
  if (c.endsWith('_0_100')) {
    return {
      hint: 'Zahl zwischen 0 und 100',
      placeholder: '50',
      validate: (v) => {
        const n = Number(v.trim());
        if (isNaN(n) || v.trim() === '') return 'Bitte eine Zahl eingeben';
        if (n < 0 || n > 100) return 'Wert muss zwischen 0 und 100 liegen';
        return null;
      },
      transform: (v) => v.trim(),
    };
  }

  // Decimal lab / sensor values
  if (c.match(/_(mmol_l|mg_dl|g_dl|u_l|inr$|magnitude_g|accel_)/)) {
    return {
      hint: 'Dezimalzahl (z.B. 4.2)',
      placeholder: '0.0',
      validate: (v) =>
        !isNaN(Number(v.trim())) && v.trim() !== ''
          ? null
          : 'Bitte eine gültige Dezimalzahl eingeben (z.B. 4.2)',
      transform: (v) => v.trim(),
    };
  }

  // Integer: age, days, count, minutes, AC assessment codes
  if (c.match(/(ageyears|_days|_count|_minutes|maxdeku|lastassessment)/) || c.startsWith('coe0i')) {
    return {
      hint: 'Ganzzahl (z.B. 42)',
      placeholder: '0',
      validate: (v) =>
        /^-?\d+$/.test(v.trim())
          ? null
          : 'Bitte eine ganze Zahl eingeben (z.B. 42)',
      transform: (v) => v.trim(),
    };
  }

  // Default: free text
  return {
    hint: '',
    placeholder: 'Wert eingeben...',
    validate: () => null,
    transform: (v) => v,
  };
}
