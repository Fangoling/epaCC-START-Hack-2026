import { useState, useEffect } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Activity, ShieldCheck } from "lucide-react";
import DashboardHeader from "@/components/DashboardHeader";
import PatientList from "@/components/PatientList";
import PatientDetailView from "@/components/PatientDetailView";
import DataQualityDashboard from "@/components/DataQualityDashboard";
import { getPatientSummaries, fieldLabels, type PatientSummary, type DataError } from "@/data/mockData";
import { fetchPatients, fetchMissingData, type BrokenEntry } from "@/lib/api";

function transformBrokenEntriesToErrors(entries: BrokenEntry[]): DataError[] {
  return entries.map((entry) => {
    const firstCol = entry.missing_columns[0] || "";
    const patientId = entry.patient_id ? `P-${entry.patient_id}` : "Unbekannt";
    return {
      id: entry.id,
      sourceInstitution: "Unbekannt",
      patientId,
      dataField: fieldLabels[firstCol] || firstCol,
      tableName: entry.table,
      columnName: firstCol,
      errorDescription: `${fieldLabels[firstCol] || firstCol} fehlt`,
      errorType: "missing" as const,
      priority: "medium" as const,
      status: "new" as const,
      category: entry.table,
      rowId: entry.row_id,
      allMissingColumns: entry.missing_columns,
    };
  });
}

const Index = () => {
  const [patients, setPatients] = useState<PatientSummary[]>([]);
  const [errors, setErrors] = useState<DataError[]>([]);
  const [selectedPatient, setSelectedPatient] = useState<PatientSummary | null>(null);
  const [activeTab, setActiveTab] = useState("explorer");
  const [apiAvailable, setApiAvailable] = useState(false);

  useEffect(() => {
    // Try to load real data from API; fall back to mock data if unavailable
    Promise.all([fetchPatients(), fetchMissingData()])
      .then(([apiPatients, missingData]) => {
        setPatients(apiPatients);
        setErrors(transformBrokenEntriesToErrors(missingData.brokenEntries));
        setSelectedPatient(apiPatients[0] || null);
        setApiAvailable(true);
      })
      .catch(() => {
        // Fallback: use mock data
        const mockPatients = getPatientSummaries();
        setPatients(mockPatients);
        setSelectedPatient(mockPatients[0] || null);
        setApiAvailable(false);
      });
  }, []);

  return (
    <div className="flex min-h-screen flex-col bg-background">
      <DashboardHeader />

      <div className="flex-1 px-6 py-4">
        <Tabs value={activeTab} onValueChange={setActiveTab}>
          <TabsList className="mb-4 bg-secondary">
            <TabsTrigger value="explorer" className="gap-2 data-[state=active]:bg-card data-[state=active]:shadow-epa">
              <Activity className="h-4 w-4" /> Smart Health Explorer
            </TabsTrigger>
            <TabsTrigger value="quality" className="gap-2 data-[state=active]:bg-card data-[state=active]:shadow-epa">
              <ShieldCheck className="h-4 w-4" /> Data Quality
            </TabsTrigger>
          </TabsList>

          <TabsContent value="explorer">
            <div className="grid grid-cols-[320px_1fr] gap-4" style={{ height: 'calc(100vh - 260px)' }}>
              <div className="overflow-hidden">
                <PatientList
                  patients={patients}
                  errors={errors}
                  selectedPatient={selectedPatient}
                  onSelectPatient={setSelectedPatient}
                />
              </div>
              <div className="overflow-y-auto pr-1">
                {selectedPatient && (
                  <PatientDetailView
                    patient={selectedPatient}
                    apiAvailable={apiAvailable}
                  />
                )}
              </div>
            </div>
          </TabsContent>

          <TabsContent value="quality">
            <DataQualityDashboard errors={errors} />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
};

export default Index;
