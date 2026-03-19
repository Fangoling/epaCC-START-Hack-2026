import { useState, useMemo } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Activity, ShieldCheck } from "lucide-react";
import DashboardHeader from "@/components/DashboardHeader";
import PatientList from "@/components/PatientList";
import PatientDetailView from "@/components/PatientDetailView";
import DataQualityDashboard from "@/components/DataQualityDashboard";
import { getPatientSummaries, type PatientSummary } from "@/data/mockData";

const Index = () => {
  const patients = useMemo(() => getPatientSummaries(), []);
  const [selectedPatient, setSelectedPatient] = useState<PatientSummary | null>(patients[0] || null);
  const [activeTab, setActiveTab] = useState("explorer");

  return (
    <div className="flex min-h-screen flex-col bg-background">
      <DashboardHeader />


      {/* Main content */}
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
                <PatientList selectedPatient={selectedPatient} onSelectPatient={setSelectedPatient} />
              </div>
              <div className="overflow-y-auto pr-1">
                {selectedPatient && (
                  <PatientDetailView patient={selectedPatient} />
                )}
              </div>
            </div>
          </TabsContent>

          <TabsContent value="quality">
            <DataQualityDashboard />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
};

export default Index;
