import { Moon, Sun } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useEffect, useState } from "react";

const DashboardHeader = () => {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains('dark'));

  useEffect(() => {
    if (dark) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [dark]);

  return (
    <header className="border-b border-border bg-card px-6 py-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="relative flex h-11 w-11 items-center justify-center rounded-xl overflow-hidden shadow-epa" style={{ background: 'linear-gradient(135deg, hsl(248 46% 16%), hsl(252 50% 22%))' }}>
            <svg width="30" height="30" viewBox="0 0 30 30" fill="none" xmlns="http://www.w3.org/2000/svg" className="relative">
              {/* Core fusion point */}
              <circle cx="15" cy="15" r="3" fill="white" opacity="0.95" />
              <circle cx="15" cy="15" r="4.5" stroke="white" strokeWidth="0.8" opacity="0.3" />
              {/* Starburst rays */}
              {[0, 45, 90, 135, 180, 225, 270, 315].map((angle, i) => {
                const len = i % 2 === 0 ? 10.5 : 7;
                const rad = (angle * Math.PI) / 180;
                const x1 = 15 + Math.cos(rad) * 5;
                const y1 = 15 + Math.sin(rad) * 5;
                const x2 = 15 + Math.cos(rad) * len;
                const y2 = 15 + Math.sin(rad) * len;
                return (
                  <line key={angle} x1={x1} y1={y1} x2={x2} y2={y2}
                    stroke="white" strokeWidth={i % 2 === 0 ? "1.5" : "0.8"}
                    opacity={i % 2 === 0 ? "0.9" : "0.5"}
                    strokeLinecap="round" />
                );
              })}
              {/* Energy particles */}
              <circle cx="22" cy="8" r="0.8" fill="white" opacity="0.5" />
              <circle cx="7" cy="21" r="0.6" fill="white" opacity="0.4" />
              <circle cx="23" cy="22" r="0.5" fill="white" opacity="0.35" />
            </svg>
          </div>
          <div className="flex items-baseline gap-3">
            <h1 className="text-lg font-bold tracking-tight">
              <span className="text-muted-foreground">epa</span><span className="text-gradient-epa">FUSION</span>
            </h1>
            <span className="text-muted-foreground/30 text-lg font-light">/</span>
            <span className="text-sm text-muted-foreground/70 tracking-wide">Wo Gesundheitsdaten verschmelzen</span>
          </div>
        </div>
        <Button variant="ghost" size="icon" onClick={() => setDark(!dark)} title={dark ? 'Light Mode' : 'Dark Mode'}>
          {dark ? <Sun className="h-5 w-5 text-muted-foreground" /> : <Moon className="h-5 w-5 text-muted-foreground" />}
        </Button>
      </div>
    </header>
  );
};

export default DashboardHeader;
