import React, { useState, useEffect } from 'react';
import mockData from './mockData.json';
import './Dashboard.css';

const Dashboard = () => {
  // State for metrics
  const [totalCases, setTotalCases] = useState(0);
  const [brokenEntries, setBrokenEntries] = useState([]);
  
  // State for the manual mapping interface
  const [selectedEntry, setSelectedEntry] = useState(null);
  const [selectedColumn, setSelectedColumn] = useState('');
  const [correctionValue, setCorrectionValue] = useState('');
  
  // UI States
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [notification, setNotification] = useState(null);

  // 1. Fetch initial data (Using mock data for the prototype)
  useEffect(() => {
    // In a real app, this would be: fetch('/api/missing-data').then(...)
    setTotalCases(mockData.totalCases);
    setBrokenEntries(mockData.brokenEntries);
  }, []);

  // Handler: Selecting a row to fix
  const handleSelectEntry = (entry) => {
    setSelectedEntry(entry);
    // Default the dropdown to the first missing column
    setSelectedColumn(entry.missing_columns[0] || '');
    setCorrectionValue('');
    setNotification(null);
  };

  // Handler: Submitting the fix (The Feedback Loop)
  const handleSubmitFix = async (e) => {
    e.preventDefault();
    if (!selectedEntry || !selectedColumn || !correctionValue) return;

    setIsSubmitting(true);

    const payload = {
      table: selectedEntry.table,
      row_id: selectedEntry.row_id,
      column_name: selectedColumn,
      new_value: correctionValue
    };

    try {
      // Simulate API Call delay
      // In production: await fetch('/api/missing-data/fix', { method: 'POST', body: JSON.stringify(payload) })
      await new Promise((resolve) => setTimeout(resolve, 800));

      console.log('Sending correction to Python Backend:', payload);

      // --- SUCCESS STATE LOGIC ---
      // 1. Update the local state to remove the fixed missing column
      setBrokenEntries((prevEntries) => {
        return prevEntries.map(entry => {
          if (entry.id === selectedEntry.id) {
            // Remove the column that was just fixed
            const updatedColumns = entry.missing_columns.filter(col => col !== selectedColumn);
            
            // If the entry has no more missing columns, return null so we can filter it out completely
            if (updatedColumns.length === 0) return null;
            
            // Otherwise, keep the entry but with the updated columns list
            return { ...entry, missing_columns: updatedColumns };
          }
          return entry;
        }).filter(Boolean); // removes any entries that turned to null
      });

      // Show success message
      setNotification({ type: 'success', message: `Successfully mapped ${selectedColumn} to ${correctionValue}!` });
      
      // Close the form
      setSelectedEntry(null);
      setCorrectionValue('');

    } catch (error) {
      setNotification({ type: 'error', message: 'Failed to save correction. Please try again.' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>epaCC Data Ingestion: Missing Data Tool</h1>
      </header>

      {/* Overview Metrics Layer */}
      <section className="metrics-grid">
        <div className="metric-card">
          <h3>Total Analyzed Cases</h3>
          <p className="metric-value">{totalCases}</p>
        </div>
        <div className="metric-card alert">
          <h3>Entries Needing Review</h3>
          <p className="metric-value">{brokenEntries.length}</p>
        </div>
        <div className="metric-card success">
          <h3>Data Health Score</h3>
          <p className="metric-value">
            {totalCases > 0 ? (((totalCases - brokenEntries.length) / totalCases) * 100).toFixed(1) : 0}%
          </p>
        </div>
      </section>

      <div className="main-content">
        {/* Missing Data Visualization (Inbox) */}
        <section className="data-inbox">
          <h2>Broken Data Entries</h2>
          {brokenEntries.length === 0 ? (
            <div className="empty-state">✅ All data is clean and mapped!</div>
          ) : (
            <div className="table-wrapper">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Table Source</th>
                    <th>Row ID</th>
                    <th>Missing Fields</th>
                    <th>Available Context</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {brokenEntries.map((entry) => (
                    <tr key={entry.id} className={selectedEntry?.id === entry.id ? 'selected-row' : ''}>
                      <td><span className="badge">{entry.table}</span></td>
                      <td>{entry.row_id}</td>
                      <td className="missing-text">{entry.missing_columns.join(', ')}</td>
                      <td className="context-text">{JSON.stringify(entry.context)}</td>
                      <td>
                        <button 
                          className="btn-fix"
                          onClick={() => handleSelectEntry(entry)}
                        >
                          Fix Entry
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        {/* Manual Correction Interface */}
        <section className="correction-panel">
          <h2>Manual Correction Interface</h2>
          
          {notification && (
            <div className={`notification ${notification.type}`}>
              {notification.message}
            </div>
          )}

          {!selectedEntry ? (
            <div className="empty-state">Select an entry from the table to review and fix.</div>
          ) : (
            <form className="correction-form" onSubmit={handleSubmitFix}>
              <div className="form-info">
                <p><strong>Fixing Table:</strong> {selectedEntry.table}</p>
                <p><strong>Row ID:</strong> {selectedEntry.row_id}</p>
              </div>

              <div className="form-group">
                <label>Which field are you fixing?</label>
                <select 
                  value={selectedColumn} 
                  onChange={(e) => setSelectedColumn(e.target.value)}
                  required
                >
                  {selectedEntry.missing_columns.map(col => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </select>
              </div>

              <div className="form-group">
                <label>Enter the Corrected Value</label>
                <input 
                  type="text" 
                  value={correctionValue}
                  onChange={(e) => setCorrectionValue(e.target.value)}
                  placeholder={`e.g., Search Case DB for correct ${selectedColumn}`}
                  required
                />
              </div>

              <div className="form-actions">
                <button type="button" className="btn-cancel" onClick={() => setSelectedEntry(null)}>Cancel</button>
                <button type="submit" className="btn-save" disabled={isSubmitting}>
                  {isSubmitting ? 'Saving...' : 'Save & Map Data'}
                </button>
              </div>
            </form>
          )}
        </section>
      </div>
    </div>
  );
};

export default Dashboard;