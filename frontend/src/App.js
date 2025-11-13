import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';
import DeviceList from './components/DeviceList';
import WorkflowList from './components/WorkflowList';
import CreateWorkflow from './components/CreateWorkflow';

const WORKFLOW_API = process.env.REACT_APP_WORKFLOW_API || 'http://localhost:5003';
const DEVICE_API = process.env.REACT_APP_DEVICE_API || 'http://localhost:5001';
const SAMPLE_API = process.env.REACT_APP_SAMPLE_API || 'http://localhost:5002';

function App() {
  const [devices, setDevices] = useState([]);
  const [workflows, setWorkflows] = useState([]);
  const [samples, setSamples] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showCreateWorkflow, setShowCreateWorkflow] = useState(false);

  const fetchDevices = async () => {
    try {
      const response = await axios.get(`${DEVICE_API}/devices`);
      setDevices(response.data);
    } catch (err) {
      console.error('Error fetching devices:', err);
      setError('Failed to fetch devices');
    }
  };

  const fetchWorkflows = async () => {
    try {
      const response = await axios.get(`${WORKFLOW_API}/workflows`);
      setWorkflows(response.data);
    } catch (err) {
      console.error('Error fetching workflows:', err);
      setError('Failed to fetch workflows');
    }
  };

  const fetchSamples = async () => {
    try {
      const response = await axios.get(`${SAMPLE_API}/samples`);
      setSamples(response.data);
    } catch (err) {
      console.error('Error fetching samples:', err);
      setError('Failed to fetch samples');
    }
  };

  const fetchData = async () => {
    setLoading(true);
    await Promise.all([fetchDevices(), fetchWorkflows(), fetchSamples()]);
    setLoading(false);
  };

  useEffect(() => {
    fetchData();
    // Poll for updates every 5 seconds
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleStartWorkflow = async (workflowId) => {
    try {
      await axios.post(`${WORKFLOW_API}/workflows/${workflowId}/start`);
      await fetchData();
    } catch (err) {
      console.error('Error starting workflow:', err);
      alert(`Failed to start workflow: ${err.response?.data?.error || err.message}`);
    }
  };

  const handleCompleteWorkflow = async (workflowId) => {
    try {
      await axios.post(`${WORKFLOW_API}/workflows/${workflowId}/complete`);
      await fetchData();
    } catch (err) {
      console.error('Error completing workflow:', err);
      alert(`Failed to complete workflow: ${err.response?.data?.error || err.message}`);
    }
  };

  const handleCreateWorkflow = async (workflowData) => {
    try {
      await axios.post(`${WORKFLOW_API}/workflows`, workflowData);
      setShowCreateWorkflow(false);
      await fetchData();
    } catch (err) {
      console.error('Error creating workflow:', err);
      alert(`Failed to create workflow: ${err.response?.data?.error || err.message}`);
    }
  };

  if (loading && devices.length === 0) {
    return (
      <div className="App">
        <div className="loading">Loading lab automation system...</div>
      </div>
    );
  }

  return (
    <div className="App">
      <header className="App-header">
        <h1>Lab Automation System</h1>
        <p>Workflow and Device Management Dashboard</p>
      </header>

      {error && (
        <div className="error-banner">
          {error}
        </div>
      )}

      <div className="main-content">
        <div className="section">
          <div className="section-header">
            <h2>Devices</h2>
            <button onClick={fetchDevices} className="refresh-btn">
              Refresh
            </button>
          </div>
          <DeviceList devices={devices} />
        </div>

        <div className="section">
          <div className="section-header">
            <h2>Workflows</h2>
            <div>
              <button onClick={fetchWorkflows} className="refresh-btn">
                Refresh
              </button>
              <button
                onClick={() => setShowCreateWorkflow(!showCreateWorkflow)}
                className="create-btn"
              >
                {showCreateWorkflow ? 'Cancel' : 'Create Workflow'}
              </button>
            </div>
          </div>

          {showCreateWorkflow && (
            <CreateWorkflow
              devices={devices}
              samples={samples}
              onSubmit={handleCreateWorkflow}
              onCancel={() => setShowCreateWorkflow(false)}
            />
          )}

          <WorkflowList
            workflows={workflows}
            onStart={handleStartWorkflow}
            onComplete={handleCompleteWorkflow}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
