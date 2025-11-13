import React, { useState } from 'react';
import './CreateWorkflow.css';

function CreateWorkflow({ devices, samples, onSubmit, onCancel }) {
  const [name, setName] = useState('');
  const [deviceId, setDeviceId] = useState('');
  const [selectedSamples, setSelectedSamples] = useState([]);
  const [steps, setSteps] = useState('');

  const handleSubmit = (e) => {
    e.preventDefault();

    const workflowData = {
      name,
      device_id: deviceId,
      sample_barcodes: selectedSamples,
      steps: steps.split('\n').filter(s => s.trim())
    };

    onSubmit(workflowData);

    // Reset form
    setName('');
    setDeviceId('');
    setSelectedSamples([]);
    setSteps('');
  };

  const toggleSample = (barcode) => {
    if (selectedSamples.includes(barcode)) {
      setSelectedSamples(selectedSamples.filter(s => s !== barcode));
    } else {
      setSelectedSamples([...selectedSamples, barcode]);
    }
  };

  const availableDevices = devices.filter(d => d.status === 'available');

  return (
    <div className="create-workflow">
      <h3>Create New Workflow</h3>
      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="name">Workflow Name</label>
          <input
            type="text"
            id="name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="e.g., PCR Setup Protocol"
            required
          />
        </div>

        <div className="form-group">
          <label htmlFor="device">Device</label>
          <select
            id="device"
            value={deviceId}
            onChange={(e) => setDeviceId(e.target.value)}
            required
          >
            <option value="">Select a device...</option>
            {availableDevices.map((device) => (
              <option key={device.id} value={device.id}>
                {device.name} ({device.type})
              </option>
            ))}
          </select>
          {availableDevices.length === 0 && (
            <p className="warning">No devices currently available</p>
          )}
        </div>

        <div className="form-group">
          <label>Samples (optional)</label>
          <div className="sample-checkboxes">
            {samples.map((sample) => (
              <label key={sample.barcode} className="checkbox-label">
                <input
                  type="checkbox"
                  checked={selectedSamples.includes(sample.barcode)}
                  onChange={() => toggleSample(sample.barcode)}
                />
                {sample.barcode} - {sample.name}
              </label>
            ))}
          </div>
        </div>

        <div className="form-group">
          <label htmlFor="steps">Workflow Steps (one per line)</label>
          <textarea
            id="steps"
            value={steps}
            onChange={(e) => setSteps(e.target.value)}
            placeholder="e.g.,&#10;Aspirate 10uL&#10;Dispense to plate A1&#10;Incubate 5 minutes"
            rows="5"
          />
        </div>

        <div className="form-actions">
          <button type="button" onClick={onCancel} className="cancel-btn">
            Cancel
          </button>
          <button type="submit" className="submit-btn">
            Create Workflow
          </button>
        </div>
      </form>
    </div>
  );
}

export default CreateWorkflow;
