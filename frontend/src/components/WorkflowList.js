import React from 'react';
import './WorkflowList.css';

function WorkflowList({ workflows, onStart, onComplete }) {
  const getStatusColor = (status) => {
    switch (status) {
      case 'created':
        return '#2196f3';
      case 'running':
        return '#ff9800';
      case 'completed':
        return '#4caf50';
      default:
        return '#999';
    }
  };

  return (
    <div className="workflow-list">
      {workflows.length === 0 ? (
        <p className="empty-state">No workflows created yet</p>
      ) : (
        workflows.map((workflow) => (
          <div key={workflow.id} className="workflow-card">
            <div className="workflow-header">
              <div>
                <h3>{workflow.name}</h3>
                <p className="workflow-id">ID: {workflow.id}</p>
              </div>
              <span
                className="status-badge"
                style={{ backgroundColor: getStatusColor(workflow.status) }}
              >
                {workflow.status}
              </span>
            </div>

            <div className="workflow-details">
              <p><strong>Device:</strong> {workflow.device_id}</p>
              {workflow.sample_barcodes && workflow.sample_barcodes.length > 0 && (
                <p>
                  <strong>Samples:</strong> {workflow.sample_barcodes.join(', ')}
                </p>
              )}
              {workflow.steps && workflow.steps.length > 0 && (
                <p>
                  <strong>Steps:</strong> {workflow.steps.length} step(s)
                </p>
              )}
              <p className="timestamp">
                Created: {new Date(workflow.created_at).toLocaleString()}
              </p>
              {workflow.started_at && (
                <p className="timestamp">
                  Started: {new Date(workflow.started_at).toLocaleString()}
                </p>
              )}
              {workflow.completed_at && (
                <p className="timestamp">
                  Completed: {new Date(workflow.completed_at).toLocaleString()}
                </p>
              )}
            </div>

            <div className="workflow-actions">
              {workflow.status === 'created' && (
                <button
                  onClick={() => onStart(workflow.id)}
                  className="start-btn"
                >
                  Start Workflow
                </button>
              )}
              {workflow.status === 'running' && (
                <button
                  onClick={() => onComplete(workflow.id)}
                  className="complete-btn"
                >
                  Complete Workflow
                </button>
              )}
            </div>
          </div>
        ))
      )}
    </div>
  );
}

export default WorkflowList;
