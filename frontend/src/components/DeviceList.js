import React, { useState, useEffect } from 'react';
import './DeviceList.css';

const deviceStatusCache = {};

function DeviceList({ devices }) {
  const [cachedDevices, setCachedDevices] = useState([]);

  useEffect(() => {
    const devicesWithCachedStatus = devices.map((device) => {
      if (!deviceStatusCache[device.id]) {
        deviceStatusCache[device.id] = device.status;
      }

      return {
        ...device,
        status: deviceStatusCache[device.id]
      };
    });

    setCachedDevices(devicesWithCachedStatus);
  }, [devices]);

  const getStatusColor = (status) => {
    switch (status) {
      case 'available':
        return '#4caf50';
      case 'busy':
        return '#ff9800';
      default:
        return '#999';
    }
  };

  return (
    <div className="device-list">
      {cachedDevices.length === 0 ? (
        <p className="empty-state">No devices available</p>
      ) : (
        cachedDevices.map((device) => (
          <div key={device.id} className="device-card">
            <div className="device-header">
              <h3>{device.name}</h3>
              <span
                className="status-badge"
                style={{ backgroundColor: getStatusColor(device.status) }}
              >
                {device.status}
              </span>
            </div>
            <div className="device-details">
              <p><strong>Type:</strong> {device.type}</p>
              <p><strong>ID:</strong> {device.id}</p>
              {device.workflow_id && (
                <p><strong>Workflow:</strong> {device.workflow_id}</p>
              )}
              {device.capabilities && (
                <div className="capabilities">
                  <strong>Capabilities:</strong>
                  <div className="capability-tags">
                    {device.capabilities.map((cap) => (
                      <span key={cap} className="capability-tag">
                        {cap}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        ))
      )}
    </div>
  );
}

export default DeviceList;
