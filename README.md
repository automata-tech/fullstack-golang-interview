# Lab Automation System - Technical Exercise

## Overview

Welcome to the Lab Automation System technical exercise! This is a microservice-based application for managing laboratory workflows and devices. Your task is to debug existing issues and implement a new feature.

**Time allocation: 60-90 minutes**

## System Architecture

The system consists of:

- **Frontend**: React application (port 3000)
- **Backend Microservices** (Go):
  - `workflow-service`: Manages automation workflows (port 5003)
  - `device-service`: Controls lab equipment (port 5001)
  - `sample-service`: Tracks samples (port 5002)
- **Infrastructure**: Redis for caching and state management

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                          Browser                                │
│                     (localhost:3000)                            │
└───────────────┬─────────────────┬─────────────────┬─────────────┘
                │                 │                 │
                │ HTTP            │ HTTP            │ HTTP
                │                 │                 │
        ┌───────▼────────┐ ┌──────▼───────┐ ┌──────▼───────┐
        │   Workflow     │ │   Device     │ │   Sample     │
        │   Service      │ │   Service    │ │   Service    │
        │   (port 5003)  │ │  (port 5001) │ │  (port 5002) │
        └────────┬───────┘ └──────┬───────┘ └──────┬───────┘
                 │                │                 │
                 │ Redis Client   │ Redis Client    │ Redis Client
                 │                │                 │
        ┌────────▼────────────────▼─────────────────▼───────┐
        │                    Redis                           │
        │          (caching and state management)            │
        └────────────────────────────────────────────────────┘
```

**Key Flows**:
- Browser connects directly to all three backend services via HTTP
- All services share Redis for state management and caching
- Workflow service coordinates with device and sample services
- Device service manages device availability and booking status

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Git

### Setup

1. Clone this repository (if not already done)
2. Navigate to the project directory
3. Start the system:

```bash
docker-compose up --build
```

4. Access the application:
   - Frontend: http://localhost:3000
   - Workflow Service: http://localhost:5003
   - Device Service: http://localhost:5001
   - Sample Service: http://localhost:5002

## Exercise Structure

This exercise is divided into two phases:

### Phase 1: Debugging (30-40 minutes)

The system currently has several bugs that prevent it from working correctly. Your task is to identify and fix them.

**Scenario**: A lab technician reports that they cannot start workflows. The system appears to have multiple issues.

**Known Issues**:
1. One or more services fail to start
2. Workflows cannot be started successfully
3. Device status display is incorrect
4. System has race conditions under concurrent load

**Your Tasks**:
- Get all services running correctly
- Enable workflows to be created and started successfully
- Ensure device status updates correctly in the UI
- Identify and fix the race condition in device booking
  - Run `./test-race-condition.sh` to reproduce the concurrency bug
  - Check logs to see if multiple workflows book the same device

**Tips**:
- Check service logs: `docker-compose logs [service-name]`
- Use browser DevTools to inspect API calls
- Test the system end-to-end after each fix
- For the race condition, look for multiple "successfully booked" messages in device-service logs

### Phase 2: Feature Implementation (30-50 minutes)

Implement a **Workflow Pause/Resume** feature that allows users to pause running workflows and resume them later.

**Requirements**:

**Backend** (`workflow-service`):
- Add `POST /workflows/<workflow_id>/pause` endpoint
  - Should release the device (but mark it as resumable)
  - Update workflow status to "paused"
  - Store any necessary state for resuming
- Add `POST /workflows/<workflow_id>/resume` endpoint
  - Should re-book the device
  - Update workflow status back to "running"
  - Handle errors gracefully (e.g., device no longer available)

**Frontend**:
- Add "Pause" button for workflows with status "running"
- Add "Resume" button for workflows with status "paused"
- Handle loading and error states appropriately
- Update the UI to distinguish paused workflows visually

**Integration**:
- Ensure device status updates correctly when paused/resumed
- Test the complete flow: create → start → pause → resume → complete

## Evaluation Criteria

We're looking for:

✅ **System Understanding**: Can you trace requests across services? Do you understand the architecture?

✅ **Debugging Skills**: Do you use a methodical approach? Check logs? Use appropriate debugging tools?

✅ **Full-Stack Competency**: Are you comfortable working across React and Go? Can you switch contexts effectively?

✅ **Problem-Solving**: When you don't know something, do you know how to find the answer?

✅ **Code Quality**: Is your implementation clean, well-structured, and maintainable?

✅ **Communication**: Do you explain your thought process? Ask clarifying questions?

## Useful Commands

```bash
# View logs for all services
docker-compose logs -f

# View logs for specific service (recommended for debugging)
docker-compose logs -f workflow-service
docker-compose logs -f device-service
docker-compose logs -f sample-service

# View recent logs without following
docker-compose logs --tail=50 workflow-service

# Test for race condition in device booking
./test-race-condition.sh

# Reset all data (clear workflows, device statuses, samples)
docker-compose restart redis

# Restart a specific service
docker-compose restart workflow-service

# Rebuild and restart
docker-compose up --build

# Stop all services
docker-compose down

# Stop all services and remove volumes (complete clean slate)
docker-compose down -v

# Check if services are healthy
curl http://localhost:5001/health
curl http://localhost:5002/health
curl http://localhost:5003/health
```

## API Documentation

### Workflow Service

- `GET /workflows` - List all workflows
- `POST /workflows` - Create workflow
  ```json
  {
    "name": "PCR Setup",
    "device_id": "liquid-handler-1",
    "sample_barcodes": ["SAMPLE001"],
    "steps": ["Aspirate 10uL", "Dispense to A1"]
  }
  ```
- `POST /workflows/<id>/start` - Start workflow
- `POST /workflows/<id>/complete` - Complete workflow

### Device Service

- `GET /devices` - List all devices
- `GET /devices/<id>` - Get device details
- `POST /devices/<id>/book` - Book device for workflow
- `POST /devices/<id>/release` - Release device

### Sample Service

- `GET /samples` - List all samples
- `GET /samples/<barcode>` - Get sample details
- `POST /samples/validate` - Validate sample barcodes

## Questions?

Feel free to ask questions at any time! We're interested in how you approach problems and work through challenges, not just whether you can find all the bugs immediately.

Good luck! 🚀
