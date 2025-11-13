#!/bin/bash

echo "=========================================="
echo "Race Condition Test: Concurrent Device Booking"
echo "=========================================="
echo ""
echo "This script attempts to create 5 workflows simultaneously,"
echo "all trying to book the same device (liquid-handler-1)."
echo ""
echo "Expected behavior: Only ONE workflow should successfully book the device."
echo "Bug behavior: Multiple workflows may book the same device."
echo ""
echo "Starting test in 3 seconds..."
sleep 3

WORKFLOW_API="http://localhost:5003"
DEVICE_ID="liquid-handler-1"

echo ""
echo "Creating 5 concurrent workflows..."
echo ""

# Create workflow creation function
create_workflow() {
  local name=$1
  local response=$(curl -s -X POST "${WORKFLOW_API}/workflows" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"Race Test ${name}\",\"device_id\":\"${DEVICE_ID}\",\"sample_barcodes\":[\"SAMPLE001\"]}" 2>&1)

  local workflow_id=$(echo "$response" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

  if [ -n "$workflow_id" ]; then
    echo "  Created workflow: Race Test ${name} (ID: ${workflow_id})"

    # Try to start the workflow
    local start_response=$(curl -s -X POST "${WORKFLOW_API}/workflows/${workflow_id}/start" 2>&1)
    local status=$(echo "$start_response" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)

    if [ "$status" == "running" ]; then
      echo "    ✓ Successfully started and booked device"
    else
      echo "    ✗ Failed to start (expected - device already booked)"
    fi
  fi
}

# Launch 5 concurrent workflow creations
for i in {1..5}; do
  create_workflow $i &
done

# Wait for all background jobs to complete
wait

echo ""
echo "=========================================="
echo "Test Complete"
echo "=========================================="
echo ""
echo "Now check the results:"
echo ""
echo "1. View device-service logs:"
echo "   docker-compose logs device-service | grep 'successfully booked'"
echo ""
echo "2. Expected: You should see ONE 'successfully booked' message"
echo "   Bug: You may see MULTIPLE 'successfully booked' messages"
echo ""
echo "3. Check device status:"
echo "   curl http://localhost:5001/devices/liquid-handler-1"
echo ""
echo "4. List all workflows to see which ones are 'running':"
echo "   curl http://localhost:5003/workflows | jq '.[] | select(.status==\"running\") | {name, id, status}'"
echo ""
