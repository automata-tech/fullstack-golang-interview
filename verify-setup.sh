#!/bin/bash

echo "=========================================="
echo "Exercise Setup Verification"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker."
    exit 1
fi
echo "✓ Docker is running"

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ docker-compose.yml not found"
    exit 1
fi
echo "✓ docker-compose.yml found"

# Check for key files
echo ""
echo "Checking key files..."
files=(
    "README.md"
    "SOLUTION_GUIDE.md"
    "test-race-condition.sh"
    "services/workflow-service/app.py"
    "services/device-service/app.py"
    "services/sample-service/app.py"
    "frontend/src/App.js"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    else
        echo "  ❌ $file missing"
    fi
done

echo ""
echo "Checking bugs are present..."

# Check Bug 1: Missing DEVICE_API_URL in docker-compose.yml
if ! grep -q "DEVICE_API_URL" docker-compose.yml; then
    echo "  ✓ Bug 1: DEVICE_API_URL missing from docker-compose.yml (correct)"
else
    echo "  ⚠ Bug 1: DEVICE_API_URL is present in docker-compose.yml (bug may be fixed)"
fi

# Check Bug 2: Wrong endpoint in workflow-service
if grep -q "/device/.*reserve" services/workflow-service/app.py; then
    echo "  ✓ Bug 2: Wrong endpoint path present in workflow-service (correct)"
else
    echo "  ⚠ Bug 2: Endpoint may already be fixed"
fi

# Check Bug 3: Race condition (time.sleep present)
if grep -q "time.sleep" services/device-service/app.py; then
    echo "  ✓ Bug 3: Race condition timing present in device-service (correct)"
else
    echo "  ⚠ Bug 3: time.sleep removed (race condition may be harder to trigger)"
fi

# Check Bug 4: Frontend caching
if grep -q "deviceStatusCache" frontend/src/components/DeviceList.js; then
    echo "  ✓ Bug 4: Status caching present in DeviceList.js (correct)"
else
    echo "  ⚠ Bug 4: Caching may already be removed"
fi

echo ""
echo "=========================================="
echo "Setup verification complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Start the system: docker-compose up --build"
echo "2. Verify services are running at:"
echo "   - Frontend: http://localhost:3000"
echo "   - Workflow API: http://localhost:5003/health"
echo "   - Device API: http://localhost:5001/health"
echo "   - Sample API: http://localhost:5002/health"
echo ""
