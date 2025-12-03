package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	ctx         = context.Background()
)

type Device struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Status       string   `json:"status"`
	Capabilities []string `json:"capabilities"`
	WorkflowID   string   `json:"workflow_id,omitempty"`
}

type BookRequest struct {
	WorkflowID string `json:"workflow_id" binding:"required"`
}

type ReleaseRequest struct {
	WorkflowID string `json:"workflow_id"`
}

type ExecuteRequest struct {
	WorkflowID string `json:"workflow_id" binding:"required"`
	Operation  string `json:"operation" binding:"required"`
}

type BookResponse struct {
	DeviceID   string `json:"device_id"`
	Status     string `json:"status"`
	WorkflowID string `json:"workflow_id"`
	BookedAt   string `json:"booked_at"`
}

type ReleaseResponse struct {
	DeviceID   string `json:"device_id"`
	Status     string `json:"status"`
	ReleasedAt string `json:"released_at"`
}

type ExecuteResponse struct {
	DeviceID   string `json:"device_id"`
	Operation  string `json:"operation"`
	Status     string `json:"status"`
	ExecutedAt string `json:"executed_at"`
}

// Simulated lab devices
var DEVICES = map[string]Device{
	"liquid-handler-1": {
		ID:           "liquid-handler-1",
		Name:         "Liquid Handler Alpha",
		Type:         "liquid_handler",
		Status:       "available",
		Capabilities: []string{"pipette", "dispense", "aspirate"},
	},
	"incubator-1": {
		ID:           "incubator-1",
		Name:         "Incubator Beta",
		Type:         "incubator",
		Status:       "available",
		Capabilities: []string{"heat", "cool", "shake"},
	},
	"plate-reader-1": {
		ID:           "plate-reader-1",
		Name:         "Plate Reader Gamma",
		Type:         "plate_reader",
		Status:       "available",
		Capabilities: []string{"absorbance", "fluorescence"},
	},
}

func getDeviceStatus(deviceID string) string {
	cached, err := redisClient.Get(ctx, fmt.Sprintf("device:%s:status", deviceID)).Result()
	if err == nil {
		return cached
	}
	if device, ok := DEVICES[deviceID]; ok {
		return device.Status
	}
	return "unknown"
}

func setDeviceStatus(deviceID, status string, workflowID *string) {
	redisClient.Set(ctx, fmt.Sprintf("device:%s:status", deviceID), status, 0)
	if workflowID != nil && *workflowID != "" {
		redisClient.Set(ctx, fmt.Sprintf("device:%s:workflow", deviceID), *workflowID, 0)
	} else {
		redisClient.Del(ctx, fmt.Sprintf("device:%s:workflow", deviceID))
	}
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "device-service",
	})
}

func listDevicesHandler(c *gin.Context) {
	// Get device IDs in sorted order for consistent ordering
	deviceIDs := make([]string, 0, len(DEVICES))
	for deviceID := range DEVICES {
		deviceIDs = append(deviceIDs, deviceID)
	}
	sort.Strings(deviceIDs)

	devices := []Device{}
	for _, deviceID := range deviceIDs {
		deviceInfo := DEVICES[deviceID]
		device := deviceInfo
		device.Status = getDeviceStatus(deviceID)
		workflowID, err := redisClient.Get(ctx, fmt.Sprintf("device:%s:workflow", deviceID)).Result()
		if err == nil {
			device.WorkflowID = workflowID
		}
		devices = append(devices, device)
	}
	c.JSON(http.StatusOK, devices)
}

func getDeviceHandler(c *gin.Context) {
	deviceID := c.Param("device_id")
	deviceInfo, ok := DEVICES[deviceID]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found"})
		return
	}

	device := deviceInfo
	device.Status = getDeviceStatus(deviceID)
	workflowID, err := redisClient.Get(ctx, fmt.Sprintf("device:%s:workflow", deviceID)).Result()
	if err == nil {
		device.WorkflowID = workflowID
	}

	c.JSON(http.StatusOK, device)
}

func bookDeviceHandler(c *gin.Context) {
	deviceID := c.Param("device_id")

	if _, ok := DEVICES[deviceID]; !ok {
		log.Printf("Device not found: %s", deviceID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found"})
		return
	}

	var req BookRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Booking request missing workflow_id: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "workflow_id required"})
		return
	}

	log.Printf("Attempting to book device %s for workflow %s", deviceID, req.WorkflowID)

	currentStatus := getDeviceStatus(deviceID)

	if currentStatus != "available" {
		log.Printf("Device %s is not available (status: %s)", deviceID, currentStatus)
		c.JSON(http.StatusConflict, gin.H{"error": "Device is not available"})
		return
	}

	time.Sleep(100 * time.Millisecond)

	setDeviceStatus(deviceID, "busy", &req.WorkflowID)

	log.Printf("Device %s successfully booked by workflow %s", deviceID, req.WorkflowID)
	c.JSON(http.StatusOK, BookResponse{
		DeviceID:   deviceID,
		Status:     "busy",
		WorkflowID: req.WorkflowID,
		BookedAt:   time.Now().UTC().Format(time.RFC3339),
	})
}

func releaseDeviceHandler(c *gin.Context) {
	deviceID := c.Param("device_id")

	if _, ok := DEVICES[deviceID]; !ok {
		log.Printf("Device not found: %s", deviceID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found"})
		return
	}

	var req ReleaseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		// workflow_id is optional for release
		req.WorkflowID = ""
	}

	log.Printf("Attempting to release device %s from workflow %s", deviceID, req.WorkflowID)

	currentWorkflow, err := redisClient.Get(ctx, fmt.Sprintf("device:%s:workflow", deviceID)).Result()
	if err == nil && currentWorkflow != req.WorkflowID && req.WorkflowID != "" {
		log.Printf("Device %s is booked by another workflow", deviceID)
		c.JSON(http.StatusForbidden, gin.H{"error": "Device is booked by another workflow"})
		return
	}

	setDeviceStatus(deviceID, "available", nil)

	log.Printf("Device %s released successfully", deviceID)
	c.JSON(http.StatusOK, ReleaseResponse{
		DeviceID:   deviceID,
		Status:     "available",
		ReleasedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

func executeOperationHandler(c *gin.Context) {
	deviceID := c.Param("device_id")

	if _, ok := DEVICES[deviceID]; !ok {
		log.Printf("Device not found: %s", deviceID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Device not found"})
		return
	}

	var req ExecuteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Execute request missing required fields: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Printf("Executing operation '%s' on device %s for workflow %s", req.Operation, deviceID, req.WorkflowID)

	currentWorkflow, err := redisClient.Get(ctx, fmt.Sprintf("device:%s:workflow", deviceID)).Result()
	if err != nil || currentWorkflow != req.WorkflowID {
		log.Printf("Device %s not booked by workflow %s", deviceID, req.WorkflowID)
		c.JSON(http.StatusForbidden, gin.H{"error": "Device not booked by this workflow"})
		return
	}

	// Simulate operation execution time
	time.Sleep(500 * time.Millisecond)

	log.Printf("Operation '%s' completed on device %s", req.Operation, deviceID)
	c.JSON(http.StatusOK, ExecuteResponse{
		DeviceID:   deviceID,
		Operation:  req.Operation,
		Status:     "completed",
		ExecutedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

func initializeDevices() {
	for deviceID := range DEVICES {
		exists, err := redisClient.Exists(ctx, fmt.Sprintf("device:%s:status", deviceID)).Result()
		if err != nil || exists == 0 {
			setDeviceStatus(deviceID, "available", nil)
		}
	}
}

func main() {
	// Configure logging
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Connect to Redis
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Failed to parse Redis URL: %v", err)
	}

	redisClient = redis.NewClient(opt)

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	log.Println("Connected to Redis successfully")

	// Initialize devices
	initializeDevices()

	// Setup Gin
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// CORS configuration
	router.Use(cors.New(cors.Config{
		AllowAllOrigins: true,
		AllowMethods:    []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:    []string{"Origin", "Content-Type", "Accept"},
	}))

	// Routes
	router.GET("/health", healthHandler)
	router.GET("/devices", listDevicesHandler)
	router.GET("/devices/:device_id", getDeviceHandler)
	router.POST("/devices/:device_id/book", bookDeviceHandler)
	router.POST("/devices/:device_id/release", releaseDeviceHandler)
	router.POST("/devices/:device_id/execute", executeOperationHandler)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "5001"
	}

	log.Printf("Device service starting on port %s", port)
	if err := router.Run("0.0.0.0:" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
