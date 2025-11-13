package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	redisClient *redis.Client
	ctx         = context.Background()
)

const WORKFLOWS_KEY = "workflows"

type WorkflowStatus string

const (
	StatusCreated   WorkflowStatus = "created"
	StatusRunning   WorkflowStatus = "running"
	StatusCompleted WorkflowStatus = "completed"
	StatusPaused    WorkflowStatus = "paused"
)

type Workflow struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	DeviceID       string         `json:"device_id"`
	SampleBarcodes []string       `json:"sample_barcodes"`
	Steps          []string       `json:"steps"`
	Status         WorkflowStatus `json:"status"`
	CreatedAt      string         `json:"created_at"`
	StartedAt      string         `json:"started_at,omitempty"`
	CompletedAt    string         `json:"completed_at,omitempty"`
}

type CreateWorkflowRequest struct {
	Name           string   `json:"name" binding:"required"`
	DeviceID       string   `json:"device_id" binding:"required"`
	SampleBarcodes []string `json:"sample_barcodes"`
	Steps          []string `json:"steps"`
}

type ExecuteStepRequest struct {
	StepIndex int `json:"step_index"`
}

type BookDeviceRequest struct {
	WorkflowID string `json:"workflow_id"`
}

type ReleaseDeviceRequest struct {
	WorkflowID string `json:"workflow_id"`
}

type ExecuteDeviceRequest struct {
	WorkflowID string `json:"workflow_id"`
	Operation  string `json:"operation"`
}

var (
	deviceAPIURL string
	sampleAPIURL string
)

func getAllWorkflows() (map[string]Workflow, error) {
	workflowsData, err := redisClient.Get(ctx, WORKFLOWS_KEY).Result()
	if err == redis.Nil {
		return make(map[string]Workflow), nil
	}
	if err != nil {
		return nil, err
	}

	var workflows map[string]Workflow
	if err := json.Unmarshal([]byte(workflowsData), &workflows); err != nil {
		return nil, err
	}

	return workflows, nil
}

func saveWorkflows(workflows map[string]Workflow) error {
	data, err := json.Marshal(workflows)
	if err != nil {
		return err
	}

	return redisClient.Set(ctx, WORKFLOWS_KEY, data, 0).Err()
}

func getWorkflow(workflowID string) (*Workflow, error) {
	workflows, err := getAllWorkflows()
	if err != nil {
		return nil, err
	}

	workflow, ok := workflows[workflowID]
	if !ok {
		return nil, nil
	}

	return &workflow, nil
}

func updateWorkflow(workflowID string, updates map[string]interface{}) (*Workflow, error) {
	workflows, err := getAllWorkflows()
	if err != nil {
		return nil, err
	}

	workflow, ok := workflows[workflowID]
	if !ok {
		return nil, nil
	}

	// Apply updates
	if name, ok := updates["name"].(string); ok {
		workflow.Name = name
	}
	if status, ok := updates["status"].(WorkflowStatus); ok {
		workflow.Status = status
	}
	if startedAt, ok := updates["started_at"].(string); ok {
		workflow.StartedAt = startedAt
	}
	if completedAt, ok := updates["completed_at"].(string); ok {
		workflow.CompletedAt = completedAt
	}

	workflows[workflowID] = workflow
	if err := saveWorkflows(workflows); err != nil {
		return nil, err
	}

	return &workflow, nil
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "workflow-service",
	})
}

func listWorkflowsHandler(c *gin.Context) {
	workflows, err := getAllWorkflows()
	if err != nil {
		log.Printf("Error getting workflows: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve workflows"})
		return
	}

	// Convert map to array
	workflowList := make([]Workflow, 0, len(workflows))
	for _, workflow := range workflows {
		workflowList = append(workflowList, workflow)
	}

	c.JSON(http.StatusOK, workflowList)
}

func getWorkflowHandler(c *gin.Context) {
	workflowID := c.Param("workflow_id")

	workflow, err := getWorkflow(workflowID)
	if err != nil {
		log.Printf("Error getting workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve workflow"})
		return
	}

	if workflow == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Workflow not found"})
		return
	}

	c.JSON(http.StatusOK, workflow)
}

func createWorkflowHandler(c *gin.Context) {
	var req CreateWorkflowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name and device_id are required"})
		return
	}

	workflowID := uuid.New().String()

	log.Printf("Creating workflow: %s (ID: %s) for device: %s", req.Name, workflowID, req.DeviceID)

	workflow := Workflow{
		ID:             workflowID,
		Name:           req.Name,
		DeviceID:       req.DeviceID,
		SampleBarcodes: req.SampleBarcodes,
		Steps:          req.Steps,
		Status:         StatusCreated,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	workflows, err := getAllWorkflows()
	if err != nil {
		log.Printf("Error getting workflows: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create workflow"})
		return
	}

	workflows[workflowID] = workflow
	if err := saveWorkflows(workflows); err != nil {
		log.Printf("Error saving workflows: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create workflow"})
		return
	}

	log.Printf("Workflow %s created successfully", workflowID)
	c.JSON(http.StatusCreated, workflow)
}

func startWorkflowHandler(c *gin.Context) {
	workflowID := c.Param("workflow_id")

	log.Printf("Starting workflow: %s", workflowID)

	workflow, err := getWorkflow(workflowID)
	if err != nil {
		log.Printf("Error getting workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve workflow"})
		return
	}

	if workflow == nil {
		log.Printf("Workflow not found: %s", workflowID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Workflow not found"})
		return
	}

	if workflow.Status != StatusCreated {
		log.Printf("Workflow %s already started or completed", workflowID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Workflow already started or completed"})
		return
	}

	deviceID := workflow.DeviceID
	log.Printf("Booking device %s for workflow %s", deviceID, workflowID)

	// Intentional bug: wrong endpoint (should be /book, not /reserve)
	bookURL := fmt.Sprintf("%s/device/%s/reserve", deviceAPIURL, deviceID)
	bookReq := BookDeviceRequest{WorkflowID: workflowID}
	bookBody, _ := json.Marshal(bookReq)

	resp, err := http.Post(bookURL, "application/json", bytes.NewBuffer(bookBody))
	if err != nil {
		log.Printf("Error communicating with device service: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to communicate with device service: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Failed to book device %s: %d - %s", deviceID, resp.StatusCode, string(body))

		var errorResp map[string]interface{}
		json.Unmarshal(body, &errorResp)

		c.JSON(resp.StatusCode, gin.H{
			"error":   "Failed to book device",
			"details": errorResp,
		})
		return
	}

	// Update workflow status
	_, err = updateWorkflow(workflowID, map[string]interface{}{
		"status":     StatusRunning,
		"started_at": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		log.Printf("Error updating workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update workflow"})
		return
	}

	// Get updated workflow
	workflow, _ = getWorkflow(workflowID)

	log.Printf("Workflow %s started successfully", workflowID)
	c.JSON(http.StatusOK, workflow)
}

func completeWorkflowHandler(c *gin.Context) {
	workflowID := c.Param("workflow_id")

	log.Printf("Completing workflow: %s", workflowID)

	workflow, err := getWorkflow(workflowID)
	if err != nil {
		log.Printf("Error getting workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve workflow"})
		return
	}

	if workflow == nil {
		log.Printf("Workflow not found: %s", workflowID)
		c.JSON(http.StatusNotFound, gin.H{"error": "Workflow not found"})
		return
	}

	if workflow.Status != StatusRunning {
		log.Printf("Workflow %s is not running", workflowID)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Workflow is not running"})
		return
	}

	deviceID := workflow.DeviceID
	log.Printf("Releasing device %s from workflow %s", deviceID, workflowID)

	releaseURL := fmt.Sprintf("%s/devices/%s/release", deviceAPIURL, deviceID)
	releaseReq := ReleaseDeviceRequest{WorkflowID: workflowID}
	releaseBody, _ := json.Marshal(releaseReq)

	resp, err := http.Post(releaseURL, "application/json", bytes.NewBuffer(releaseBody))
	if err != nil {
		log.Printf("Error communicating with device service: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to communicate with device service: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("Failed to release device %s: %d", deviceID, resp.StatusCode)

		var errorResp map[string]interface{}
		json.Unmarshal(body, &errorResp)

		c.JSON(resp.StatusCode, gin.H{
			"error":   "Failed to release device",
			"details": errorResp,
		})
		return
	}

	// Update workflow status
	_, err = updateWorkflow(workflowID, map[string]interface{}{
		"status":       StatusCompleted,
		"completed_at": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		log.Printf("Error updating workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update workflow"})
		return
	}

	// Get updated workflow
	workflow, _ = getWorkflow(workflowID)

	log.Printf("Workflow %s completed successfully", workflowID)
	c.JSON(http.StatusOK, workflow)
}

func executeStepHandler(c *gin.Context) {
	workflowID := c.Param("workflow_id")

	workflow, err := getWorkflow(workflowID)
	if err != nil {
		log.Printf("Error getting workflow: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve workflow"})
		return
	}

	if workflow == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Workflow not found"})
		return
	}

	if workflow.Status != StatusRunning {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Workflow is not running"})
		return
	}

	var req ExecuteStepRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		req.StepIndex = 0
	}

	steps := workflow.Steps
	if req.StepIndex >= len(steps) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid step index"})
		return
	}

	step := steps[req.StepIndex]
	deviceID := workflow.DeviceID

	executeURL := fmt.Sprintf("%s/devices/%s/execute", deviceAPIURL, deviceID)
	executeReq := ExecuteDeviceRequest{
		WorkflowID: workflowID,
		Operation:  step,
	}
	executeBody, _ := json.Marshal(executeReq)

	resp, err := http.Post(executeURL, "application/json", bytes.NewBuffer(executeBody))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to communicate with device service: %v", err)})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		var errorResp map[string]interface{}
		json.Unmarshal(body, &errorResp)

		c.JSON(resp.StatusCode, gin.H{
			"error":   "Failed to execute step",
			"details": errorResp,
		})
		return
	}

	var result map[string]interface{}
	body, _ := io.ReadAll(resp.Body)
	json.Unmarshal(body, &result)

	c.JSON(http.StatusOK, gin.H{
		"workflow_id": workflowID,
		"step_index":  req.StepIndex,
		"step":        step,
		"result":      result,
	})
}

func main() {
	// Configure logging
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Get environment variables
	deviceAPIURL = os.Getenv("DEVICE_API_URL")
	if deviceAPIURL == "" {
		log.Fatal("DEVICE_API_URL environment variable is required")
	}

	sampleAPIURL = os.Getenv("SAMPLE_API_URL")
	if sampleAPIURL == "" {
		sampleAPIURL = "http://localhost:5002"
	}

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
	router.GET("/workflows", listWorkflowsHandler)
	router.GET("/workflows/:workflow_id", getWorkflowHandler)
	router.POST("/workflows", createWorkflowHandler)
	router.POST("/workflows/:workflow_id/start", startWorkflowHandler)
	router.POST("/workflows/:workflow_id/complete", completeWorkflowHandler)
	router.POST("/workflows/:workflow_id/execute-step", executeStepHandler)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "5003"
	}

	log.Printf("Workflow service starting on port %s", port)
	if err := router.Run("0.0.0.0:" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
