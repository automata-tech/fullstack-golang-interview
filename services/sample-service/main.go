package main

import (
	"context"
	"encoding/json"
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

const SAMPLES_KEY = "samples"

type Sample struct {
	Barcode   string   `json:"barcode"`
	Name      string   `json:"name"`
	Type      string   `json:"type"`
	Location  Location `json:"location"`
	CreatedAt string   `json:"created_at"`
	UpdatedAt string   `json:"updated_at,omitempty"`
}

type Location struct {
	Plate string `json:"plate"`
	Well  string `json:"well"`
}

type CreateSampleRequest struct {
	Barcode  string   `json:"barcode" binding:"required"`
	Name     string   `json:"name"`
	Type     string   `json:"type"`
	Location Location `json:"location"`
}

type UpdateLocationRequest struct {
	Location Location `json:"location" binding:"required"`
}

type ValidateRequest struct {
	Barcodes []string `json:"barcodes" binding:"required"`
}

type ValidationResult struct {
	Barcode string `json:"barcode"`
	Exists  bool   `json:"exists"`
}

func getAllSamples() (map[string]Sample, error) {
	samplesData, err := redisClient.Get(ctx, SAMPLES_KEY).Result()
	if err == redis.Nil {
		return make(map[string]Sample), nil
	}
	if err != nil {
		return nil, err
	}

	var samples map[string]Sample
	if err := json.Unmarshal([]byte(samplesData), &samples); err != nil {
		return nil, err
	}

	return samples, nil
}

func saveSamples(samples map[string]Sample) error {
	data, err := json.Marshal(samples)
	if err != nil {
		return err
	}

	return redisClient.Set(ctx, SAMPLES_KEY, data, 0).Err()
}

func initializeSamples() error {
	samples := map[string]Sample{
		"SAMPLE001": {
			Barcode: "SAMPLE001",
			Name:    "Blood Sample A",
			Type:    "blood",
			Location: Location{
				Plate: "PLATE-01",
				Well:  "A1",
			},
			CreatedAt: "2025-01-15T10:00:00Z",
		},
		"SAMPLE002": {
			Barcode: "SAMPLE002",
			Name:    "Tissue Sample B",
			Type:    "tissue",
			Location: Location{
				Plate: "PLATE-01",
				Well:  "A2",
			},
			CreatedAt: "2025-01-15T10:05:00Z",
		},
		"SAMPLE003": {
			Barcode: "SAMPLE003",
			Name:    "Saliva Sample C",
			Type:    "saliva",
			Location: Location{
				Plate: "PLATE-02",
				Well:  "B1",
			},
			CreatedAt: "2025-01-15T10:10:00Z",
		},
	}

	return saveSamples(samples)
}

func healthHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":  "healthy",
		"service": "sample-service",
	})
}

func listSamplesHandler(c *gin.Context) {
	samples, err := getAllSamples()
	if err != nil {
		log.Printf("Error getting samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve samples"})
		return
	}

	// Convert map to array with consistent ordering
	sampleList := make([]Sample, 0, len(samples))
	for _, sample := range samples {
		sampleList = append(sampleList, sample)
	}

	// Sort by barcode for consistent ordering
	sort.Slice(sampleList, func(i, j int) bool {
		return sampleList[i].Barcode < sampleList[j].Barcode
	})

	c.JSON(http.StatusOK, sampleList)
}

func getSampleHandler(c *gin.Context) {
	barcode := c.Param("barcode")

	samples, err := getAllSamples()
	if err != nil {
		log.Printf("Error getting samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve samples"})
		return
	}

	sample, ok := samples[barcode]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Sample not found"})
		return
	}

	c.JSON(http.StatusOK, sample)
}

func createSampleHandler(c *gin.Context) {
	var req CreateSampleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Sample creation missing barcode: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "barcode is required"})
		return
	}

	samples, err := getAllSamples()
	if err != nil {
		log.Printf("Error getting samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve samples"})
		return
	}

	if _, exists := samples[req.Barcode]; exists {
		log.Printf("Sample already exists: %s", req.Barcode)
		c.JSON(http.StatusConflict, gin.H{"error": "Sample already exists"})
		return
	}

	log.Printf("Creating sample: %s", req.Barcode)

	sample := Sample{
		Barcode:   req.Barcode,
		Name:      req.Name,
		Type:      req.Type,
		Location:  req.Location,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	samples[req.Barcode] = sample
	if err := saveSamples(samples); err != nil {
		log.Printf("Error saving samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save sample"})
		return
	}

	log.Printf("Sample %s created successfully", req.Barcode)
	c.JSON(http.StatusCreated, sample)
}

func updateSampleLocationHandler(c *gin.Context) {
	barcode := c.Param("barcode")

	samples, err := getAllSamples()
	if err != nil {
		log.Printf("Error getting samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve samples"})
		return
	}

	sample, ok := samples[barcode]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Sample not found"})
		return
	}

	var req UpdateLocationRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "location is required"})
		return
	}

	sample.Location = req.Location
	sample.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	samples[barcode] = sample

	if err := saveSamples(samples); err != nil {
		log.Printf("Error saving samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update sample"})
		return
	}

	c.JSON(http.StatusOK, sample)
}

func validateSamplesHandler(c *gin.Context) {
	var req ValidateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Printf("Validation request missing barcodes: %v", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "barcodes array is required"})
		return
	}

	log.Printf("Validating %d sample(s)", len(req.Barcodes))

	samples, err := getAllSamples()
	if err != nil {
		log.Printf("Error getting samples: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve samples"})
		return
	}

	results := make([]ValidationResult, len(req.Barcodes))
	for i, barcode := range req.Barcodes {
		_, exists := samples[barcode]
		results[i] = ValidationResult{
			Barcode: barcode,
			Exists:  exists,
		}
		if !exists {
			log.Printf("Sample not found: %s", barcode)
		}
	}

	c.JSON(http.StatusOK, results)
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

	// Initialize sample data if not exists
	existingSamples, err := getAllSamples()
	if err != nil {
		log.Fatalf("Failed to check existing samples: %v", err)
	}
	if len(existingSamples) == 0 {
		if err := initializeSamples(); err != nil {
			log.Fatalf("Failed to initialize samples: %v", err)
		}
		log.Println("Initialized sample data")
	}

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
	router.GET("/samples", listSamplesHandler)
	router.GET("/samples/:barcode", getSampleHandler)
	router.POST("/samples", createSampleHandler)
	router.PUT("/samples/:barcode/location", updateSampleLocationHandler)
	router.POST("/samples/validate", validateSamplesHandler)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "5002"
	}

	log.Printf("Sample service starting on port %s", port)
	if err := router.Run("0.0.0.0:" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
