package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	watchNamespaces []string
	port            string
	scrapeInterval  time.Duration
	once            sync.Once
)

const defaultKubeconfig = `apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCakNDQWU2Z0F3SUJBZ0lCQVRBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwdGFXNXAKYTNWaVpVTkJNQjRYRFRJMk1ESXdNakV4TkRNek9Wb1hEVE0yTURJd01URXhORE16T1Zvd0ZURVRNQkVHQTFVRQpBeE1LYldsdWFXdDFZbVZEUVRDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBS1RpCjdzY3h3cDROZVVObE9INFhnUXRmeW0vSmxxaWpaTFM4a2FiWHYwaEQ2b2hJRjB6MzZsTlVkWWM4R0hIZlM3eWUKUWtMQlhsem9hZHZ3RUdUWS85bVpHYzk4WDFCWUpwRXQ2aUg1UmZENzdSKzZUaUJTRklQUzQrTFJvSDVXRm4yZwpoUW14VnBHQkRLSVBCeGNkRTdzTjJXL0txN3JOQ1l4cmN0YUNkVTZHYlZHY2FDOXJlVTgxYkdJOHUyUlV5akEzCm52cjdOOFFuNHIyaURuQTJjUjgxcWZPakdBZXE5NTd1dlJHVW5BNGx3VTM1enUxMGVyWlhLcHFlR0hkUzVzNTQKWFlKRG1oSGlRQVVUeXRKcWVXY2JRa1lhNHFrcURpNGk1Vy9DUWlQTmYvVzZwODNyMmpiZ0IrZHo1cHBoZG9EZApnbHZxSFJBc0JrbjYvZm11NmZrQ0F3RUFBYU5oTUY4d0RnWURWUjBQQVFIL0JBUURBZ0trTUIwR0ExVWRKUVFXCk1CUUdDQ3NHQVFVRkJ3TUNCZ2dyQmdFRkJRY0RBVEFQQmdOVkhSTUJBZjhFQlRBREFRSC9NQjBHQTFVZERnUVcKQkJSNmxwVFpHMnRueVg5MGF3dnJhaEFaU3I3bUxUQU5CZ2txaGtpRzl3MEJBUXNGQUFPQ0FRRUFiWTFkdUhwdQpoaFJ6Nk1PZ21xL1FSWmRUSmptRmRiYjRBbitwTGRyS2dtVTI2UWx4OUwzZnNwb1lTRmJFekZwVk1CK2VWU2FOCkxObzViRUhFYVJTcllUV1ZQam5wcVRiTUlmY0w5bWhpSEdFcTFGVmRPYUlaZXdTdmJKUHJYN1FWVnFkc1JhTEwKRVJxZ1V6NDBZcXprV1NCdlpKQ2crRndDWmFSd0VWYWxJMDJGMm9TK0c0ZUp4VGhnT2E2YkRPMDFwbmpSSElTRQpVMzdlM2JORXBaL2FJa2FreVcvb09RSEhuM2R6SXJhN0xwM0svQXB5T0FtQ21lemZvRDcwYnE4UmVzVUF2V1dGCmJ6bFpucENNN09wai9CdXFKdS9RYWJ2OHhYWk9tUzlOc0VEekNYaUJqeHgrQUZqaFQ4OXpXbFRuRTdiSW1OTzYKTUp2NjBUS1BkWmFpWXc9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    server: https://192.168.49.2:8443
  name: minikube
contexts:
- context:
    cluster: minikube
    user: minikube
    namespace: default
  name: minikube
current-context: minikube
kind: Config
preferences: {}
users:
- name: minikube
  user:
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURJVENDQWdtZ0F3SUJBZ0lCQWpBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwdGFXNXAKYTNWaVpVTkJNQjRYRFRJMk1ESXdNakV4TkRNek9Wb1hEVEk1TURJd01qRXhORE16T1Zvd01URVhNQlVHQTFVRQpDaE1PYzNsemRHVnRPbTFoYzNSbGNuTXhGakFVQmdOVkJBTVREVzFwYm1scmRXSmxMWFZ6WlhJd2dnRWlNQTBHCkNTcUdTSWIzRFFFQkFRVUFBNElCRHdBd2dnRUtBb0lCQVFERlJ0eDQ4NGwvU1JDa29IbWswR1VENTJFamtET2QKeGxmRERHYkxQckxGTytQTmUvQi9DQlJFK1ZnUytQaFdwQ3JsSkRPMTUwUTRtdlpUZWRxWGd5bTdrNWJYOWhBQQoxOFd5dmZTdjJYdkNpK1Ywa3JJUDJJSi9KQ0x4TWpSaWNYTUwxTlhvb3BKYzBtQ3g5cEJzaEU2N0pucFAwWkZYCmlwSGxKbW9rVWZyUE90Y1NFRmhQbW85TUF5V0VFT3dJaTNGMXR2VEtxZE41dlozbWlNWDhJcEFWUFJ6d2FVMHcKUVFFaWVhZG94TGh3WHVtU1pLd0FnelB2cTJDU0xaT1l0S3VxK1YrTHBla3I0eWduSnJBNzhCaGloY25rRnF4NwpIMkNuUmhXS0pzaGIzSWFjV0FmK0Vja2gxZ2d6bGJDYjFHcHNJdGt6MzlsczhGak1FZVAvU25LSkFnTUJBQUdqCllEQmVNQTRHQTFVZER3RUIvd1FFQXdJRm9EQWRCZ05WSFNVRUZqQVVCZ2dyQmdFRkJRY0RBUVlJS3dZQkJRVUgKQXdJd0RBWURWUjBUQVFIL0JBSXdBREFmQmdOVkhTTUVHREFXZ0JSNmxwVFpHMnRueVg5MGF3dnJhaEFaU3I3bQpMVEFOQmdrcWhraUc5dzBCQVFzRkFBT0NBUUVBUWthUFhLWkVOK1ZFMkx2TEdHU0VJN2R5VEI1VklsNk5OYi9iCmNFcDdaSThXOGN5cHNUQm04TkZ1Q2tzb1dTVm1EQ1pDSDBRR1lSemI1bHE0T3A1UGRZcW5Gc1BVY21tSlYyV1kKK1hrQ0wzWDBxRlNZMXlFbG4rMnRzN2ltaXo0ZzZkRi9EWG4rY3NjdTRBSXZqVHFqMVNuUE5KQS9yLzJBdlJORQpmVHhsbWNrRUFMVkc3UXdjV3ZDWDdId0VqRFFDRUNIcHIrMzA3L01mTHRSMEkxckxsdjcyeW14U0dQS2c5Q3hxCnp3OWdPU0FmVU5FV0RGRjR1UEI4SnQxL2I0VVhSZEJrZWYzd3dSZWkvSTFvdmFQUjN5VHg4SC9lVXhwaEhrWWsKWERhNGxVc25YV1ZCQzg1Q3NKVXJFK1VCdjRDMldOYng5ZVBzaE1WNVBHd295Q05GdGc9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb3dJQkFBS0NBUUVBeFViY2VQT0pmMGtRcEtCNXBOQmxBK2RoSTVBem5jWlh3d3hteXo2eXhUdmp6WHZ3CmZ3Z1VSUGxZRXZqNFZxUXE1U1F6dGVkRU9KcjJVM25hbDRNcHU1T1cxL1lRQU5mRnNyMzByOWw3d292bGRKS3kKRDlpQ2Z5UWk4VEkwWW5GekM5VFY2S0tTWE5KZ3NmYVFiSVJPdXlaNlQ5R1JWNHFSNVNacUpGSDZ6enJYRWhCWQpUNXFQVEFNbGhCRHNDSXR4ZGJiMHlxblRlYjJkNW9qRi9DS1FGVDBjOEdsTk1FRUJJbm1uYU1TNGNGN3BrbVNzCkFJTXo3NnRna2kyVG1MU3JxdmxmaTZYcEsrTW9KeWF3Ty9BWVlvWEo1QmFzZXg5Z3AwWVZpaWJJVzl5R25GZ0gKL2hISklkWUlNNVd3bTlScWJDTFpNOS9aYlBCWXpCSGovMHB5aVFJREFRQUJBb0lCQUNlME9VOUdoSmZQbHIvcgpaRkFkZVJjdURFamlEdUZrTittVHAyU2tlOHBpWVZqTDV2MUtIUG84ek5NVXRMYUxWKzdDT0g0Vnk0OHc4UDZmCitiU2d1MWQ3UHRLOFBVQk9MUVhxWVVLN0hNTnM4SU5qdXQ2aGpySVVEY3hKZEcyVHM3bmYzaVZ5QXM4WHNFcGcKKzNRN3RMVEo2N2dBejZXMHgrUTh0UVFXVThvOURPeDNoZHVyb2NJbllEaTRMazNpSm80UjNUbUlHcnRWWVNwcQoxQzhHOVQ5TWVBYTZqWDJZK1NZei9hVFh3UGxadk1GdjdZUHBLNUNXMWE4RGhDQXE1aUZpeUlzeDNpcU9Cb3JlCm5PUU1waXMxbmw3YlFPcGZ0eGJ1cGJqRkJhNUI4djRGWmxIUURIa2Y1MVNVQUwvMnh6UU5saEFKaDhxVHlmSUUKaHN0WlI1a0NnWUVBMjZGMkw0OGkwQnlET0ltVUVoV2htL2xXaHFXa3dmT3lzUzRSUE1aenZLOXN1NkcyeVJzKwpmWUc4djVISkNXNUJPb0ZwQ0l3cHlPRi8xUFZ1QmFCai9RQ3Q3eW8ra1JMRFlFS2c3RGdnZk55ajRGekhNVGUvCk5VUURhbVNtM01tYW41Y05UQkwva1Z6STBhNWgyNTU2blIvZHRma2w0bm4yc0RBcEZlbWMrbHNDZ1lFQTVmSEgKQnBISHpKVG9RbC9vMGprV0U3NlhRUU1IWkhLL2toWU9Ob2diMGNNczBUSEVhQTJXTXc4bXBabnZhOEZhMjVzVQo0OXNCK1Jlc0xqaUpuWURHNEFrSHFsWEtFYUwxNmh5MUlKNjlXL0ZwRlFpVVR4aXFGb2ZBOTZDNjZlcUkvYzZYClBIOFRCd2dCRGVoNG9SZHg5bU9wWEpIaERwWTF4c1lZT1ZiWnMrc0NnWUFxVGwvRnFYeTdPY0xORVRORWlJWW8KMVU2bGdTTExlWFhpUzAxbXQ1Tnp0UmJzemFtMzgxZUdOWWQySDA3cVVpS2VjbThaQm1iR0d5blVpN0kxd3o5LwpiTElVYjc2OWt5K3ZTeVpVV2p0bjBkaC9UMS9QU3oyNXRQQXpmay9tRjYrQkxrZVJiOWRxMk1TV0gxRWFUTnl4Cmg0SGRtN0NBZjUzVk1uRzNsdGgySVFLQmdRQ3VZcXhUMlI4emtnS0t3LzNuNEk5VHJnazdyclplZ1grenBMSm0Kdk5hTVFINnVzQldKN0RQcXlTVEFGbnd5dGxMWGxVZEVmb1dDaVdkMUxqOS9pWGhKMDg5U2FQbDBZcWdwUWxoRApRdC9NNk1xT3Z4RHE0NE9xem8yVHZ2dkNCcktaK2FGTXFmcWVMSDNRTkd1M2ovWkhxOUYzZU5LN28wTnBXalpvCjFlc0l2UUtCZ0dzc3dRMlZRZm9Vbm03ZmwyVTJXeENwNXJKZzg3blRzK2xUOUR4T01xejJqTVpSK1FudlFwZ20KSVhieklmc1BDTHUrMUtteTVHUW9adkRLWW54S3FBdkxjcTZ3N0E4WGFWck11U29rZWYxbUE4SEZnL210eHNIaQp3NElWWXJkYzNVS01mY0tjaTR0cjk0RzdKaXdTejUzRGhIMDFWdjg3WjgvaXJkYkVvUXdQCi0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==
`

func initConfig() {
	once.Do(func() {
		watchNs := os.Getenv("WATCH_NAMESPACES")
		if watchNs == "" {
			watchNamespaces = []string{"default"}
		} else {
			watchNamespaces = strings.Split(watchNs, ",")
		}

		port = os.Getenv("PORT")
		if port == "" {
			port = "9302"
		}

		interval := os.Getenv("SCRAPE_INTERVAL")
		if interval == "" {
			scrapeInterval = 30 * time.Second
		} else {
			var err error
			scrapeInterval, err = time.ParseDuration(interval)
			if err != nil {
				log.Printf("Invalid SCRAPE_INTERVAL, using default 30s: %v", err)
				scrapeInterval = 30 * time.Second
			}
		}
	})
}

// ===== Prometheus Metrics =====
var (
	sparkServiceStateTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_state_transitions_total",
			Help: "Spark service state transition events (one point per state change)",
		},
		[]string{"service_id", "service_name", "namespace", "app_name", "state"},
	)

	sparkServiceProcessingTime = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_processing_time_seconds_total",
			Help: "Spark processing time recorded at completion (seconds)",
		},
		[]string{"service_id", "service_name", "namespace", "app_name", "status"},
	)

	sparkCrStatusCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "spark_cr_count",
			Help: "Count of Spark CRs by status",
		},
		[]string{"status", "namespace"},
	)

	sparkServicePendingDuration = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_pending_duration_seconds_total",
			Help: "Pending duration recorded at completion (seconds)",
		},
		[]string{"service_id", "service_name", "namespace", "app_name"},
	)

	sparkServiceCores = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_cores_total",
			Help: "Total CPU cores used by service (recorded at completion)",
		},
		[]string{"service_id", "service_name", "namespace", "app_name"},
	)

	sparkServiceMemory = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_service_memory_bytes_total",
			Help: "Total memory used by service in bytes (recorded at completion)",
		},
		[]string{"service_id", "service_name", "namespace", "app_name"},
	)

	scrapeErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "spark_exporter_errors_total",
			Help: "Total number of scrape errors",
		},
		[]string{"operation"},
	)
)

func init() {
	initConfig()

	prometheus.MustRegister(sparkServiceStateTransitions)
	prometheus.MustRegister(sparkServiceProcessingTime)
	prometheus.MustRegister(sparkCrStatusCount)
	prometheus.MustRegister(sparkServicePendingDuration)
	prometheus.MustRegister(sparkServiceCores)
	prometheus.MustRegister(sparkServiceMemory)
	prometheus.MustRegister(scrapeErrors)

	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type SparkServiceData struct {
	ServiceID       string
	ServiceName     string
	AppName         string
	Namespace       string
	Status          string
	PreviousStatus  string
	ProcessingTime  float64
	PendingDuration float64
	Cores           float64
	Memory          int64
	LastUpdateTime  time.Time
	Recorded        bool
}

var sparkServiceStore = make(map[string]*SparkServiceData)
var sparkServiceMutex sync.RWMutex

func getSparkServiceID(obj *unstructured.Unstructured) string {
	labels := obj.GetLabels()
	if labels == nil {
		return "unknown"
	}
	if val, ok := labels["service-id"]; ok && val != "" {
		return val
	}
	return obj.GetName()
}

func getSparkServiceName(obj *unstructured.Unstructured) string {
	labels := obj.GetLabels()
	if labels == nil {
		return "unknown"
	}
	if val, ok := labels["service-name"]; ok && val != "" {
		return val
	}
	return "unknown"
}

func getSparkAppName(obj *unstructured.Unstructured) string {
	labels := obj.GetLabels()
	if labels == nil {
		return "unknown"
	}
	if val, ok := labels["app-name"]; ok && val != "" {
		return val
	}
	return obj.GetName()
}

func getSparkStatus(obj *unstructured.Unstructured) (state string, submissionTime, terminationTime string, err error) {
	status, found, err := unstructured.NestedFieldNoCopy(obj.Object, "status")
	if err != nil || !found {
		return "", "", "", fmt.Errorf("status not found")
	}

	statusMap, ok := status.(map[string]interface{})
	if !ok {
		return "", "", "", fmt.Errorf("invalid status format")
	}

	if appState, ok := statusMap["applicationState"].(map[string]interface{}); ok {
		if s, ok := appState["state"].(string); ok {
			state = s
		}
	}

	if t, ok := statusMap["lastSubmissionAttemptTime"].(string); ok {
		submissionTime = t
	}

	if t, ok := statusMap["terminationTime"].(string); ok {
		terminationTime = t
	}

	return state, submissionTime, terminationTime, nil
}

func calculateProcessingTime(submissionTime, terminationTime string) float64 {
	if submissionTime == "" || terminationTime == "" {
		return 0
	}

	submission, err := time.Parse(time.RFC3339Nano, submissionTime)
	if err != nil {
		return 0
	}

	termination, err := time.Parse(time.RFC3339Nano, terminationTime)
	if err != nil {
		return 0
	}

	return termination.Sub(submission).Seconds()
}

func extractResourceInfo(obj *unstructured.Unstructured) (cores float64, memory int64) {
	spec := obj.Object["spec"].(map[string]interface{})

	if driver, ok := spec["driver"].(map[string]interface{}); ok {
		if c, ok := driver["cores"]; ok {
			cores += parseFloat(c)
		}
		if m, ok := driver["memory"].(string); ok {
			memory += parseMemory(m)
		}
	}

	if executor, ok := spec["executor"].(map[string]interface{}); ok {
		if c, ok := executor["cores"]; ok {
			coresPerExecutor := parseFloat(c)
			if instances, ok := executor["instances"]; ok {
				instancesCount := parseInt(instances)
				cores += coresPerExecutor * float64(instancesCount)
			}
		}
		if m, ok := executor["memory"].(string); ok {
			memPerExecutor := parseMemory(m)
			if instances, ok := executor["instances"]; ok {
				instancesCount := parseInt(instances)
				memory += memPerExecutor * int64(instancesCount)
			}
		}
	}

	return cores, memory
}

func parseFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}

func parseInt(v interface{}) int {
	switch val := v.(type) {
	case float64:
		return int(val)
	case int64:
		return int(val)
	case int:
		return val
	case string:
		i, err := strconv.Atoi(val)
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}

func parseMemory(memStr string) int64 {
	memStr = strings.TrimSpace(memStr)
	memStr = strings.ToLower(memStr)

	var multiplier int64 = 1
	var numStr string

	if strings.HasSuffix(memStr, "mi") {
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(memStr, "mi")
	} else if strings.HasSuffix(memStr, "gi") {
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(memStr, "gi")
	} else if strings.HasSuffix(memStr, "m") {
		multiplier = 1000 * 1000
		numStr = strings.TrimSuffix(memStr, "m")
	} else if strings.HasSuffix(memStr, "g") {
		multiplier = 1000 * 1000 * 1000
		numStr = strings.TrimSuffix(memStr, "g")
	} else {
		numStr = memStr
	}

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0
	}

	return int64(num * float64(multiplier))
}

func isTerminalState(state string) bool {
	return state == "COMPLETED" || state == "FAILED"
}

func updateSparkMetrics(obj *unstructured.Unstructured) {
	namespace := obj.GetNamespace()
	name := obj.GetName()

	state, submissionTime, terminationTime, err := getSparkStatus(obj)
	if err != nil {
		scrapeErrors.WithLabelValues("get_status").Inc()
		log.Printf("Error getting status for %s/%s: %v", namespace, name, err)
		return
	}

	serviceID := getSparkServiceID(obj)
	serviceName := getSparkServiceName(obj)
	appName := getSparkAppName(obj)
	cores, memory := extractResourceInfo(obj)

	sparkServiceMutex.Lock()
	defer sparkServiceMutex.Unlock()

	storeKey := fmt.Sprintf("%s/%s/%s", namespace, serviceID, name)

	data, exists := sparkServiceStore[storeKey]
	if !exists {
		data = &SparkServiceData{
			ServiceID:      serviceID,
			ServiceName:    serviceName,
			AppName:        appName,
			Namespace:      namespace,
			PreviousStatus: "",
			LastUpdateTime: time.Now(),
		}
		sparkServiceStore[storeKey] = data

		sparkServiceStateTransitions.WithLabelValues(serviceID, serviceName, namespace, appName, state).Inc()
		log.Printf("Initial state for service %s (app: %s): %s", serviceID, appName, state)
	}

	if data.PreviousStatus != state {
		sparkServiceStateTransitions.WithLabelValues(serviceID, serviceName, namespace, appName, state).Inc()
		log.Printf("State transition for service %s (app: %s): %s -> %s", serviceID, appName, data.PreviousStatus, state)
		data.PreviousStatus = state
	}

	data.Status = state
	data.Cores = cores
	data.Memory = memory
	data.LastUpdateTime = time.Now()

	if isTerminalState(state) && !data.Recorded {
		data.ProcessingTime = calculateProcessingTime(submissionTime, terminationTime)
		data.PendingDuration = 2.0

		labelValues := []string{serviceID, serviceName, namespace, appName}
		statusLabelValues := []string{serviceID, serviceName, namespace, appName, state}

		sparkServiceProcessingTime.WithLabelValues(statusLabelValues...).Add(data.ProcessingTime)
		sparkServicePendingDuration.WithLabelValues(labelValues...).Add(data.PendingDuration)
		sparkServiceCores.WithLabelValues(labelValues...).Add(cores)
		sparkServiceMemory.WithLabelValues(labelValues...).Add(float64(memory))

		data.Recorded = true

		log.Printf("Recorded metrics for service %s (app: %s): state=%s, processing_time=%.2fs, cores=%.2f, memory=%d bytes",
			serviceID, appName, state, data.ProcessingTime, cores, memory)
	}

	updateCrCountMetrics()
}

func updateCrCountMetrics() {
	statusCount := make(map[string]int)
	namespaceCount := make(map[string]map[string]int)

	sparkServiceMutex.RLock()
	defer sparkServiceMutex.RUnlock()

	for _, data := range sparkServiceStore {
		statusCount[data.Status]++
		if namespaceCount[data.Namespace] == nil {
			namespaceCount[data.Namespace] = make(map[string]int)
		}
		namespaceCount[data.Namespace][data.Status]++
	}

	for status, count := range statusCount {
		sparkCrStatusCount.WithLabelValues(status, "").Set(float64(count))
	}

	for ns, statusMap := range namespaceCount {
		for status, count := range statusMap {
			sparkCrStatusCount.WithLabelValues(status, ns).Set(float64(count))
		}
	}
}

func handleSparkEvent(eventType watch.EventType, obj *unstructured.Unstructured) {
	namespace := obj.GetNamespace()
	name := obj.GetName()
	serviceID := getSparkServiceID(obj)

	switch eventType {
	case watch.Added, watch.Modified:
		updateSparkMetrics(obj)

	case watch.Deleted:
		sparkServiceMutex.Lock()
		defer sparkServiceMutex.Unlock()

		storeKey := fmt.Sprintf("%s/%s/%s", namespace, serviceID, name)
		if data, exists := sparkServiceStore[storeKey]; exists {
			labelValues := prometheus.Labels{
				"service_id":   serviceID,
				"service_name": data.ServiceName,
				"namespace":    namespace,
				"app_name":     data.AppName,
			}
			statusLabelValues := prometheus.Labels{
				"service_id":   serviceID,
				"service_name": data.ServiceName,
				"namespace":    namespace,
				"app_name":     data.AppName,
				"state":        data.Status,
			}
			completedLabelValues := prometheus.Labels{
				"service_id":   serviceID,
				"service_name": data.ServiceName,
				"namespace":    namespace,
				"app_name":     data.AppName,
				"status":       data.Status,
			}

			sparkServiceStateTransitions.Delete(statusLabelValues)
			sparkServiceProcessingTime.Delete(completedLabelValues)
			sparkServicePendingDuration.Delete(labelValues)
			sparkServiceCores.Delete(labelValues)
			sparkServiceMemory.Delete(labelValues)

			delete(sparkServiceStore, storeKey)
			log.Printf("Deleted metrics for service %s (app: %s)", serviceID, data.AppName)

			updateCrCountMetrics()
		}
	}
}

func watchSparkNamespace(ctx context.Context, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespace string, stopCh <-chan struct{}) {
	watcher, err := dynamicClient.Resource(gvr).Namespace(namespace).Watch(ctx, metav1.ListOptions{})
	if err != nil {
		scrapeErrors.WithLabelValues("watch").Inc()
		log.Printf("Error watching namespace %s: %v", namespace, err)
		return
	}
	defer watcher.Stop()

	log.Printf("Watching Spark applications in namespace: %s", namespace)

	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				log.Printf("Watcher closed for namespace %s", namespace)
				return
			}
			if obj, ok := event.Object.(*unstructured.Unstructured); ok {
				handleSparkEvent(event.Type, obj)
			}
		case <-stopCh:
			log.Printf("Stopping watcher for namespace %s", namespace)
			return
		}
	}
}

func scanSparkApps(dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespace string) {
	list, err := dynamicClient.Resource(gvr).Namespace(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		scrapeErrors.WithLabelValues("list").Inc()
		log.Printf("Error listing Spark apps in namespace %s: %v", namespace, err)
		return
	}

	log.Printf("Scanning %d Spark applications in namespace: %s", len(list.Items), namespace)

	for _, item := range list.Items {
		handleSparkEvent(watch.Added, &item)
	}
}

func getDynamicClient() (dynamic.Interface, error) {
	config, err := clientcmd.RESTConfigFromKubeConfig([]byte(defaultKubeconfig))
	if err != nil {
		return nil, fmt.Errorf("failed to create kubeconfig: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return dynamicClient, nil
}

func main() {
	log.Printf("Starting Spark CR Exporter v1")
	log.Printf("Namespaces: %v", watchNamespaces)
	log.Printf("Port: %s", port)

	dynamicClient, err := getDynamicClient()
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Spark CR Exporter v1 (Counter-based)")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Metrics: /metrics")
		fmt.Fprintf(w, "Namespaces: %v\n", watchNamespaces)
		fmt.Fprintf(w, "Port: %s\n", port)
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Available Metrics (All Counters - points on timeline):")
		fmt.Fprintln(w, "  spark_service_state_transitions_total{service_id, service_name, namespace, app_name, state}")
		fmt.Fprintln(w, "    - Records each state change (RUNNING, COMPLETED, FAILED, etc)")
		fmt.Fprintln(w, "  spark_service_processing_time_seconds_total{service_id, service_name, namespace, app_name, status}")
		fmt.Fprintln(w, "    - Processing time recorded at completion (seconds)")
		fmt.Fprintln(w, "  spark_service_pending_duration_seconds_total{service_id, service_name, namespace, app_name}")
		fmt.Fprintln(w, "    - Pending duration recorded at completion (seconds)")
		fmt.Fprintln(w, "  spark_service_cores_total{service_id, service_name, namespace, app_name}")
		fmt.Fprintln(w, "    - Total CPU cores used (recorded at completion)")
		fmt.Fprintln(w, "  spark_service_memory_bytes_total{service_id, service_name, namespace, app_name}")
		fmt.Fprintln(w, "    - Total memory used in bytes (recorded at completion)")
		fmt.Fprintln(w, "  spark_cr_count{status, namespace}")
		fmt.Fprintln(w, "    - Current count of Spark CRs by status (Gauge)")
		fmt.Fprintln(w, "  spark_exporter_errors_total{operation}")
		fmt.Fprintf(w, "\nTracked services: %d\n", len(sparkServiceStore))
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})

	go func() {
		log.Printf("Server listening on :%s", port)
		if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	log.Printf("HTTP server goroutine started, waiting for signals...")

	stopCh := make(chan struct{})
	defer close(stopCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sparkAppGVR := schema.GroupVersionResource{
		Group:    "sparkoperator.k8s.io",
		Version:  "v1beta2",
		Resource: "sparkapplications",
	}

	for _, ns := range watchNamespaces {
		scanSparkApps(dynamicClient, sparkAppGVR, ns)
	}

	for _, ns := range watchNamespaces {
		go watchSparkNamespace(ctx, dynamicClient, sparkAppGVR, ns, stopCh)
	}

	log.Printf("Spark watchers started for %d namespaces", len(watchNamespaces))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}
