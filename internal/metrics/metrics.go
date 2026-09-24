package metrics

import "github.com/prometheus/client_golang/prometheus"

var PaymentsCreated = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "payflow_payments_created_total",
		Help: "Total number of successfully created payments.",
	},
)

var HTTPRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "payflow_http_request_duration_seconds",
		Help: "Duration of HTTP requests in seconds.",
	},
	[]string{"method", "path"},
)

func init() {
	prometheus.MustRegister(PaymentsCreated)
	prometheus.MustRegister(HTTPRequestDuration)
}
