package metrics

import "github.com/prometheus/client_golang/prometheus"

var PaymentsCreated = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "payflow_payments_created_total",
		Help: "Total number of successfully created payments.",
	},
)

func init() {
	prometheus.MustRegister(PaymentsCreated)
}
