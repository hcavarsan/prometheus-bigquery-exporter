package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/prometheus-bigquery-exporter/internal/setup"
	"github.com/m-lab/prometheus-bigquery-exporter/query"
	"github.com/m-lab/prometheus-bigquery-exporter/sql"

	"cloud.google.com/go/bigquery"
	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	counterSources = flagx.StringArray{}
	gaugeSources   = flagx.StringArray{}
	project        = flag.String("project", "", "GCP project name.")
	refresh        = flag.Duration("refresh", 5*time.Minute, "Interval between updating metrics.")
)

func init() {
	flag.Var(&gaugeSources, "gauge-query", "Name of file containing a gauge query.")
	*prometheusx.ListenAddress = ":9348"
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func sleepUntilNext(d time.Duration) {
	next := time.Now().Truncate(d).Add(d)
	time.Sleep(time.Until(next))
}

// Updated function for handling metric name and corresponding query extraction.
func fileToMetrics(filename string) (map[string]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	currentMetric := ""
	metrics := make(map[string]string)
	var currentQuery strings.Builder

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Ignore comments
		if strings.HasPrefix(line, "--") && !strings.HasPrefix(line, "-- MetricName:") {
			continue
		}

		if strings.HasPrefix(line, "-- MetricName:") {
			currentMetric = strings.TrimPrefix(line, "-- MetricName:")
			currentMetric = strings.TrimSpace(currentMetric)
		} else if len(line) > 0 && line != "" {
			// append a space at the end of line
			currentQuery.WriteString(line + " ")

			if strings.HasSuffix(line, ";") {
				metrics[currentMetric] = currentQuery.String()
				currentMetric = ""
				currentQuery.Reset()
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return metrics, nil
}

func fileToQueries(filename string, vars map[string]string) []string {
	queryBytes, err := ioutil.ReadFile(filename)
	rtx.Must(err, "Failed to open %q", filename)
	queriesString := strings.Split(string(queryBytes), ";")

	var queries []string
	for _, q := range queriesString {
		q = strings.TrimSpace(q)

		if len(q) > 0 {
			q = strings.Replace(q, "UNIX_START_TIME", vars["UNIX_START_TIME"], -1)
			q = strings.Replace(q, "REFRESH_RATE_SEC", vars["REFRESH_RATE_SEC"], -1)

			queries = append(queries, q)
			log.Println(queries)
		}
	}

	return queries
}

func reloadRegisterUpdate(client *bigquery.Client, files []setup.File, vars map[string]string) {
	var wg sync.WaitGroup
	// Define and initialize a metric registry
	metricRegistry := make(map[string]bool)

	for i := range files {
		wg.Add(1)
		go func(f *setup.File) {
			defer wg.Done()
			modified, err := f.IsModified()
			if err != nil {
				log.Println("Error:", f.Name, "Failed to check if file is modified:", err)
				return
			}

			if modified {
				// Extract metrics and queries from the file contents.
				metrics, err := fileToMetrics(f.Name)
				if err != nil {
					log.Println("Error:", f.Name, "Failed to process metrics:", err)
					return
				}

				for metricName, query := range metrics {
					// Register only if the metric has not been registered yet.
					if !metricRegistry[metricName] {
						log.Println("Preparing to register:", metricName, "WITH", query)
						c := sql.NewCollector(
							newRunner(client), prometheus.GaugeValue,
							metricName, query) // Using the specific query for this metric.
						err := prometheus.Register(c)
						if err != nil {
							log.Println("Failed to register collector for", metricName, ":", err)
						} else {
							log.Println("Successfully registered:", metricName)
							metricRegistry[metricName] = true
						}
					}
				}
			} else {
				start := time.Now()
				err = f.Update()
				if err != nil {
					log.Println("Error:", f.Name, "Failed to update:", err)
				} else {
					log.Println("Updating:", time.Since(start))
				}
			}
		}(&files[i])
	}
	wg.Wait()
}

var mainCtx, mainCancel = context.WithCancel(context.Background())
var newRunner = func(client *bigquery.Client) sql.QueryRunner {
	return query.NewBQRunner(client)
}

func main() {
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from env")

	srv := prometheusx.MustServeMetrics()
	defer srv.Shutdown(mainCtx)

	files := make([]setup.File, len(gaugeSources))
	for i := range files {
		files[i].Name = gaugeSources[i]
	}

	client, err := bigquery.NewClient(mainCtx, *project)
	rtx.Must(err, "Failed to allocate a new bigquery.Client")
	vars := map[string]string{
		"UNIX_START_TIME":  fmt.Sprintf("%d", time.Now().UTC().Unix()),
		"REFRESH_RATE_SEC": fmt.Sprintf("%d", int(refresh.Seconds())),
	}

	for mainCtx.Err() == nil {
		reloadRegisterUpdate(client, files, vars)
		sleepUntilNext(*refresh)
	}
}
