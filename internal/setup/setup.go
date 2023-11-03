package setup

import (
	"fmt"
	"log"
	"os"

	"github.com/m-lab/go/logx"
	"github.com/m-lab/prometheus-bigquery-exporter/sql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/afero"
)

var fs = afero.NewOsFs()

// File represents a query file and related metadata to keep it up to date and
// registered with the prometheus collector registry.
type File struct {
	Name string
	stat os.FileInfo
	c    *sql.Collector
}

// IsModified reports true if the file has been modified since the last call.
// The first call should almost always return false.
func (f *File) IsModified() (bool, error) {
	var err error
	if f.stat == nil {
		f.stat, err = fs.Stat(f.Name)
		logx.Debug.Println("IsModified:stat1:", f.Name, err)
		// Return true on the first successful Stat(), or the error otherwise.
		return err == nil, err
	}
	curr, err := fs.Stat(f.Name)
	if err != nil {
		log.Printf("Failed to stat %q: %v", f.Name, err)
		return false, err
	}
	logx.Debug.Println("IsModified:stat2:", f.Name, curr.ModTime(), f.stat.ModTime(),
		curr.ModTime().After(f.stat.ModTime()))
	modified := curr.ModTime().After(f.stat.ModTime())
	if modified {
		// Update the stat cache to the latest version.
		f.stat = curr
	}
	return modified, nil
}

// Register the given collector. If a collector was previously registered with
// this file, then it is unregistered first. If either registration or
// unregister fails, then the error is returned.
func (f *File) Register(queryRunner sql.QueryRunner) error {
	if f.c != nil {
		ok := prometheus.Unregister(f.c)
		logx.Debug.Println("Unregister:", ok)
		if !ok {
			return fmt.Errorf("failed to unregister %q", f.Name)
		}
		f.c = nil
	}

	c := sql.NewCollector(queryRunner, prometheus.GaugeValue, f.Name, "your_query_here") // Add appropriate arguments here

	err := prometheus.Register(c)
	if err != nil {
		return err
	}
	logx.Debug.Println("Register:", f.Name, c.RegisterErr)

	f.c = c
	return c.RegisterErr
}

// Update runs the collector query again.
func (f *File) Update() error {
	if f.c != nil {
		return f.c.Update()
	}
	return nil
}
