package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type MemoryMetricEntry struct {
	Timestamp      time.Time `json:"timestamp"`
	HeapAllocBytes float64   `json:"heap_alloc_bytes"`
	HeapInuseBytes float64   `json:"heap_inuse_bytes"`
	HeapSysBytes   float64   `json:"heap_sys_bytes"`
}

type MemoryMonitor struct {
	cfg      *Config
	metrics  []MemoryMetricEntry
	mutex    sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	filename string
}

func NewMemoryMonitor(cfg *Config) *MemoryMonitor {
	ctx, cancel := context.WithCancel(context.Background())

	filename := cfg.MemoryMonitoringFile
	if filename == "" {
		filename = fmt.Sprintf("memory_metrics_%d.json", time.Now().Unix())
	}

	return &MemoryMonitor{
		cfg:      cfg,
		metrics:  make([]MemoryMetricEntry, 0),
		ctx:      ctx,
		cancel:   cancel,
		filename: filename,
	}
}

func (m *MemoryMonitor) Start() {
	if !m.cfg.MemoryMonitoringEnabled {
		return
	}

	interval := time.Duration(m.cfg.MemoryMonitoringInterval) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}

	log.WithFields(log.Fields{
		"interval": interval,
		"file":     m.filename,
	}).Info("Starting memory monitoring")

	m.wg.Add(1)
	go m.monitorLoop(interval)
}

func (m *MemoryMonitor) Stop() {
	if !m.cfg.MemoryMonitoringEnabled {
		return
	}

	log.Info("Stopping memory monitoring")
	m.cancel()
	m.wg.Wait()

	if err := m.writeToFile(); err != nil {
		log.WithError(err).Error("Failed to write memory metrics to file")
	} else {
		log.WithFields(log.Fields{
			"file":    m.filename,
			"entries": len(m.metrics),
		}).Info("Memory metrics written to file")
	}
}

func (m *MemoryMonitor) monitorLoop(interval time.Duration) {
	defer m.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	m.recordMetric()

	for {
		select {
		case <-m.ctx.Done():
			m.recordMetric()
			return
		case <-ticker.C:
			m.recordMetric()
		}
	}
}

func (m *MemoryMonitor) recordMetric() {
	memstats, err := readMemoryMetrics(m.cfg)
	if err != nil {
		log.WithError(err).Warn("Failed to read memory metrics")
		return
	}

	entry := MemoryMetricEntry{
		Timestamp:      time.Now(),
		HeapAllocBytes: memstats.HeapAllocBytes,
		HeapInuseBytes: memstats.HeapInuseBytes,
		HeapSysBytes:   memstats.HeapSysBytes,
	}

	m.mutex.Lock()
	m.metrics = append(m.metrics, entry)
	m.mutex.Unlock()

	log.WithFields(log.Fields{
		"heap_alloc_mb": entry.HeapAllocBytes / 1024 / 1024,
		"heap_inuse_mb": entry.HeapInuseBytes / 1024 / 1024,
		"heap_sys_mb":   entry.HeapSysBytes / 1024 / 1024,
	}).Debug("Recorded memory metric")
}

func (m *MemoryMonitor) writeToFile() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if err := os.MkdirAll("./results", 0o755); err != nil {
		return fmt.Errorf("failed to create results directory: %w", err)
	}

	filepath := fmt.Sprintf("./results/%s", m.filename)

	data, err := json.MarshalIndent(m.metrics, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

func (m *MemoryMonitor) GetMetrics() []MemoryMetricEntry {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	result := make([]MemoryMetricEntry, len(m.metrics))
	copy(result, m.metrics)
	return result
}
