package collectors

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestBlocklistCollectNilCache(t *testing.T) {
	rbd := &CephRBDCollector{
		pvMetadata:    prometheus.NewDesc("a", "a", nil, nil),
		childrenCount: prometheus.NewDesc("b", "b", nil, nil),
		mirrorState:   prometheus.NewDesc("c", "c", nil, nil),
	}
	c := NewCephBlocklistCollector(rbd)

	ch := make(chan prometheus.Metric, 10)
	c.Collect(ch)
	close(ch)
	if len(ch) != 0 {
		t.Errorf("expected 0 metrics with nil cache, got %d", len(ch))
	}
}

func TestBlocklistCollectPopulated(t *testing.T) {
	rbd := &CephRBDCollector{
		pvMetadata:    prometheus.NewDesc("a", "a", nil, nil),
		childrenCount: prometheus.NewDesc("b", "b", nil, nil),
		mirrorState:   prometheus.NewDesc("c", "c", nil, nil),
	}
	rbd.cache.Store(&rbdCacheSnapshot{
		blockedNodes: map[string]string{
			"node-1": "consumer-a",
			"node-2": "consumer-b",
		},
	})

	c := NewCephBlocklistCollector(rbd)

	ch := make(chan prometheus.Metric, 10)
	c.Collect(ch)
	close(ch)

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}

	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(metrics))
	}

	nodes := make(map[string]bool)
	for _, m := range metrics {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		if d.Gauge.GetValue() != 1 {
			t.Errorf("expected gauge value 1, got %v", d.Gauge.GetValue())
		}
		for _, lp := range d.Label {
			if lp.GetName() == "node" {
				nodes[lp.GetValue()] = true
			}
		}
	}

	if !nodes["node-1"] || !nodes["node-2"] {
		t.Errorf("expected node-1 and node-2 in metrics, got %v", nodes)
	}
}

func TestBlocklistDescribe(t *testing.T) {
	rbd := &CephRBDCollector{
		pvMetadata:    prometheus.NewDesc("a", "a", nil, nil),
		childrenCount: prometheus.NewDesc("b", "b", nil, nil),
		mirrorState:   prometheus.NewDesc("c", "c", nil, nil),
	}
	c := NewCephBlocklistCollector(rbd)

	ch := make(chan *prometheus.Desc, 10)
	c.Describe(ch)
	close(ch)

	var descs []*prometheus.Desc
	for d := range ch {
		descs = append(descs, d)
	}
	if len(descs) != 1 {
		t.Errorf("expected 1 descriptor, got %d", len(descs))
	}
}
