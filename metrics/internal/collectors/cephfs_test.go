package collectors

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type fakeFSAdmin struct {
	volumes    []string
	volumesErr error
	groups     map[string][]string          // volume -> groups
	groupsErr  map[string]error             // volume -> error
	subvols    map[string]map[string][]string // volume -> group -> subvols
	subvolsErr error
	snapshots  map[string][]string          // "vol/group/sv" -> snapshots
	metadata   map[string]string            // "vol/group/sv" -> pv name
}

func (f *fakeFSAdmin) ListVolumes() ([]string, error) {
	return f.volumes, f.volumesErr
}

func (f *fakeFSAdmin) ListSubVolumeGroups(volume string) ([]string, error) {
	if f.groupsErr != nil {
		if err, ok := f.groupsErr[volume]; ok {
			return nil, err
		}
	}
	return f.groups[volume], nil
}

func (f *fakeFSAdmin) ListSubVolumes(volume, group string) ([]string, error) {
	if f.subvolsErr != nil {
		return nil, f.subvolsErr
	}
	return f.subvols[volume][group], nil
}

func (f *fakeFSAdmin) ListSubVolumeSnapshots(volume, group, sv string) ([]string, error) {
	key := volume + "/" + group + "/" + sv
	return f.snapshots[key], nil
}

func (f *fakeFSAdmin) GetMetadata(volume, group, sv, key string) (string, error) {
	k := volume + "/" + group + "/" + sv
	if val, ok := f.metadata[k]; ok {
		return val, nil
	}
	return "", fmt.Errorf("metadata not found")
}

func TestCephFSSubvolumeCountCollectorCollect(t *testing.T) {
	c := &CephFSSubvolumeCountCollector{
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "pv_metadata"),
			"test", []string{"name", "subvolume", "volume", "subvolume_group", "consumer_name"}, nil,
		),
		subvolumeCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "subvolume_count"),
			"test", []string{"consumer_name"}, nil,
		),
		snapshotContentCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "snapshot_content_count"),
			"test", []string{"consumer_name"}, nil,
		),
	}

	t.Run("nil cache returns no metrics", func(t *testing.T) {
		ch := make(chan prometheus.Metric, 10)
		c.Collect(ch)
		close(ch)
		if len(ch) != 0 {
			t.Errorf("expected 0 metrics, got %d", len(ch))
		}
	})

	t.Run("populated cache emits metrics", func(t *testing.T) {
		snap := &cephfsCacheSnapshot{
			groups: []cephfsGroupData{
				{
					consumerName: "consumer-a",
					volume:       "cephfs-vol",
					group:        "csi",
					subvolumes: map[string]string{
						"sv-001": "pvc-aaa",
						"sv-002": "pvc-bbb",
					},
				},
				{
					consumerName: "consumer-b",
					volume:       "cephfs-vol",
					group:        "csi-2",
					subvolumes: map[string]string{
						"sv-003": "pvc-ccc",
					},
				},
			},
			subvolumesByConsumer: map[string]int{
				"consumer-a": 2,
				"consumer-b": 3,
			},
			snapshotContentsByConsumer: map[string]int{
				"consumer-a": 1,
				"consumer-b": 4,
			},
		}
		c.cache.Store(snap)

		ch := make(chan prometheus.Metric, 20)
		c.Collect(ch)
		close(ch)

		var metrics []prometheus.Metric
		for m := range ch {
			metrics = append(metrics, m)
		}

		// 2 subvolumeCount (per consumer) + 2 snapshotContentCount (per consumer) + 3 pvMetadata = 7
		if len(metrics) != 7 {
			t.Fatalf("expected 7 metrics, got %d", len(metrics))
		}

		countsByConsumer := make(map[string]float64)
		for _, m := range metrics {
			var d dto.Metric
			if err := m.Write(&d); err != nil {
				t.Fatal(err)
			}
			for _, lp := range d.Label {
				if lp.GetName() == "consumer_name" && d.Gauge != nil {
					countsByConsumer[lp.GetValue()] = d.Gauge.GetValue()
				}
			}
		}
		if countsByConsumer["consumer-a"] != 2 {
			t.Errorf("consumer-a subvolume_count = %v, want 2", countsByConsumer["consumer-a"])
		}
		if countsByConsumer["consumer-b"] != 3 {
			t.Errorf("consumer-b subvolume_count = %v, want 3", countsByConsumer["consumer-b"])
		}
	})
}

func TestCephFSSubvolumeCountCollectorDescribe(t *testing.T) {
	c := &CephFSSubvolumeCountCollector{
		pvMetadata:           prometheus.NewDesc("a", "a", nil, nil),
		subvolumeCount:       prometheus.NewDesc("b", "b", nil, nil),
		snapshotContentCount: prometheus.NewDesc("c", "c", nil, nil),
	}

	ch := make(chan *prometheus.Desc, 10)
	c.Describe(ch)
	close(ch)

	var descs []*prometheus.Desc
	for d := range ch {
		descs = append(descs, d)
	}
	if len(descs) != 3 {
		t.Errorf("expected 3 descriptors, got %d", len(descs))
	}
}

func TestCephFSCacheAtomicPointer(t *testing.T) {
	var p atomic.Pointer[cephfsCacheSnapshot]
	if p.Load() != nil {
		t.Error("zero value should be nil")
	}
	snap := &cephfsCacheSnapshot{subvolumesByConsumer: map[string]int{"test": 42}}
	p.Store(snap)
	if p.Load().subvolumesByConsumer["test"] != 42 {
		t.Error("stored snapshot should be retrievable")
	}
}

func newTestCephFSCollector(fsa fsAdmin) *CephFSSubvolumeCountCollector {
	return &CephFSSubvolumeCountCollector{
		newFSAdmin: func() (fsAdmin, error) { return fsa, nil },
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "pv_metadata"),
			"test", []string{"name", "subvolume", "volume", "subvolume_group", "consumer_name"}, nil,
		),
		subvolumeCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "subvolume_count"),
			"test", []string{"consumer_name"}, nil,
		),
		snapshotContentCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "snapshot_content_count"),
			"test", []string{"consumer_name"}, nil,
		),
	}
}

func TestCephFSRunScan(t *testing.T) {
	fsa := &fakeFSAdmin{
		volumes: []string{"cephfs"},
		groups:  map[string][]string{"cephfs": {"csi", "nfs"}},
		subvols: map[string]map[string][]string{
			"cephfs": {
				"csi": {"sv-1", "sv-2"},
				"nfs": {"sv-3"},
			},
		},
		snapshots: map[string][]string{
			"cephfs/csi/sv-1": {"snap-a"},
		},
		metadata: map[string]string{
			"cephfs/csi/sv-1": "pvc-aaa",
			"cephfs/csi/sv-2": "pvc-bbb",
		},
	}
	c := newTestCephFSCollector(fsa)

	ok := c.runScan()
	if !ok {
		t.Fatal("expected runScan to succeed")
	}

	snap := c.cache.Load()
	if snap == nil {
		t.Fatal("cache should be populated")
	}

	// sv-1 and sv-2 have PV metadata, sv-3 does not.
	totalPVLinked := 0
	for _, g := range snap.groups {
		totalPVLinked += len(g.subvolumes)
	}
	if totalPVLinked != 2 {
		t.Errorf("expected 2 PV-linked subvolumes, got %d", totalPVLinked)
	}

	// All subvolumes counted regardless of PV metadata.
	total := 0
	for _, count := range snap.subvolumesByConsumer {
		total += count
	}
	if total != 3 {
		t.Errorf("expected 3 total subvolumes, got %d", total)
	}

	// sv-1 has 1 snapshot.
	totalSnaps := 0
	for _, count := range snap.snapshotContentsByConsumer {
		totalSnaps += count
	}
	if totalSnaps != 1 {
		t.Errorf("expected 1 snapshot content, got %d", totalSnaps)
	}
}

func TestCephFSPartialVolumeFailure(t *testing.T) {
	fsa := &fakeFSAdmin{
		volumes:   []string{"vol-bad", "vol-good"},
		groupsErr: map[string]error{"vol-bad": fmt.Errorf("permission denied")},
		groups:    map[string][]string{"vol-good": {"csi"}},
		subvols: map[string]map[string][]string{
			"vol-good": {"csi": {"sv-ok"}},
		},
		metadata: map[string]string{
			"vol-good/csi/sv-ok": "pvc-ok",
		},
	}
	c := newTestCephFSCollector(fsa)

	ok := c.runScan()
	if !ok {
		t.Fatal("expected runScan to succeed with partial failure")
	}

	snap := c.cache.Load()
	if snap == nil {
		t.Fatal("cache should be populated")
	}

	total := 0
	for _, count := range snap.subvolumesByConsumer {
		total += count
	}
	if total != 1 {
		t.Errorf("expected 1 subvolume from vol-good, got %d", total)
	}
}

func TestCephFSAllVolumesFail(t *testing.T) {
	fsa := &fakeFSAdmin{
		volumes:   []string{"vol-1"},
		groupsErr: map[string]error{"vol-1": fmt.Errorf("connection lost")},
	}
	c := newTestCephFSCollector(fsa)

	ok := c.runScan()
	if ok {
		t.Error("expected runScan to fail when all volumes fail")
	}
}

func TestCephFSConnectionFailure(t *testing.T) {
	c := &CephFSSubvolumeCountCollector{
		newFSAdmin: func() (fsAdmin, error) {
			return nil, fmt.Errorf("connection refused")
		},
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "pv_metadata"),
			"test", []string{"name", "subvolume", "volume", "subvolume_group", "consumer_name"}, nil,
		),
		subvolumeCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "subvolume_count"),
			"test", []string{"consumer_name"}, nil,
		),
		snapshotContentCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "snapshot_content_count"),
			"test", []string{"consumer_name"}, nil,
		),
	}

	ok := c.runScan()
	if ok {
		t.Error("expected runScan to fail on connection error")
	}
}
