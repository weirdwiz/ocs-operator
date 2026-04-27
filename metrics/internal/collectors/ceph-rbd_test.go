package collectors

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/ceph/go-ceph/rbd"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// --- Mock types for RBD interface testing ---

type fakeIOContext struct {
	ns string
}

func (f *fakeIOContext) SetNamespace(ns string) { f.ns = ns }
func (f *fakeIOContext) Destroy()               {}

type fakeConnector struct {
	pools    []string
	poolsErr error
	ioctxErr map[string]error // pool -> error
	monBuf   []byte
	monErr   error
}

func (f *fakeConnector) ListPools() ([]string, error) {
	return f.pools, f.poolsErr
}

func (f *fakeConnector) OpenIOContext(pool string) (rbdIOContext, error) {
	if f.ioctxErr != nil {
		if err, ok := f.ioctxErr[pool]; ok {
			return nil, err
		}
	}
	return &fakeIOContext{}, nil
}

func (f *fakeConnector) MonCommand(args []byte) ([]byte, string, error) {
	return f.monBuf, "", f.monErr
}

type fakeImage struct {
	metadata    map[string]string
	metadataErr error
	snaps       []rbd.SnapInfo
	snapsErr    error
	children    []string
	childrenErr error
}

func (f *fakeImage) GetMetadata(key string) (string, error) {
	if f.metadataErr != nil {
		return "", f.metadataErr
	}
	val, ok := f.metadata[key]
	if !ok {
		return "", rbd.ErrNotFound
	}
	return val, nil
}

func (f *fakeImage) GetSnapshotNames() ([]rbd.SnapInfo, error) {
	return f.snaps, f.snapsErr
}

func (f *fakeImage) ListChildren() ([]string, []string, error) {
	return nil, f.children, f.childrenErr
}

func (f *fakeImage) Close() error { return nil }

type fakeRbdOps struct {
	images       map[string][]string // ns -> image names
	imagesErr    error
	namespaces   []string
	nsErr        error
	mirrorMode   rbd.MirrorMode
	mirrorModeErr error
	peers         []*rbd.MirrorPeerSite
	peersErr      error
	statuses      []rbd.GlobalMirrorImageIDAndStatus
	statusesErr   error
	imagesByName  map[string]*fakeImage
	openErr       error
}

func (f *fakeRbdOps) GetImageNames(ioctx rbdIOContext) ([]string, error) {
	if f.imagesErr != nil {
		return nil, f.imagesErr
	}
	ns := ioctx.(*fakeIOContext).ns
	return f.images[ns], nil
}

func (f *fakeRbdOps) NamespaceList(_ rbdIOContext) ([]string, error) {
	return f.namespaces, f.nsErr
}

func (f *fakeRbdOps) GetMirrorMode(_ rbdIOContext) (rbd.MirrorMode, error) {
	return f.mirrorMode, f.mirrorModeErr
}

func (f *fakeRbdOps) MirrorImageGlobalStatusList(_ rbdIOContext) ([]rbd.GlobalMirrorImageIDAndStatus, error) {
	return f.statuses, f.statusesErr
}

func (f *fakeRbdOps) ListMirrorPeerSite(_ rbdIOContext) ([]*rbd.MirrorPeerSite, error) {
	return f.peers, f.peersErr
}

func (f *fakeRbdOps) OpenImageReadOnly(_ rbdIOContext, name string) (rbdImage, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	if img, ok := f.imagesByName[name]; ok {
		return img, nil
	}
	return nil, fmt.Errorf("image %s not found", name)
}

func TestChunkImages(t *testing.T) {
	tests := []struct {
		name       string
		images     []string
		wantChunks int
		wantLast   int
	}{
		{
			name:       "empty",
			images:     nil,
			wantChunks: 0,
		},
		{
			name:       "under chunk size",
			images:     makeImageNames(50),
			wantChunks: 1,
			wantLast:   50,
		},
		{
			name:       "exact chunk size",
			images:     makeImageNames(imageChunkSize),
			wantChunks: 1,
			wantLast:   imageChunkSize,
		},
		{
			name:       "one over",
			images:     makeImageNames(imageChunkSize + 1),
			wantChunks: 2,
			wantLast:   1,
		},
		{
			name:       "three full chunks",
			images:     makeImageNames(imageChunkSize * 3),
			wantChunks: 3,
			wantLast:   imageChunkSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := chunkImages("pool", "ns", tt.images)
			if len(chunks) != tt.wantChunks {
				t.Fatalf("got %d chunks, want %d", len(chunks), tt.wantChunks)
			}
			if tt.wantChunks > 0 {
				last := chunks[len(chunks)-1]
				if len(last.images) != tt.wantLast {
					t.Errorf("last chunk has %d images, want %d", len(last.images), tt.wantLast)
				}
				if last.pool != "pool" || last.radosNamespace != "ns" {
					t.Errorf("chunk pool/ns = %s/%s, want pool/ns", last.pool, last.radosNamespace)
				}
			}
		})
	}
}

func TestCephRBDCollectorCollect(t *testing.T) {
	c := &CephRBDCollector{
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd", "pv_metadata"),
			"test", []string{"name", "image", "pool_name", "rados_namespace", "consumer_name"}, nil,
		),
		childrenCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd", "children_count"),
			"test", []string{"image", "pool_name", "rados_namespace", "consumer_name"}, nil,
		),
		mirrorState: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd_mirror", "image_state"),
			"test", []string{"image", "pool_name", "site_name", "consumer_name"}, nil,
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
		snap := &rbdCacheSnapshot{
			pools: map[poolNsKey]*rbdPoolData{
				{pool: "rbd-pool", radosNamespace: "ns1"}: {
					consumerName: "consumer-a",
					images: map[string]rbdImageData{
						"img-001": {pvName: "pvc-abc", children: 2},
						"img-002": {pvName: "pvc-def", children: 0},
					},
					mirrors: []rbdMirrorData{
						{imageName: "img-001", siteName: "site-b", state: 4},
					},
				},
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

		// 2 images * 2 descs (pvMetadata + childrenCount) + 1 mirror = 5
		if len(metrics) != 5 {
			t.Fatalf("expected 5 metrics, got %d", len(metrics))
		}

		// Verify one of the metrics has the right label values.
		found := false
		for _, m := range metrics {
			var d dto.Metric
			if err := m.Write(&d); err != nil {
				t.Fatal(err)
			}
			for _, lp := range d.Label {
				if lp.GetName() == "site_name" && lp.GetValue() == "site-b" {
					found = true
					if d.Gauge.GetValue() != 4 {
						t.Errorf("mirror state = %v, want 4", d.Gauge.GetValue())
					}
				}
			}
		}
		if !found {
			t.Error("did not find mirror metric with site_name=site-b")
		}
	})
}

func TestCephRBDCollectorDescribe(t *testing.T) {
	c := &CephRBDCollector{
		pvMetadata: prometheus.NewDesc("a", "a", nil, nil),
		childrenCount: prometheus.NewDesc("b", "b", nil, nil),
		mirrorState: prometheus.NewDesc("c", "c", nil, nil),
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

func makeImageNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("img-%04d", i)
	}
	return names
}

func TestExtractIPFromAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{name: "ipv4 with port and nonce", addr: "10.128.2.36:0/1180205774", want: "10.128.2.36"},
		{name: "ipv4 with port no nonce", addr: "10.128.2.36:6789", want: "10.128.2.36"},
		{name: "ipv6 with port and nonce", addr: "[2001:db8::1]:6789/1180205774", want: "2001:db8::1"},
		{name: "ipv6 with port no nonce", addr: "[2001:db8::1]:6789", want: "2001:db8::1"},
		{name: "nonce only no port", addr: "10.0.0.1/999", want: "10.0.0.1"},
		{name: "bare ip", addr: "10.0.0.1", want: "10.0.0.1"},
		{name: "empty string", addr: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractIPFromAddr(tt.addr); got != tt.want {
				t.Errorf("extractIPFromAddr(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestStripCIDRMask(t *testing.T) {
	tests := []struct {
		name string
		cidr string
		want string
	}{
		{name: "with /32", cidr: "10.0.0.1/32", want: "10.0.0.1"},
		{name: "with /24", cidr: "10.0.0.0/24", want: "10.0.0.0"},
		{name: "no mask", cidr: "10.0.0.1", want: "10.0.0.1"},
		{name: "empty", cidr: "", want: ""},
		{name: "ipv6 with mask", cidr: "2001:db8::1/128", want: "2001:db8::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripCIDRMask(tt.cidr); got != tt.want {
				t.Errorf("stripCIDRMask(%q) = %q, want %q", tt.cidr, got, tt.want)
			}
		})
	}
}

// Verify cache type satisfies atomic.Pointer usage.
func TestRBDCacheAtomicPointer(t *testing.T) {
	var p atomic.Pointer[rbdCacheSnapshot]
	if p.Load() != nil {
		t.Error("zero value should be nil")
	}
	snap := &rbdCacheSnapshot{pools: map[poolNsKey]*rbdPoolData{}}
	p.Store(snap)
	if p.Load() != snap {
		t.Error("stored value should be retrievable")
	}
}

func newTestRBDCollector(conn *fakeConnector, ops *fakeRbdOps) *CephRBDCollector {
	return &CephRBDCollector{
		rbdConn: conn,
		rbdOps:  ops,
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd", "pv_metadata"),
			"test", []string{"name", "image", "pool_name", "rados_namespace", "consumer_name"}, nil,
		),
		childrenCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd", "children_count"),
			"test", []string{"image", "pool_name", "rados_namespace", "consumer_name"}, nil,
		),
		mirrorState: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd_mirror", "image_state"),
			"test", []string{"image", "pool_name", "site_name", "consumer_name"}, nil,
		),
	}
}

func TestScanPoolPartialNamespaceFailure(t *testing.T) {
	conn := &fakeConnector{pools: []string{"rbd-pool"}}
	ops := &fakeRbdOps{
		images: map[string][]string{
			"":   {"img-default"},
			"ns1": {"img-ns1"},
		},
		namespaces:   []string{"ns1", "ns-unknown"},
		mirrorMode:   rbd.MirrorModeDisabled,
	}
	c := newTestRBDCollector(conn, ops)

	nsToConsumer := map[string]string{"ns1": "consumer-a"}
	newPools := make(map[poolNsKey]*rbdPoolData)

	work, err := c.scanPool(conn, "rbd-pool", nsToConsumer, newPools)
	if err != nil {
		t.Fatalf("scanPool failed: %v", err)
	}

	// Default namespace ("") should produce work, ns1 should too, ns-unknown filtered out.
	if len(work) < 1 {
		t.Fatalf("expected at least 1 work batch, got %d", len(work))
	}

	// ns1 should be in the pools map (has a consumer mapping).
	if _, ok := newPools[poolNsKey{"rbd-pool", "ns1"}]; !ok {
		t.Error("expected ns1 in newPools")
	}
	// ns-unknown is NOT in nsToConsumer, so it should be skipped.
	if _, ok := newPools[poolNsKey{"rbd-pool", "ns-unknown"}]; ok {
		t.Error("ns-unknown should have been filtered out")
	}
}

func TestScanPoolIOContextFailure(t *testing.T) {
	conn := &fakeConnector{
		pools:    []string{"good-pool", "bad-pool", "good-pool-2"},
		ioctxErr: map[string]error{"bad-pool": fmt.Errorf("permission denied")},
	}
	ops := &fakeRbdOps{
		images:     map[string][]string{"": {"img-1"}},
		mirrorMode: rbd.MirrorModeDisabled,
	}
	c := newTestRBDCollector(conn, ops)

	nsToConsumer := map[string]string{}
	newPools := make(map[poolNsKey]*rbdPoolData)

	// scanPool on bad-pool should return error.
	_, err := c.scanPool(conn, "bad-pool", nsToConsumer, newPools)
	if err == nil {
		t.Error("expected error for bad-pool")
	}

	// scanPool on good-pool should succeed.
	_, err = c.scanPool(conn, "good-pool", nsToConsumer, newPools)
	if err != nil {
		t.Fatalf("expected success for good-pool: %v", err)
	}
}

func TestProcessImageWithMockOps(t *testing.T) {
	ops := &fakeRbdOps{
		imagesByName: map[string]*fakeImage{
			"img-with-pv": {
				metadata: map[string]string{pvMetadataKey: "pvc-abc-123"},
				snaps:    []rbd.SnapInfo{{Id: 1, Name: "snap1", Size: 1024}},
				children: []string{"child-1", "child-2"},
			},
			"img-no-pv": {
				metadataErr: rbd.ErrNotFound,
			},
			"img-no-snaps": {
				metadata: map[string]string{pvMetadataKey: "pvc-def-456"},
			},
		},
	}
	conn := &fakeConnector{pools: []string{"test-pool"}}
	c := newTestRBDCollector(conn, ops)

	ioctx := &fakeIOContext{}

	t.Run("image with PV metadata and children", func(t *testing.T) {
		data, ok := c.processImage(ioctx, "img-with-pv")
		if !ok {
			t.Fatal("expected ok=true")
		}
		if data.pvName != "pvc-abc-123" {
			t.Errorf("pvName = %q, want %q", data.pvName, "pvc-abc-123")
		}
		if data.children != 2 {
			t.Errorf("children = %d, want 2", data.children)
		}
	})

	t.Run("image without PV metadata is skipped", func(t *testing.T) {
		_, ok := c.processImage(ioctx, "img-no-pv")
		if ok {
			t.Error("expected ok=false for image without PV metadata")
		}
	})

	t.Run("image with no snapshots has zero children", func(t *testing.T) {
		data, ok := c.processImage(ioctx, "img-no-snaps")
		if !ok {
			t.Fatal("expected ok=true")
		}
		if data.children != 0 {
			t.Errorf("children = %d, want 0", data.children)
		}
	})

	t.Run("nonexistent image is skipped", func(t *testing.T) {
		_, ok := c.processImage(ioctx, "no-such-image")
		if ok {
			t.Error("expected ok=false for nonexistent image")
		}
	})
}

func TestCollectMirrorDataDisabled(t *testing.T) {
	ops := &fakeRbdOps{mirrorMode: rbd.MirrorModeDisabled}
	c := newTestRBDCollector(&fakeConnector{}, ops)

	result := c.collectMirrorData(&fakeIOContext{}, "pool", "")
	if result != nil {
		t.Errorf("expected nil for disabled mirror mode, got %v", result)
	}
}

func TestCollectMirrorDataWithPeers(t *testing.T) {
	ops := &fakeRbdOps{
		mirrorMode: rbd.MirrorModePool,
		peers: []*rbd.MirrorPeerSite{
			{MirrorUUID: "uuid-1", SiteName: "site-remote"},
		},
		statuses: []rbd.GlobalMirrorImageIDAndStatus{
			{
				Status: rbd.GlobalMirrorImageStatus{
					Name: "img-mirrored",
					SiteStatuses: []rbd.SiteMirrorImageStatus{
						{MirrorUUID: "", State: 0},
						{MirrorUUID: "uuid-1", State: 4},
					},
				},
			},
		},
	}
	c := newTestRBDCollector(&fakeConnector{}, ops)

	result := c.collectMirrorData(&fakeIOContext{}, "pool", "ns")
	if len(result) != 1 {
		t.Fatalf("expected 1 mirror entry, got %d", len(result))
	}
	if result[0].siteName != "site-remote" {
		t.Errorf("siteName = %q, want %q", result[0].siteName, "site-remote")
	}
	if result[0].state != 4 {
		t.Errorf("state = %v, want 4", result[0].state)
	}
	if result[0].imageName != "img-mirrored" {
		t.Errorf("imageName = %q, want %q", result[0].imageName, "img-mirrored")
	}
}

func TestFetchBlocklistIPs(t *testing.T) {
	t.Run("parses blocklist JSON", func(t *testing.T) {
		conn := &fakeConnector{
			monBuf: []byte(`[{"addr":"10.0.0.1:0/123"},{"addr":"10.0.0.2:6789/456"},{"addr":"10.0.0.1:0/789"}]`),
		}
		c := newTestRBDCollector(conn, &fakeRbdOps{})

		ips, err := c.fetchBlocklistIPs()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ips) != 2 {
			t.Fatalf("expected 2 unique IPs, got %d: %v", len(ips), ips)
		}
	})

	t.Run("empty blocklist", func(t *testing.T) {
		conn := &fakeConnector{monBuf: []byte(`[]`)}
		c := newTestRBDCollector(conn, &fakeRbdOps{})

		ips, err := c.fetchBlocklistIPs()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(ips) != 0 {
			t.Errorf("expected 0 IPs, got %d", len(ips))
		}
	})

	t.Run("MonCommand error", func(t *testing.T) {
		conn := &fakeConnector{monErr: fmt.Errorf("connection refused")}
		c := newTestRBDCollector(conn, &fakeRbdOps{})

		_, err := c.fetchBlocklistIPs()
		if err == nil {
			t.Error("expected error")
		}
	})
}
