package collectors

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/rbd"
	"github.com/prometheus/client_golang/prometheus"
	cephconn "github.com/red-hat-storage/ocs-operator/metrics/v4/internal/ceph"
	"github.com/red-hat-storage/ocs-operator/v4/pkg/util"
	rookclient "github.com/rook/rook/pkg/client/clientset/versioned"
	"golang.org/x/sync/errgroup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

const (
	defaultImageWorkers = 8
	imageChunkSize      = 100
)

var _ prometheus.Collector = &CephRBDCollector{}

type rbdImageData struct {
	pvName   string
	children int
}

type rbdMirrorData struct {
	imageName string
	siteName  string
	state     float64
}

type poolNsKey struct {
	pool           string
	radosNamespace string
}

type rbdPoolData struct {
	consumerName string
	images       map[string]rbdImageData
	mirrors      []rbdMirrorData
}

type blocklistEntry struct {
	Addr string `json:"addr"`
}

type rbdCacheSnapshot struct {
	pools        map[poolNsKey]*rbdPoolData
	blockedNodes map[string]string // node name -> consumer_name
}

type rbdImageBatch struct {
	pool           string
	radosNamespace string
	images         []string
}

type CephRBDCollector struct {
	conn         *cephconn.Conn
	rookClient   rookclient.Interface
	dynClient    dynamic.Interface
	namespace    string
	scanInterval time.Duration

	rbdConn rbdConnector
	rbdOps  rbdPoolOps

	pvMetadata    *prometheus.Desc
	childrenCount *prometheus.Desc
	mirrorState   *prometheus.Desc

	cache atomic.Pointer[rbdCacheSnapshot]
}

func NewCephRBDCollector(conn *cephconn.Conn, rookClient rookclient.Interface, dynClient dynamic.Interface, ns string, scanInterval time.Duration) *CephRBDCollector {
	return &CephRBDCollector{
		conn:         conn,
		rookClient:   rookClient,
		dynClient:    dynClient,
		namespace:    ns,
		scanInterval: scanInterval,
		rbdOps:       &realRbdPoolOps{},
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd", "pv_metadata"),
			"Attributes of Ceph RBD based Persistent Volume",
			[]string{"name", "image", "pool_name", "rados_namespace", "consumer_name"},
			nil,
		),
		childrenCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd", "children_count"),
			"Number of RBD children (clones) for a PV-backed image",
			[]string{"image", "pool_name", "rados_namespace", "consumer_name"},
			nil,
		),
		mirrorState: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "rbd_mirror", "image_state"),
			"Mirrored image state",
			[]string{"image", "pool_name", "site_name", "consumer_name"},
			nil,
		),
	}
}

func (c *CephRBDCollector) Run(stopCh <-chan struct{}) {
	runScanLoop(stopCh, c.scanInterval, c.runScan)
}

func (c *CephRBDCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pvMetadata
	ch <- c.childrenCount
	ch <- c.mirrorState
}

func (c *CephRBDCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.cache.Load()
	if snap == nil {
		klog.Warning("RBD cache not yet populated, skipping")
		return
	}

	for key, poolData := range snap.pools {
		for imageName, img := range poolData.images {
			ch <- prometheus.MustNewConstMetric(c.pvMetadata,
				prometheus.GaugeValue, 1,
				img.pvName, imageName, key.pool, key.radosNamespace, poolData.consumerName,
			)
			ch <- prometheus.MustNewConstMetric(c.childrenCount,
				prometheus.GaugeValue, float64(img.children),
				imageName, key.pool, key.radosNamespace, poolData.consumerName,
			)
		}
		for _, m := range poolData.mirrors {
			ch <- prometheus.MustNewConstMetric(c.mirrorState,
				prometheus.GaugeValue, m.state,
				m.imageName, key.pool, m.siteName, poolData.consumerName,
			)
		}
	}
}

func (c *CephRBDCollector) runScan() bool {
	start := time.Now()

	newPools, work, err := c.enumeratePools()
	if err != nil {
		klog.Errorf("rbd scan: %v", err)
		return false
	}

	c.processWork(newPools, work)

	blockedNodes := c.scanBlocklist()
	if blockedNodes == nil {
		if prev := c.cache.Load(); prev != nil {
			blockedNodes = prev.blockedNodes
		}
	}

	c.cache.Store(&rbdCacheSnapshot{pools: newPools, blockedNodes: blockedNodes})

	totalImages := 0
	totalEnumerated := 0
	for _, poolData := range newPools {
		totalImages += len(poolData.images)
	}
	for _, w := range work {
		totalEnumerated += len(w.images)
	}
	klog.Infof("rbd scan: completed in %v, enumerated=%d, pv_linked=%d, blocklisted_nodes=%d",
		time.Since(start), totalEnumerated, totalImages, len(blockedNodes))
	return true
}

func (c *CephRBDCollector) getConnector() (rbdConnector, error) {
	if c.rbdConn != nil {
		return c.rbdConn, nil
	}
	conn, err := c.conn.Get()
	if err != nil {
		return nil, err
	}
	return &radosConnWrapper{conn: conn}, nil
}

func (c *CephRBDCollector) enumeratePools() (map[poolNsKey]*rbdPoolData, []rbdImageBatch, error) {
	connector, err := c.getConnector()
	if err != nil {
		c.conn.Reconnect()
		return nil, nil, fmt.Errorf("failed to get ceph connection: %w", err)
	}

	pools, err := connector.ListPools()
	if err != nil {
		c.conn.Reconnect()
		return nil, nil, fmt.Errorf("failed to list pools: %w", err)
	}

	nsToConsumer := buildRadosNamespaceToConsumerMap(c.rookClient, c.namespace)
	newPools := make(map[poolNsKey]*rbdPoolData)
	var work []rbdImageBatch

	anyPoolSucceeded := false
	for _, pool := range pools {
		poolWork, err := c.scanPool(connector, pool, nsToConsumer, newPools)
		if err != nil {
			klog.Errorf("rbd scan: %v", err)
			continue
		}
		anyPoolSucceeded = true
		work = append(work, poolWork...)
	}

	if len(pools) > 0 && !anyPoolSucceeded {
		c.conn.Reconnect()
		return nil, nil, fmt.Errorf("failed to open IO context for any pool, reconnecting")
	}

	return newPools, work, nil
}

func (c *CephRBDCollector) scanPool(
	connector rbdConnector,
	pool string,
	nsToConsumer map[string]string,
	newPools map[poolNsKey]*rbdPoolData,
) ([]rbdImageBatch, error) {
	ioctx, err := connector.OpenIOContext(pool)
	if err != nil {
		return nil, fmt.Errorf("failed to open IO context for pool %s: %w", pool, err)
	}
	defer ioctx.Destroy()

	var work []rbdImageBatch

	ioctx.SetNamespace("")
	if poolData, chunks := c.enumerateNamespace(ioctx, pool, "", nsToConsumer); poolData != nil {
		newPools[poolNsKey{pool, ""}] = poolData
		work = append(work, chunks...)
	}

	namespaces, err := c.rbdOps.NamespaceList(ioctx)
	if err != nil {
		klog.Errorf("rbd scan: failed to list namespaces for pool %s: %v", pool, err)
		return work, nil
	}
	for _, ns := range namespaces {
		if _, ok := nsToConsumer[ns]; !ok {
			continue
		}
		ioctx.SetNamespace(ns)
		if poolData, chunks := c.enumerateNamespace(ioctx, pool, ns, nsToConsumer); poolData != nil {
			newPools[poolNsKey{pool, ns}] = poolData
			work = append(work, chunks...)
		}
	}

	return work, nil
}

func (c *CephRBDCollector) processWork(newPools map[poolNsKey]*rbdPoolData, work []rbdImageBatch) {
	if len(work) == 0 {
		return
	}

	results := make([]map[string]rbdImageData, len(work))
	group := new(errgroup.Group)
	group.SetLimit(defaultImageWorkers)

	for i, chunk := range work {
		group.Go(func() error {
			results[i] = c.processImageChunk(chunk)
			return nil
		})
	}
	// processImageChunk always returns nil; errors are logged per-chunk.
	_ = group.Wait()

	for i, result := range results {
		if result == nil {
			continue
		}
		key := poolNsKey{work[i].pool, work[i].radosNamespace}
		poolData := newPools[key]
		for name, data := range result {
			poolData.images[name] = data
		}
	}
}

func (c *CephRBDCollector) enumerateNamespace(
	ioctx rbdIOContext,
	pool, radosNamespace string,
	nsToConsumer map[string]string,
) (*rbdPoolData, []rbdImageBatch) {
	images, err := c.rbdOps.GetImageNames(ioctx)
	if err != nil {
		klog.Errorf("rbd scan: failed to list images for %s/%q: %v", pool, radosNamespace, err)
		return nil, nil
	}

	poolData := &rbdPoolData{
		consumerName: nsToConsumer[radosNamespace],
		images:       make(map[string]rbdImageData, len(images)),
		mirrors:      c.collectMirrorData(ioctx, pool, radosNamespace),
	}

	chunks := chunkImages(pool, radosNamespace, images)
	return poolData, chunks
}

func (c *CephRBDCollector) collectMirrorData(ioctx rbdIOContext, pool, radosNamespace string) []rbdMirrorData {
	mirrorMode, err := c.rbdOps.GetMirrorMode(ioctx)
	if err != nil {
		klog.Warningf("rbd scan: failed to get mirror mode for %s/%q: %v", pool, radosNamespace, err)
		return nil
	}
	if mirrorMode == rbd.MirrorModeDisabled {
		return nil
	}

	peers, err := c.rbdOps.ListMirrorPeerSite(ioctx)
	if err != nil {
		klog.Errorf("rbd scan: failed to build mirror peer map for %s: %v", pool, err)
		return nil
	}
	peerMap := make(map[string]string, len(peers))
	for _, peer := range peers {
		peerMap[peer.MirrorUUID] = peer.SiteName
	}

	statuses, err := c.rbdOps.MirrorImageGlobalStatusList(ioctx)
	if err != nil {
		klog.Errorf("rbd scan: failed to list mirror status for %s/%q: %v", pool, radosNamespace, err)
		return nil
	}

	var mirrors []rbdMirrorData
	for _, item := range statuses {
		for _, site := range item.Status.SiteStatuses {
			if site.MirrorUUID == "" {
				continue
			}
			mirrors = append(mirrors, rbdMirrorData{
				imageName: item.Status.Name,
				siteName:  peerMap[site.MirrorUUID],
				state:     float64(site.State),
			})
		}
	}
	return mirrors
}

func chunkImages(pool, radosNamespace string, images []string) []rbdImageBatch {
	var chunks []rbdImageBatch
	for i := 0; i < len(images); i += imageChunkSize {
		end := i + imageChunkSize
		if end > len(images) {
			end = len(images)
		}
		chunks = append(chunks, rbdImageBatch{
			pool:           pool,
			radosNamespace: radosNamespace,
			images:         images[i:end],
		})
	}
	return chunks
}

func (c *CephRBDCollector) processImage(ioctx rbdIOContext, name string) (rbdImageData, bool) {
	img, err := c.rbdOps.OpenImageReadOnly(ioctx, name)
	if err != nil {
		klog.V(4).Infof("rbd worker: failed to open image %s: %v", name, err)
		return rbdImageData{}, false
	}
	defer img.Close()

	pvName, err := img.GetMetadata(pvMetadataKey)
	if err != nil {
		if !errors.Is(err, rbd.ErrNotFound) {
			klog.V(4).Infof("rbd worker: failed to get metadata for %s: %v", name, err)
		}
		return rbdImageData{}, false
	}

	children := 0
	snaps, err := img.GetSnapshotNames()
	if err != nil {
		klog.V(4).Infof("rbd worker: failed to get snapshots for %s: %v", name, err)
	} else if len(snaps) > 0 {
		_, childImages, err := img.ListChildren()
		if err != nil {
			klog.V(4).Infof("rbd worker: failed to list children for %s: %v", name, err)
		} else {
			children = len(childImages)
		}
	}

	return rbdImageData{pvName: pvName, children: children}, true
}

func (c *CephRBDCollector) processImageChunk(w rbdImageBatch) map[string]rbdImageData {
	connector, err := c.getConnector()
	if err != nil {
		klog.Errorf("rbd worker: failed to get ceph connection for %s/%q: %v", w.pool, w.radosNamespace, err)
		return nil
	}
	ioctx, err := connector.OpenIOContext(w.pool)
	if err != nil {
		klog.Errorf("rbd worker: failed to get IO context for %s/%q: %v", w.pool, w.radosNamespace, err)
		return nil
	}
	defer ioctx.Destroy()

	if w.radosNamespace != "" {
		ioctx.SetNamespace(w.radosNamespace)
	}

	results := make(map[string]rbdImageData, len(w.images))
	for _, name := range w.images {
		if data, ok := c.processImage(ioctx, name); ok {
			results[name] = data
		}
	}
	return results
}

func buildRadosNamespaceToConsumerMap(client rookclient.Interface, ns string) map[string]string {
	nsToConsumer := make(map[string]string)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rnsList, err := client.CephV1().CephBlockPoolRadosNamespaces(ns).List(
		ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("failed to list CephBlockPoolRadosNamespaces: %v", err)
		return nsToConsumer
	}

	for _, rns := range rnsList.Items {
		radosNSName := rns.Spec.Name
		if radosNSName == "" {
			radosNSName = rns.Name
		}
		if radosNSName == util.ImplicitRbdRadosNamespaceName {
			radosNSName = ""
		}
		if name := consumerOwnerName(rns.OwnerReferences); name != "" {
			nsToConsumer[radosNSName] = name
		}
	}
	return nsToConsumer
}

// BlockedNodes returns the current blocklist snapshot mapping node names
// to consumer names. Returns nil if the cache is not yet populated.
func (c *CephRBDCollector) BlockedNodes() map[string]string {
	snap := c.cache.Load()
	if snap == nil {
		return nil
	}
	return snap.blockedNodes
}

var csiAddonsNodeGVR = schema.GroupVersionResource{
	Group:    "csiaddons.openshift.io",
	Version:  "v1alpha1",
	Resource: "csiaddonsnodes",
}

// scanBlocklist queries the Ceph OSD blocklist and correlates blocklisted IPs
// with CSIAddonsNode resources to map them to Kubernetes node names.
// Errors are logged but do not fail the RBD scan.
func (c *CephRBDCollector) scanBlocklist() map[string]string {
	if c.dynClient == nil {
		return nil
	}

	blockedIPs, err := c.fetchBlocklistIPs()
	if err != nil {
		klog.Errorf("blocklist scan: %v", err)
		return nil
	}

	if len(blockedIPs) == 0 {
		return make(map[string]string)
	}

	ipToNode := buildIPToNodeMap(c.dynClient, c.namespace)
	if len(ipToNode) == 0 {
		klog.V(4).Info("blocklist scan: no IP-to-node mapping from CSIAddonsNode resources")
		return make(map[string]string)
	}

	blockedNodes := make(map[string]string)
	for _, ip := range blockedIPs {
		if nodeName, ok := ipToNode[ip]; ok {
			blockedNodes[nodeName] = ""
		}
	}
	if len(blockedNodes) > 0 {
		klog.Infof("blocklist scan: %d IPs blocklisted, %d mapped to nodes",
			len(blockedIPs), len(blockedNodes))
	}
	return blockedNodes
}

func (c *CephRBDCollector) fetchBlocklistIPs() ([]string, error) {
	connector, err := c.getConnector()
	if err != nil {
		return nil, fmt.Errorf("failed to get ceph connection: %w", err)
	}

	cmd := []byte(`{"prefix": "osd blocklist ls", "format": "json"}`)
	buf, _, err := connector.MonCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("MonCommand osd blocklist ls: %w", err)
	}

	var entries []blocklistEntry
	dec := json.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to parse blocklist JSON: %w", err)
	}

	seen := make(map[string]struct{}, len(entries))
	var ips []string
	for _, e := range entries {
		ip := extractIPFromAddr(e.Addr)
		if ip == "" {
			continue
		}
		if _, dup := seen[ip]; !dup {
			seen[ip] = struct{}{}
			ips = append(ips, ip)
		}
	}
	return ips, nil
}

// extractIPFromAddr extracts the IP from a Ceph blocklist addr string.
// Format: "IP:port/nonce" e.g. "10.128.2.36:0/1180205774"
// or IPv6: "[2001:db8::1]:6789/1180205774"
func extractIPFromAddr(addr string) string {
	// Strip the /nonce suffix first.
	if idx := strings.IndexByte(addr, '/'); idx >= 0 {
		addr = addr[:idx]
	}
	// net.SplitHostPort handles both IPv4 "1.2.3.4:port" and IPv6 "[::1]:port".
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// buildIPToNodeMap lists CSIAddonsNode resources and builds a map from
// client IP to Kubernetes node name using NetworkFenceClientStatus CIDRs.
func buildIPToNodeMap(dynClient dynamic.Interface, ns string) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	list, err := dynClient.Resource(csiAddonsNodeGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("blocklist scan: failed to list CSIAddonsNode: %v", err)
		return nil
	}

	ipToNode := make(map[string]string)
	for _, item := range list.Items {
		spec, _ := item.Object["spec"].(map[string]interface{})
		if spec == nil {
			continue
		}
		driver, _ := spec["driver"].(map[string]interface{})
		if driver == nil {
			continue
		}
		nodeID, _ := driver["nodeID"].(string)
		if nodeID == "" {
			continue
		}
		status, _ := item.Object["status"].(map[string]interface{})
		if status == nil {
			continue
		}
		fenceStatuses, _ := status["networkFenceClientStatus"].([]interface{})
		for _, fs := range fenceStatuses {
			fsMap, _ := fs.(map[string]interface{})
			if fsMap == nil {
				continue
			}
			clientDetails, _ := fsMap["ClientDetails"].([]interface{})
			for _, cd := range clientDetails {
				cdMap, _ := cd.(map[string]interface{})
				if cdMap == nil {
					continue
				}
				cidrs, _ := cdMap["cidrs"].([]interface{})
				for _, cidr := range cidrs {
					cidrStr, _ := cidr.(string)
					if cidrStr == "" {
						continue
					}
					ipToNode[stripCIDRMask(cidrStr)] = nodeID
				}
			}
		}
	}
	return ipToNode
}

// stripCIDRMask removes the CIDR mask from an IP/CIDR string.
// "10.0.0.1/32" -> "10.0.0.1", "10.0.0.1" -> "10.0.0.1"
func stripCIDRMask(cidr string) string {
	if idx := strings.IndexByte(cidr, '/'); idx >= 0 {
		return cidr[:idx]
	}
	return cidr
}
