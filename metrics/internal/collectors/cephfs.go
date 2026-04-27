package collectors

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/ceph/go-ceph/cephfs/admin"
	"github.com/prometheus/client_golang/prometheus"
	cephconn "github.com/red-hat-storage/ocs-operator/metrics/v4/internal/ceph"
	rookclient "github.com/rook/rook/pkg/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

var _ prometheus.Collector = &CephFSSubvolumeCountCollector{}

type cephfsGroupData struct {
	consumerName string
	volume       string
	group        string
	subvolumes   map[string]string // subvolume name -> PV name
}

type cephfsCacheSnapshot struct {
	groups                     []cephfsGroupData
	subvolumesByConsumer       map[string]int // consumer_name -> subvolume count
	snapshotContentsByConsumer map[string]int // consumer_name -> snapshot content count
}

type CephFSSubvolumeCountCollector struct {
	conn                 *cephconn.Conn
	rookClient           rookclient.Interface
	namespace            string
	scanInterval         time.Duration
	pvMetadata           *prometheus.Desc
	subvolumeCount       *prometheus.Desc
	snapshotContentCount *prometheus.Desc
	cache                atomic.Pointer[cephfsCacheSnapshot]
	newFSAdmin           func() (fsAdmin, error)
}

func NewCephFSSubvolumeCountCollector(conn *cephconn.Conn, rookClient rookclient.Interface, ns string, scanInterval time.Duration) *CephFSSubvolumeCountCollector {
	c := &CephFSSubvolumeCountCollector{
		conn:         conn,
		rookClient:   rookClient,
		namespace:    ns,
		scanInterval: scanInterval,
		pvMetadata: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "pv_metadata"),
			"Attributes of CephFS based Persistent Volume",
			[]string{"name", "subvolume", "volume", "subvolume_group", "consumer_name"},
			nil,
		),
		subvolumeCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "subvolume_count"),
			"Number of CephFS subvolumes per storage consumer",
			[]string{"consumer_name"}, nil,
		),
		snapshotContentCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, "cephfs", "snapshot_content_count"),
			"Number of CephFS snapshot contents per storage consumer",
			[]string{"consumer_name"}, nil,
		),
	}
	c.newFSAdmin = func() (fsAdmin, error) {
		radosConn, err := c.conn.Get()
		if err != nil {
			return nil, err
		}
		return admin.NewFromConn(radosConn), nil
	}
	return c
}

func (c *CephFSSubvolumeCountCollector) Run(stopCh <-chan struct{}) {
	runScanLoop(stopCh, c.scanInterval, c.runScan)
}

func (c *CephFSSubvolumeCountCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pvMetadata
	ch <- c.subvolumeCount
	ch <- c.snapshotContentCount
}

func (c *CephFSSubvolumeCountCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.cache.Load()
	if snap == nil {
		klog.Warning("CephFS cache not yet populated, skipping")
		return
	}

	for consumer, count := range snap.subvolumesByConsumer {
		ch <- prometheus.MustNewConstMetric(c.subvolumeCount,
			prometheus.GaugeValue, float64(count), consumer)
	}

	for consumer, count := range snap.snapshotContentsByConsumer {
		ch <- prometheus.MustNewConstMetric(c.snapshotContentCount,
			prometheus.GaugeValue, float64(count), consumer)
	}

	for _, g := range snap.groups {
		for svName, pvName := range g.subvolumes {
			ch <- prometheus.MustNewConstMetric(c.pvMetadata,
				prometheus.GaugeValue, 1,
				pvName, svName, g.volume, g.group, g.consumerName,
			)
		}
	}
}

func (c *CephFSSubvolumeCountCollector) reconnect() {
	if c.conn != nil {
		c.conn.Reconnect()
	}
}

func (c *CephFSSubvolumeCountCollector) runScan() bool {
	start := time.Now()

	fsa, err := c.newFSAdmin()
	if err != nil {
		klog.Errorf("cephfs scan: failed to get ceph connection: %v", err)
		c.reconnect()
		return false
	}

	volumes, err := fsa.ListVolumes()
	if err != nil {
		klog.Errorf("cephfs scan: failed to list volumes: %v", err)
		c.reconnect()
		return false
	}

	groupToConsumer := buildSubVolumeGroupToConsumerMap(c.rookClient, c.namespace)

	var groups []cephfsGroupData
	anyVolumeSucceeded := false
	subvolumesByConsumer := make(map[string]int)
	snapshotContentsByConsumer := make(map[string]int)
	pvLinkedSubvolumes := 0

	for _, volume := range volumes {
		svGroups, err := fsa.ListSubVolumeGroups(volume)
		if err != nil {
			klog.Errorf("cephfs scan: failed to list subvolume groups for %s: %v", volume, err)
			continue
		}
		anyVolumeSucceeded = true

		for _, group := range svGroups {
			subvolNames, err := fsa.ListSubVolumes(volume, group)
			if err != nil {
				klog.Errorf("cephfs scan: failed to list subvolumes for %s/%s: %v", volume, group, err)
				continue
			}
			consumerName := groupToConsumer[group]
			subvolumesByConsumer[consumerName] += len(subvolNames)

			subvolumes := make(map[string]string, len(subvolNames))
			for _, sv := range subvolNames {
				snapshots, err := fsa.ListSubVolumeSnapshots(volume, group, sv)
				if err != nil {
					klog.V(4).Infof("cephfs scan: failed to list snapshots for %s/%s/%s: %v", volume, group, sv, err)
				} else {
					snapshotContentsByConsumer[consumerName] += len(snapshots)
				}

				pvName, err := fsa.GetMetadata(volume, group, sv, pvMetadataKey)
				if err != nil {
					klog.V(4).Infof("cephfs scan: no PV metadata for %s/%s/%s: %v", volume, group, sv, err)
					continue
				}
				subvolumes[sv] = pvName
			}

			if len(subvolumes) > 0 {
				groups = append(groups, cephfsGroupData{
					consumerName: groupToConsumer[group],
					volume:       volume,
					group:        group,
					subvolumes:   subvolumes,
				})
				pvLinkedSubvolumes += len(subvolumes)
			}
		}
	}

	if len(volumes) > 0 && !anyVolumeSucceeded {
		klog.Error("cephfs scan: failed for all volumes, reconnecting")
		c.reconnect()
		return false
	}

	totalSubvolumes := 0
	for _, count := range subvolumesByConsumer {
		totalSubvolumes += count
	}

	c.cache.Store(&cephfsCacheSnapshot{
		groups:                     groups,
		subvolumesByConsumer:       subvolumesByConsumer,
		snapshotContentsByConsumer: snapshotContentsByConsumer,
	})

	klog.Infof("cephfs scan: completed in %v, groups=%d, subvolumes=%d (with PV: %d)",
		time.Since(start), len(groups), totalSubvolumes, pvLinkedSubvolumes)
	return true
}

func buildSubVolumeGroupToConsumerMap(client rookclient.Interface, ns string) map[string]string {
	groupToConsumer := make(map[string]string)
	if client == nil {
		return groupToConsumer
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	svgList, err := client.CephV1().CephFilesystemSubVolumeGroups(ns).List(
		ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("failed to list CephFilesystemSubVolumeGroups: %v", err)
		return groupToConsumer
	}

	for _, svg := range svgList.Items {
		groupName := svg.Spec.Name
		if groupName == "" {
			groupName = svg.Name
		}
		if name := consumerOwnerName(svg.OwnerReferences); name != "" {
			groupToConsumer[groupName] = name
		}
	}
	return groupToConsumer
}
