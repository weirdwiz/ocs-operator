package collectors

import (
	"github.com/ceph/go-ceph/rbd"
)

type rbdImage interface {
	GetMetadata(key string) (string, error)
	GetSnapshotNames() ([]rbd.SnapInfo, error)
	ListChildren() ([]string, []string, error)
	Close() error
}

type rbdConnector interface {
	ListPools() ([]string, error)
	MonCommand(args []byte) ([]byte, string, error)
	OpenIOContext(pool string) (rbdIOContext, error)
}

type rbdIOContext interface {
	SetNamespace(ns string)
	Destroy()
}

type rbdPoolOps interface {
	GetImageNames(ioctx rbdIOContext) ([]string, error)
	NamespaceList(ioctx rbdIOContext) ([]string, error)
	GetMirrorMode(ioctx rbdIOContext) (rbd.MirrorMode, error)
	MirrorImageGlobalStatusList(ioctx rbdIOContext) ([]rbd.GlobalMirrorImageIDAndStatus, error)
	ListMirrorPeerSite(ioctx rbdIOContext) ([]*rbd.MirrorPeerSite, error)
	OpenImageReadOnly(ioctx rbdIOContext, name string) (rbdImage, error)
}
