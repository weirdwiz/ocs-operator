package collectors

import (
	"github.com/ceph/go-ceph/rados"
	"github.com/ceph/go-ceph/rbd"
)

type radosConnWrapper struct {
	conn *rados.Conn
}

func (w *radosConnWrapper) ListPools() ([]string, error) {
	return w.conn.ListPools()
}

func (w *radosConnWrapper) MonCommand(args []byte) ([]byte, string, error) {
	return w.conn.MonCommand(args)
}

func (w *radosConnWrapper) OpenIOContext(pool string) (rbdIOContext, error) {
	ioctx, err := w.conn.OpenIOContext(pool)
	if err != nil {
		return nil, err
	}
	return &radosIOContextWrapper{ioctx: ioctx}, nil
}

type radosIOContextWrapper struct {
	ioctx *rados.IOContext
}

func (w *radosIOContextWrapper) SetNamespace(ns string) {
	w.ioctx.SetNamespace(ns)
}

func (w *radosIOContextWrapper) Destroy() {
	w.ioctx.Destroy()
}

func (w *radosIOContextWrapper) raw() *rados.IOContext {
	return w.ioctx
}

type realRbdPoolOps struct{}

func (r *realRbdPoolOps) GetImageNames(ioctx rbdIOContext) ([]string, error) {
	return rbd.GetImageNames(ioctx.(*radosIOContextWrapper).raw())
}

func (r *realRbdPoolOps) NamespaceList(ioctx rbdIOContext) ([]string, error) {
	return rbd.NamespaceList(ioctx.(*radosIOContextWrapper).raw())
}

func (r *realRbdPoolOps) GetMirrorMode(ioctx rbdIOContext) (rbd.MirrorMode, error) {
	return rbd.GetMirrorMode(ioctx.(*radosIOContextWrapper).raw())
}

func (r *realRbdPoolOps) MirrorImageGlobalStatusList(ioctx rbdIOContext) ([]rbd.GlobalMirrorImageIDAndStatus, error) {
	return rbd.MirrorImageGlobalStatusList(ioctx.(*radosIOContextWrapper).raw(), "", 0)
}

func (r *realRbdPoolOps) ListMirrorPeerSite(ioctx rbdIOContext) ([]*rbd.MirrorPeerSite, error) {
	return rbd.ListMirrorPeerSite(ioctx.(*radosIOContextWrapper).raw())
}

func (r *realRbdPoolOps) OpenImageReadOnly(ioctx rbdIOContext, name string) (rbdImage, error) {
	return rbd.OpenImageReadOnly(ioctx.(*radosIOContextWrapper).raw(), name, rbd.NoSnapshot)
}
