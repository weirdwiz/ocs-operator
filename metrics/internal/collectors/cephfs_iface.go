package collectors

type fsAdmin interface {
	ListVolumes() ([]string, error)
	ListSubVolumeGroups(volume string) ([]string, error)
	ListSubVolumes(volume, group string) ([]string, error)
	ListSubVolumeSnapshots(volume, group, sv string) ([]string, error)
	GetMetadata(volume, group, sv, key string) (string, error)
}
