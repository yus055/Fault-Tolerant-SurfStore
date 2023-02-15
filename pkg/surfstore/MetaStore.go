package surfstore

import (
	context "context"
	"fmt"
	"sync"

	proto "google.golang.org/protobuf/proto"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
	my sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.my.Lock()
	defer m.my.Unlock()
	return proto.Clone(&FileInfoMap{FileInfoMap: m.FileMetaMap}).(*FileInfoMap), nil

}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.my.Lock()
	defer m.my.Unlock()
	if value, ok := m.FileMetaMap[fileMetaData.Filename]; ok {
		if value.Version+1 == fileMetaData.Version {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		} else {
			return nil, fmt.Errorf("The version of the update file is wrong!")
		}
	} else {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	}
	var ver = new(Version)
	ver.Version = m.FileMetaMap[fileMetaData.Filename].Version
	return ver, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.my.Lock()
	defer m.my.Unlock()
	var addr = new(BlockStoreAddr)
	addr.Addr = m.BlockStoreAddr
	return addr, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
