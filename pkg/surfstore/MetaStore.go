package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddrs []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	//panic("todo")
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//panic("todo")
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	oldfileMetaData, ok := m.FileMetaMap[filename]

	// Case 1: file exists both in local and cloud, but file version not correct (other clients sync first)
	if ok && version != oldfileMetaData.Version+1 { 
		return &Version{Version: -1}, fmt.Errorf("file version not correct") 

	} else {
	// Case 2: file exists both in local and cloud, version correct, successful update
	// Case 3: new file in local, sync to cloud
		m.FileMetaMap[filename] = fileMetaData
		return &Version{Version: version}, nil
	}
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	cHashRing := NewConsistentHashRing(m.BlockStoreAddrs)
	blockStoreMap := make(map[string]*BlockHashes)

	for _, blockHash := range blockHashesIn.Hashes {
		serverAddr := cHashRing.GetResponsibleServer(blockHash)
		hashes := blockStoreMap[serverAddr].Hashes
		hashes = append(hashes, blockHash)
		blockStoreMap[serverAddr] = &BlockHashes{Hashes: hashes}
	}

	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddrs: blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}

