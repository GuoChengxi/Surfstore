package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//panic("todo")
	hashValue := blockHash.Hash
	theBlock, ok := bs.BlockMap[hashValue]

	if !ok {
		return theBlock, fmt.Errorf("Block with hash value %s is not found in Blockmap", blockHash.Hash)
	} else {
		return theBlock, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//panic("todo")
	hashValue := GetBlockHashString(block.BlockData)
	bs.BlockMap[hashValue] = block

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	//panic("todo")
	var blockHashesOut []string
	for _, blockHashIn := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[blockHashIn]
		if ok {
			blockHashesOut = append(blockHashesOut, blockHashIn)
		}
	}
	return &BlockHashes{Hashes: blockHashesOut}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	hashes := []string{}
	for _, block := range bs.BlockMap {
		hashes = append(hashes, GetBlockHashString(block.BlockData))
	}
	blockHashes := BlockHashes{Hashes: hashes} 
	return &blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
