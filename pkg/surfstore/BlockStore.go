package surfstore

import (
	context "context"
	"fmt"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	lock sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.lock.Lock()
	defer bs.lock.Unlock()
	if block, ok := bs.BlockMap[blockHash.Hash]; ok {
		return block, nil
	}
	return nil, fmt.Errorf("Not Found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.lock.Lock()
	defer bs.lock.Unlock()
	hash := GetBlockHashString(block.BlockData)
	print(hash)
	bs.BlockMap[hash] = block
	var success_new = new(Success)
	success_new.Flag = true
	return success_new, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.lock.Lock()
	defer bs.lock.Unlock()
	hashes := []string{}
	for _, hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[hash]; ok {
			hashes = append(hashes, hash)
		}
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
