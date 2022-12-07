package surfstore

import (
	context "context"
	"errors"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	if block, found := bs.BlockMap[blockHash.Hash]; found {
		return block, nil
	}
	return nil, errors.New("Block not found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	h := GetBlockHashString(block.BlockData)
	success := &Success{Flag: true}
	// if _, found := bs.BlockMap[h]; found {
	// 	success.Flag = false
	// 	return success, errors.New("Block hash value already exists")
	// }
	bs.BlockMap[h] = block
	return success, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashlist_out []string
	for _, input_hash := range blockHashesIn.Hashes {
		if _, found := bs.BlockMap[input_hash]; found {
			hashlist_out = append(hashlist_out, input_hash)
		}
	}
	return &BlockHashes{Hashes: hashlist_out}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
