package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	fmt.Printf("GetBlock Success: blockData \"%s\"; blockSize %d\n", block.BlockData, block.BlockSize)

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)

	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	fmt.Printf("PutBlock Success: %v \n", success.GetFlag())
	*succ = success.Flag

	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	fmt.Printf("HasBlocks Success")
	*blockHashesOut = blockHashes.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)
	c := NewBlockStoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	BlockHashes, err := c.GetBlockHashes(ctx, &empty.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	fmt.Printf("GetBlockHashes Success")
	*blockHashes = BlockHashes.Hashes

	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	//panic("todo")

	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		check(err)

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &empty.Empty{})

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		fmt.Printf("MetaStoreAddr %s: GetFileInfoMap Success\n", metaStoreAddr)
		*serverFileInfoMap = fileInfoMap.FileInfoMap

		return conn.Close()
	}
	return fmt.Errorf("all servers down")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//panic("todo")

	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		check(err)

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		version, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		fmt.Printf("MetaStoreAddr %s: UpdateFile Success\n", metaStoreAddr)
		//log.Println("version: ", version)
		*latestVersion = version.Version

		return conn.Close()
	}
	return fmt.Errorf("all servers down")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// panic("todo")

	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		check(err)

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		theBlockStoreMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		fmt.Printf("MetaStoreAddr %s: GetBlockStoreMap Success\n", metaStoreAddr)

		theblockStoreMap := make(map[string][]string)
		for serverAddr, BlockHashes := range theBlockStoreMap.BlockStoreMap {
			theblockStoreMap[serverAddr] = BlockHashes.Hashes
		}
		*blockStoreMap = theblockStoreMap

		return conn.Close()
	}
	return fmt.Errorf("all servers down")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// panic("todo")

	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		check(err)

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		BlockStoreAddrs, err := c.GetBlockStoreAddrs(ctx, &empty.Empty{})

		if err != nil {
			if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
				continue
			}
			if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
				continue
			}
			conn.Close()
			return err
		}
		*blockStoreAddrs = BlockStoreAddrs.BlockStoreAddrs
		fmt.Printf("MetaStoreAddr %s: GetBlockStoreAddrs Success: %s \n", metaStoreAddr, *blockStoreAddrs)

		return conn.Close()
	}
	return fmt.Errorf("all servers down")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
