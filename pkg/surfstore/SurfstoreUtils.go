package surfstore

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	reflect "reflect"
	"time"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//panic("todo")
	baseDir := client.BaseDir
	blockSize := client.BlockSize
	files, err := os.ReadDir(baseDir)
	check(err)

	// load index.db or create one if does not exist
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	_, err = os.Stat(metaFilePath)
	if errors.Is(err, os.ErrNotExist) {
		os.Create(metaFilePath)
	}

	// local index refers to the local meta file
	localIndex, err := LoadMetaFromMetaFile(baseDir)
	check(err)

	// store updated info about current local directory in a new fileMetaMap and also update local index
	newLocalIndex := make(map[string][]string)
	for _, file := range files {
		filename := file.Name()
		if filename == "index.db" {
			continue
		}
		f, err := os.Open(ConcatPath(baseDir, filename))
		check(err)
		defer f.Close()

		// compute hash list for each file
		var blockHashList []string
		for {
			block := make([]byte, blockSize)
			bytes_read, err := f.Read(block)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Println(err.Error())
				time.Sleep(3 * time.Second)
			}
			block = block[:bytes_read]
			hashValue := GetBlockHashString(block)
			blockHashList = append(blockHashList, hashValue)
		}
		newLocalIndex[filename] = blockHashList

		// compare the new hash list with the old local index file
		fileMetaData, ok := localIndex[filename]
		// log.Println("ok: ", ok)
		if !ok {
			// Case 1: the file is new
			localIndex[filename] = &FileMetaData{Filename: filename, Version: 1, BlockHashList: blockHashList}
		} else {
			// Case 2: the file exists in meta file (index.db)
			oldBlockHashList := fileMetaData.BlockHashList
			if !reflect.DeepEqual(blockHashList, oldBlockHashList) {
				fileMetaData.Version += 1
				fileMetaData.BlockHashList = blockHashList
			}
		}
	}

	// traverse through the local index, check if there exists deleted files and update local index
	for filename, fileMetaData := range localIndex {
		_, ok := newLocalIndex[filename]
		if !ok && !reflect.DeepEqual(fileMetaData.BlockHashList, []string{TOMBSTONE_HASHVALUE}) {
			fileMetaData.Version += 1
			fileMetaData.BlockHashList = []string{TOMBSTONE_HASHVALUE}
		}
	}

	// log.Println("Before Sync - localIndex")
	// PrintMetaMap(localIndex)

	var remoteIndex map[string]*FileMetaData
	err = client.GetFileInfoMap(&remoteIndex)
	check(err)

	blockStoreAddrs := []string{}
	err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	check(err)
	// fmt.Println("line 96: ", blockStoreAddrs)
	cHashRing := NewConsistentHashRing(blockStoreAddrs)
	// log.Println("cHashRing: ", cHashRing.ServerMap)

	// traverse through the local index and compare with remote index
	for filename, localMetaData := range localIndex {
		remoteMetaData, ok := remoteIndex[filename]
		// Case 1: if the local file is not in remote index
		if !ok {
			uploadFile(client, localMetaData, cHashRing)
		} else {
			// Case 2: if the file is in both local and remote index
			if localMetaData.Version > remoteMetaData.Version {
				uploadFile(client, localMetaData, cHashRing)
			} else if localMetaData.Version < remoteMetaData.Version {
				downloadFile(client, remoteMetaData, cHashRing)
				localIndex[filename] = remoteMetaData // update local index
			} else if localMetaData.Version == remoteMetaData.Version &&
				!reflect.DeepEqual(localMetaData.BlockHashList, remoteMetaData.BlockHashList) {
				downloadFile(client, remoteMetaData, cHashRing)
				localIndex[filename] = remoteMetaData // update local index
			}
		}
	}

	for filename, remoteMetaData := range remoteIndex {
		_, ok := localIndex[filename]
		if !ok {
			downloadFile(client, remoteMetaData, cHashRing)
			localIndex[filename] = remoteMetaData // update local index
		}
	}

	// write the updated localIndex variable back to index.db after cloud sync
	// log.Println("After Sync - localIndex:")
	// PrintMetaMap(localIndex)
	// log.Println("After Sync - remoteIndex:")
	// client.GetFileInfoMap(&remoteIndex)
	// PrintMetaMap(remoteIndex)

	err = WriteMetaFile(localIndex, baseDir)
	check(err)

	//log.Println("After Sync - localIndex:")
	//PrintMetaMap(localIndex)

}

func uploadFile(client RPCClient, localMetaData *FileMetaData, cHashRing *ConsistentHashRing) {
	file, err := os.Open(ConcatPath(client.BaseDir, localMetaData.Filename))
	check(err)
	defer file.Close()

	var latestVersion int32

	// if the file has been deleted
	if reflect.DeepEqual(localMetaData.BlockHashList, []string{TOMBSTONE_HASHVALUE}) {
		err := client.UpdateFile(localMetaData, &latestVersion)
		check(err)
		return
	}

	// store file data in BlockStore
	blockSize := client.BlockSize
	// var blockStoreAddrs []string
	var succ bool
	// client.GetBlockStoreMap()
	// err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	check(err)

	for {
		block := make([]byte, blockSize)
		byte_read, err := file.Read(block)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println(err.Error())
			time.Sleep(3 * time.Second)
		}
		block = block[:byte_read] //?
		blockHash := GetBlockHashString(block)
		blockStoreAddr := cHashRing.GetResponsibleServer(blockHash)
		// log.Println("blockStoreAddr: ", blockStoreAddr)
		err = client.PutBlock(&Block{BlockData: block, BlockSize: int32(byte_read)}, blockStoreAddr, &succ)
		check(err)
	}

	// update remote index (file) info in MetaStore
	err = client.UpdateFile(localMetaData, &latestVersion)
	check(err)

}

func downloadFile(client RPCClient, remoteMetaData *FileMetaData, cHashRing *ConsistentHashRing) {
	filepath := ConcatPath(client.BaseDir, remoteMetaData.Filename)
	file, err := os.Create(filepath)
	check(err)
	defer file.Close()

	blockHashList := remoteMetaData.BlockHashList

	// if the file has been deleted
	if reflect.DeepEqual(blockHashList, []string{TOMBSTONE_HASHVALUE}) {
		err := os.Remove(filepath)
		check(err)
		return
	}

	// get file data from BlockStores
	var fileData string
	for _, blockHash := range blockHashList {
		var block Block
		blockStoreAddr := cHashRing.GetResponsibleServer(blockHash)
		err := client.GetBlock(blockHash, blockStoreAddr, &block)
		check(err)
		fileData += string(block.BlockData)
	}

	// var blockStoreAddrs []string
	// err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	// check(err)
	// blockStoreMap := make(map[string][]string)
	// err = client.GetBlockStoreMap(blockHashList, &blockStoreMap)
	// check(err)
	// for blockStoreAddr, blockHashList := range blockStoreMap {
	// }

	// rewrite the original file
	file.WriteString(fileData)

}

func checkFunc(err error, msg string) {
	if err != nil {
		fmt.Println(msg, err.Error())
	}
}

func check(err error) {
	if err != nil {
		fmt.Println(err.Error())
	}
}

// update local index
//*localMetaData = FileMetaData{Filename: remoteMetaData.Filename, Version: remoteMetaData.Version,
//							BlockHashList: remoteMetaData.BlockHashList}
//*localMetaData = *remoteMetaData
//localMetaData = remoteMetaData
