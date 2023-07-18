package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// panic("todo")

	// sort the hash values
	hashValues := []string{}
	for hashValue := range c.ServerMap {
		hashValues = append(hashValues, hashValue)
	}
	sort.Strings(hashValues)

	// locate the corresponding server
	// (find the server with the largest value that is smaller than blockId)
	responsibleServer := ""
	theHashValue := blockId
	// log.Println("theHashValue: ", theHashValue)
	for _, hashValue := range hashValues {
		if hashValue > theHashValue {
			responsibleServer = c.ServerMap[hashValue]
			break
		}
		// log.Printf("i: %d; hashValue: %s \n", i, hashValue)
	}
	if responsibleServer == "" {
		// fmt.Println("c.ServerMap:", c.ServerMap)
		responsibleServer = c.ServerMap[hashValues[0]]
	}
	// log.Println("GetResponsibleServer - line42: ", responsibleServer)

	return strings.TrimPrefix(responsibleServer, "blockstore")
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	serverMap := make(map[string]string)
	consistentHashRing := ConsistentHashRing{ServerMap: serverMap}
	for _, serverAddr := range serverAddrs {
		hashValue := consistentHashRing.Hash("blockstore" + serverAddr)
		consistentHashRing.ServerMap[hashValue] = "blockstore" + serverAddr
		// fmt.Println("NewConsistentHashRing: " + serverAddr + ": " + hashValue)
	}

	return &consistentHashRing
}
