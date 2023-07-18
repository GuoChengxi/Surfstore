package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		isLeader:      false,
		isLeaderMutex: &isLeaderMutex,
		term:          0,
		metaStore:     NewMetaStore(config.BlockAddrs),
		log:           make([]*UpdateOperation, 0),

		serverId:    id,
		peerServers: config.RaftAddrs,
		commitIndex: -1, 
		lastApplied:    -1,
		pendingCommits: make([]*chan bool, 0),

		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	// panic("todo")

	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	listener, err := net.Listen("tcp", server.peerServers[server.serverId])
	fmt.Printf("RaftSurfStore listening on %s with id=%d\n", server.peerServers[server.serverId], server.serverId)
	check(err)

	return grpcServer.Serve(listener)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// for debug
func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 // not found
}