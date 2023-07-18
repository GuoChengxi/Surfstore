package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	serverId       int64
	peerServers    []string // metaStore addresses of raft servers
	commitIndex    int64    // index of highest log entry known to be committed
	lastApplied    int64    // index of highest log entry applied to state machine
	pendingCommits []*chan bool

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// panic("todo")
	if s.getIsCrashed() {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.getIsLeader() {
		return nil, ERR_NOT_LEADER
	}
	for {
		majorityWork, err := s.SendHeartbeat(ctx, empty)
		check(err)
		if err == nil && majorityWork.Flag {
			break
		}
	}

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	if s.getIsCrashed() {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.getIsLeader() {
		return nil, ERR_NOT_LEADER
	}
	for {
		majorityWork, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		check(err)
		if err == nil && majorityWork.Flag {
			break
		}
	}

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	if s.getIsCrashed() {
		return nil, ERR_SERVER_CRASHED
	}
	if !s.getIsLeader() {
		return nil, ERR_NOT_LEADER
	}
	for {
		majorityWork, err := s.SendHeartbeat(ctx, empty)
		check(err)
		if err == nil && majorityWork.Flag {
			break
		}
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// panic("todo")
	if !s.getIsLeader() {
		return &Version{Version: filemeta.Version}, ERR_NOT_LEADER
	}
	if s.getIsCrashed() {
		return &Version{Version: filemeta.Version}, ERR_SERVER_CRASHED
	}

	// append entry to my own log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})

	// if a majority of servers down, block, and commit until they recover
	for {
		majorityWork, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		check(err)
		if err == nil && majorityWork.Flag {
			break
		}
	}

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	go s.sendToAllFollowersInParallel(ctx, len(s.pendingCommits)-1)
	
	hasCommit := <-commitChan

	if hasCommit {
		s.lastApplied = s.commitIndex
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return &Version{Version: filemeta.Version}, fmt.Errorf("commit failure")
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context, pendCommitIdx int) {
	// send AppendEntry RPC to all my followers and count the replicas

	replies := make(chan bool, len(s.peerServers)-1)
	for idx, serverAddr := range s.peerServers {
		if int64(idx) == s.serverId {
			continue
		}
		go s.sendToFollower(ctx, serverAddr, replies)
	}

	totalReplies := 1 // count myself
	totalOK := 1

	// wait in loop for responses
	for {
		replyIsOK := <-replies
		if replyIsOK {
			totalOK++
		}
		totalReplies++
		if totalReplies == len(s.peerServers) {
			break
		}
	}

	if totalOK > len(s.peerServers)/2 {
		*s.pendingCommits[pendCommitIdx] <- true
		s.commitIndex++
	} else {
		*s.pendingCommits[pendCommitIdx] <- false
	}

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, serverAddr string, replies chan bool) {
	prevLogIndex := s.commitIndex
	var prevLogTerm int64
	if prevLogIndex == -1 {
		prevLogTerm = 0 // not applicable if log has no entry yet
	} else {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  prevLogTerm,  // index of log entry immediately preceding new ones
		PrevLogIndex: prevLogIndex, // term of prevLogIndex entry
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	check(err)

	client := NewRaftSurfstoreClient(conn)

	fmt.Printf("SendToFollower: Server %d calls server %d to appendEntry: Term %d, PrevLogTerm %d, PrevLogIndex %d, Entries length %d\n",
		s.serverId, indexOf(serverAddr, s.peerServers), s.term, prevLogTerm, prevLogIndex, len(s.log))

	output, err := client.AppendEntries(ctx, input)
	checkFunc(err, "[sendToFollower]")

	if output != nil && output.Success {
		replies <- true
	} else if err != nil {
		if strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) { // note that RPC appends additional syntax to the returned error variable
			replies <- false
		} else if strings.Contains(err.Error(), ERR_NOT_LEADER.Error()) {
			replies <- false
			s.setIsLeader(false)
		} else {
			return
		}
	}

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// panic("todo")

	if s.getIsCrashed() {
		return nil, ERR_SERVER_CRASHED
	}

	appendEntryOutput := new(AppendEntryOutput)

	// fmt.Println("AppendEntries new log:", input.Entries, "(for serverId:", s.serverId, ")")
	// fmt.Println("AppendEntries original log:", s.log, "(for serverId:", s.serverId, ")")
	fmt.Printf("Server %d: AppendEntries new log length: %d\n", s.serverId, len(input.Entries))
	fmt.Printf("Server %d: AppendEntries original log length: %d\n", s.serverId, len(s.log))

	// Reply false if leader's term < current server's term (§5.1) ("leader" who called AppendEntries should be converted to follower)
	if input.Term < s.term {
		return nil, ERR_NOT_LEADER
	}

	// Update current server's term and if it was leader, convert to follower
	if input.Term > s.term {
		s.term = input.Term
		s.setIsLeader(false)
	}

	// Delete incorrect entries, and append any new entries not already in the log
	for {
		if input.PrevLogIndex == -1 {
			break
		}
		if input.PrevLogIndex > int64(len(s.log)-1) || 
				s.log[input.PrevLogIndex].Term != input.Entries[input.PrevLogIndex].Term {
			input.PrevLogIndex -= 1
		} else {
			break
		}
	}
	
	s.log = s.log[:(input.PrevLogIndex + 1)]

	s.log = append(s.log, input.Entries[input.PrevLogIndex+1:]...)

	// s.log = nil
	// s.log = append(s.log, input.Entries...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	// of last new entry)
	for s.commitIndex < input.LeaderCommit {
		newEntry := s.log[s.commitIndex+1]
		s.metaStore.UpdateFile(ctx, newEntry.FileMetaData)
		s.commitIndex++
		// s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
		fmt.Printf("Server %d committed index %d\n", s.serverId, s.commitIndex)
	}
	s.lastApplied = s.commitIndex

	// appendEntryOutput.ServerId = s.serverId
	// appendEntryOutput.Term = s.term
	appendEntryOutput.Success = true
	// appendEntryOutput.MatchedIndex = s.commitIndex

	// fmt.Println("AppendEntries modified log:", s.log, "(for serverId:", s.serverId, ")")
	fmt.Printf("Server %d: AppendEntries modified log length: %d\n", s.serverId, len(s.log))

	return appendEntryOutput, nil

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")

	if s.getIsCrashed() {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.setIsLeader(true)
	s.term++

	fmt.Printf("Server %d was just set as leader [term: %d]\n", s.serverId, s.term)

	s.SendHeartbeat(ctx, &emptypb.Empty{})
	return &Success{Flag: true}, nil
}

// You are guaranteed to have SendHeartbeat called:
// 1. After every call to SetLeader the node that had SetLeader called will have SendHeartbeat called
// 2. After every UpdateFile call the node that had UpdateFile called will have SendHeartbeat called
// 3. After the test, the leader will have SendHeartbeat called one final time. Then all of the nodes
//    should be ready for the internal state to be collected through GetInternalState
// 4. After every SyncClient operation

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// panic("todo")

	if s.getIsCrashed() {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	if !s.getIsLeader() {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	// send AppendEntries RPC to all the followers
	// time.Sleep(time.Second * 2)
	replies := make(chan bool, len(s.peerServers)-1)
	for idx, serverAddr := range s.peerServers {
		if int64(idx) == s.serverId {
			continue
		}
		go s.sendToFollower(ctx, serverAddr, replies)
	}

	totalReplies := 1 // count myself
	totalOK := 1

	// wait in loop for responses
	for {
		replyIsOK := <-replies
		if replyIsOK {
			totalOK++
		}
		totalReplies++
		if totalReplies == len(s.peerServers) {
			break
		}
	}

	var success bool
	if totalOK > len(s.peerServers)/2 {
		success = true
	} else {
		success = false
	}

	return &Success{Flag: success}, nil
}

func (s *RaftSurfstore) getIsCrashed() bool {
	s.isCrashedMutex.RLock()
	serverIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	return serverIsCrashed
}

func (s *RaftSurfstore) getIsLeader() bool {
	s.isLeaderMutex.RLock()
	serverIsLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	return serverIsLeader
}

func (s *RaftSurfstore) setIsLeader(isLeader bool) {
	s.isLeaderMutex.Lock()
	s.isLeader = isLeader
	s.isLeaderMutex.Unlock()
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
