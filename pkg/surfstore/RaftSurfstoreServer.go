package surfstore

import (
	context "context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore
	//I add s(ta)
	commitIndex    int64
	pendingCommits []chan bool

	lastApplied int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond
	next_index    []int
	match_index   []int

	rpcClients []RaftSurfstoreClient
	//I add l(ta)
	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if s.isCrashed {
		fmt.Printf("%d is crashed.", s.serverId)
		return nil, ERR_SERVER_CRASHED
	}
	if s.isLeader == false {
		return nil, ERR_NOT_LEADER
	}

	out, err := s.metaStore.GetFileInfoMap(ctx, empty)
	return out, err
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	out, err := s.metaStore.GetBlockStoreAddr(ctx, empty)
	return out, err
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isLeader == false {
		return nil, ERR_NOT_LEADER
	}
	if s.isCrashed == true {
		return nil, ERR_SERVER_CRASHED
	}
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, &op)
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	go s.attemptCommit(ctx)
	select {
	case success := <-committed:
		if success && s.isLeader && !s.isCrashed {
			s.lastApplied = s.lastApplied + 1

			return s.metaStore.UpdateFile(ctx, filemeta)
		}
	case <-time.After(time.Duration(10) * time.Second):
		s.log = append(s.log, &op)
		s.metaStore.UpdateFile(ctx, op.FileMetaData)
		s.commitIndex = 0
		return nil, nil
	}

	return nil, nil

}

//I add s(ta)
func (s *RaftSurfstore) attemptCommit(ctx context.Context) {
	targetIdx := int64(len(s.pendingCommits) - 1)
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		go s.commitEntry(int64(idx), targetIdx, commitChan, ctx)
	}
	if s.isCrashed || !s.isLeader {
		return
	}
	commitCount := 1
	for s.isLeader && !s.isCrashed {
		select {
		case commit := <-commitChan:
			if commit != nil && commit.Success {
				commitCount++
			}
			if commitCount > len(s.ipList)/2 {
				s.pendingCommits[targetIdx] <- true
				s.commitIndex = s.commitIndex + 1
				break
			}

		case <-time.After(time.Duration(10) * time.Second):
			return
		}

	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput, ctx context.Context) {
	index := entryIdx - 1
	for s.isLeader && !s.isCrashed {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)

		a := 0
		if index >= 0 {
			a = int(math.Max(float64(s.log[index].Term), 0))
		}

		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  int64(a),
			PrevLogIndex: index,
			Entries:      s.log[index+1 : entryIdx+1],
			LeaderCommit: s.commitIndex,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if err != nil && strings.Contains(err.Error(), ERR_NOT_MATCH.Error()) {
			index -= 1
			continue
		}
		if err != nil && strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
			time.Sleep(time.Millisecond * 200)
			return
		}

		if output != nil && output.Success {
			commitChan <- output
			return
		}

	}
}

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	//crashed
	if s.isCrashed == true {
		return nil, ERR_SERVER_CRASHED
	}

	output := &AppendEntryOutput{
		ServerId:     s.serverId,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	//1. Reply false if term < currentTerm
	if input.Term < s.term {
		output.Success = false
		return output, nil
	}
	if input.Term > s.term {
		s.term = input.Term
		s.isLeader = false
	}
	//2. Reply false if log doesn't contain an entry at prevLogIndex whose term
	//matches prevLogTerm

	if input.PrevLogIndex >= 0 && int(input.PrevLogIndex) > len(s.log)-1 {
		return nil, ERR_NOT_MATCH
	}
	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it
	if input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		s.log = s.log[:input.PrevLogIndex]
		return nil, ERR_NOT_MATCH
	}

	s.log = s.log[:input.PrevLogIndex+1]
	if input.PrevLogIndex == -1 && input.Entries != nil {
		s.log = make([]*UpdateOperation, 0)
	}
	s.log = append(s.log, input.Entries...)

	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	//of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}
	output.Success = true

	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if s.isCrashed == true {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	for key, _ := range s.next_index {
		if key == int(s.serverId) {
			continue
		}
		s.next_index[key] = len(s.log)
		s.match_index[key] = -1
	}
	s.isLeader = true
	s.term++
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//crash

	if s.isCrashed == true {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	// not leader
	if s.isLeader == false {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}
	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// create correct AppendEntryInput from s.nextIndex, etc
		beat_term := 0
		if len(s.log) > 1 {
			beat_term = int(math.Max(float64(s.log[len(s.log)-2].Term), 0))
		}
		beat_index := int(math.Max(float64(len(s.log)-2), -1))
		for s.isLeader && !s.isCrashed {
			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  int64(beat_term),
				PrevLogIndex: int64(beat_index),
				//figure out which entries to send
				Entries:      s.log[beat_index+1 : len(s.log)],
				LeaderCommit: s.commitIndex,
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			output, err2 := client.AppendEntries(ctx, input)
			if err2 != nil && strings.Contains(err2.Error(), ERR_NOT_MATCH.Error()) {
				beat_index -= 1
				if beat_index >= 0 {
					beat_term = int(s.log[beat_index].Term)
				} else {
					beat_term = 0
				}
				continue
			}
			if err2 != nil && strings.Contains(err2.Error(), ERR_SERVER_CRASHED.Error()) {
				break
			}
			if output != nil {
				// server is alive
				if output.Term > s.term && output.Success == true {
					s.term = output.Term
					return &Success{Flag: true}, nil
				}
				break
			}
		}
	}

	return &Success{Flag: true}, nil
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
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)

	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
