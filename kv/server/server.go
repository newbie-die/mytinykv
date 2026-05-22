package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.Version)

	// Acquire latch for the key
	server.Latches.WaitForLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})

	// Check if key is locked
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}

	// If locked by another transaction with Ts < req.Version, return lock error
	if lock != nil && lock.Ts < req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return resp, nil
	}

	// Get the value
	value, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Collect all keys to latch
	keys := make([][]byte, 0, len(req.Mutations))
	for _, mut := range req.Mutations {
		keys = append(keys, mut.Key)
	}

	// Acquire latches for all keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	// Process each mutation
	for _, mut := range req.Mutations {
		// Check for write-write conflict
		write, commitTs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}

		// If there's a write with commitTs >= startTs, it's a conflict
		if write != nil && commitTs >= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: commitTs,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			return resp, nil
		}

		// Check if key is already locked
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}

		if lock != nil {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: lock.Info(mut.Key),
			})
			return resp, nil
		}

		// Write the value to Default CF
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
		}

		// Create and write the lock
		lock = &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mut.Op),
		}
		txn.PutLock(mut.Key, lock)
	}

	// Write all modifications to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}

	// Get storage reader
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	// Create MVCC transaction
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	// Acquire latches for all keys
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	// Process each key
	for _, key := range req.Keys {
		// Get the lock
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		// Check if lock exists
		if lock == nil {
			// Lock doesn't exist, check if already committed
			write, commitTs, err := txn.CurrentWrite(key)
			if err != nil {
				return nil, err
			}

			// If already committed with the same startTs, it's idempotent - return success
			if write != nil && write.StartTS == req.StartVersion {
				return resp, nil
			}

			// If it's a rollback, return error
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "transaction has been rolled back",
				}
				return resp, nil
			}

			// Prewrite was lost, but we can still succeed (idempotent)
			continue
		}

		// Check if lock belongs to this transaction
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "lock belongs to another transaction",
			}
			return resp, nil
		}

		// Write the commit record
		write := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		}
		txn.PutWrite(key, req.CommitVersion, write)

		// Delete the lock
		txn.DeleteLock(key)
	}

	// Write all modifications to storage
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
