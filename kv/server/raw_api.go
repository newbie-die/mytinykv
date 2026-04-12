package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	r, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, nil
	}
	defer r.Close()

	val, err := r.GetCF(req.GetCf(), req.GetKey())
	resp := &kvrpcpb.RawGetResponse{}
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if val == nil {
		resp.NotFound = true
	} else {
		resp.Value = val
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	mod := []storage.Modify{{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}}
	err := server.storage.Write(req.GetContext(), mod)
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	mod := []storage.Modify{{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}}
	err := server.storage.Write(req.GetContext(), mod)
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	r, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, nil
	}
	defer r.Close()

	iter := r.IterCF(req.GetCf())
	defer iter.Close()

	var kvs []*kvrpcpb.KvPair
	limit := req.GetLimit()
	iter.Seek(req.GetStartKey())
	for i := uint32(0); i < limit && iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Error: &kvrpcpb.KeyError{}})
		} else {
			kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: val})
		}
		i++
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
