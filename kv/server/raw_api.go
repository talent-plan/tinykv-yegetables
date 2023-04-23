package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	response := &kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       val,
		NotFound:    false,
	}
	if val == nil {
		response.NotFound = true
	}
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	mods := make([]storage.Modify, 0)
	mods = append(mods, storage.Modify{Data: storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}})
	return nil, server.storage.Write(nil, mods)
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	mods := make([]storage.Modify, 0)
	mods = append(mods, storage.Modify{Data: storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}})
	response := &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}
	return response, server.storage.Write(nil, mods)
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         make([]*kvrpcpb.KvPair, 0),
	}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	it := reader.IterCF(req.GetCf())
	it.Seek(req.GetStartKey())
	var count uint32
	count = 0
	for it.Valid() && count < req.GetLimit() {
		item := it.Item()
		v, err := item.Value()
		if err == nil {
			response.Kvs = append(response.Kvs, &kvrpcpb.KvPair{
				Error: nil,
				Key:   item.Key(),
				Value: v,
			})
		} else {
			return nil, err
		}
		it.Next()
		count++
	}
	return response, nil
}
