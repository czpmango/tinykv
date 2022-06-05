package server

import (
	"context"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	v, err := reader.GetCF(req.Cf, req.Key)

	return &kvrpcpb.RawGetResponse{
		Value:    v,
		NotFound: err != nil && err == badger.ErrKeyNotFound,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return nil, server.storage.Write(nil, []storage.Modify{{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}})
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return nil, server.storage.Write(nil, []storage.Modify{{Data: storage.Put{
		Key: req.Key,
		Cf:  req.Cf,
	}}})
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	cfIter := reader.IterCF(req.Cf)
	defer cfIter.Close()

	var resp kvrpcpb.RawScanResponse
	n := req.Limit

	for cfIter.Seek(req.StartKey); cfIter.Valid() && (n > 0); cfIter.Next() {
		item := cfIter.Item()
		k := item.Key()
		v, err := item.Value()
		if err != nil {
			return nil, err
		}
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: k, Value: v})
		n--
	}

	return &resp, nil
}
