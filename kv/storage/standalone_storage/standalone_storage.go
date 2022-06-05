package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Kv     *badger.DB
	KvPath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Create a badger.DB instance
	kvDb:=engine_util.CreateDB(conf.DBPath, conf.Raft);
	return &StandAloneStorage{
		Kv: kvDb,
		KvPath: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// FIXME: Start storage daemon?
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if err := s.Kv.Close(); err != nil {
		return err
	}

	return nil
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
}

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(reader.storage.Kv, cf, key)
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// TBD: Start a ReadOnly transaction?
    txn := reader.storage.Kv.NewTransaction(false);
	// Note: txn cannot be discarded bcz its iter is still being used as a return value
	// defer txn.Discard()

	return engine_util.NewCFIterator(cf, txn)
}

func (reader *StandAloneStorageReader) Close() {}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{storage:s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if len(batch) > 0 {
		err := s.Kv.Update(func(txn *badger.Txn) error {
			for _, entry := range batch {
					var err1 error
					k := engine_util.KeyWithCF(entry.Cf(),entry.Key())
					v := entry.Value()
			
					if len(v) == 0 {
							err1 = txn.Delete(k)
					} else {
							err1 = txn.Set(k, v)
					}
					if err1 != nil {
							return err1
					}
			}
			return nil
	})
	return err
}
	return nil
}
