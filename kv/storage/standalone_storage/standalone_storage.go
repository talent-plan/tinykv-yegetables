package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
		db:   nil,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.conf == nil {
		return errors.New("conf is null")
	}
	path := s.conf.DBPath
	if len(path) == 0 {
		return errors.New("path is null")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// not exist
		if err = os.Mkdir(path, 0700); err != nil {
			return err
		}
	}
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// defer os.RemoveAll(dir)
	if s.db == nil {
		return errors.New("db is null")
	}
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.New("db is null")
	}
	return &standAloneReader{s, s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return errors.New("db is null")
	}
	txn := s.db.NewTransaction(true)
	for _, modify := range batch {
		switch t := modify.Data.(type) {

		case storage.Put:
			err := engine_util.PutCFFromTxn(txn, modify.Cf(), modify.Key(), modify.Value())
			if err != nil {
				txn.Discard()
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCFFromTxn(txn, modify.Cf(), modify.Key())
			if err != nil {
				txn.Discard()
				return err
			}
		default:
			log.Info(t)
			txn.Discard()
			return errors.New("modify type is unknown")
		}
	}
	return txn.Commit()
}
