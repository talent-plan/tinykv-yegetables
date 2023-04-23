package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

var (
	NotFoundKeyError = "Key not found"
)

type standAloneReader struct {
	storage *StandAloneStorage
	txn     *badger.Txn
}

func (s *standAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err != nil && err.Error() == NotFoundKeyError {
		return nil, nil
	}
	return v, err
}
func (s *standAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}
func (s *standAloneReader) Close() {
	s.txn.Commit()
}
