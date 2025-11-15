package storage

import (
	"github.com/gernest/bsi/internal/rbf"
	"github.com/gernest/bsi/internal/storage/samples"
	"github.com/gernest/roaring"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

// SelectExemplar implements storage.ExemplarQuerier.
func (db *Store) SelectExemplar(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	var shards view
	err := db.findShardsAmy(&shards, matchers)
	if err != nil {
		return nil, err
	}

	var result samples.Samples
	result.Init()

	err = db.readExemplar(&result, &shards, start, end)
	if err != nil {
		return nil, err
	}

	err = db.translate(&result)
	if err != nil {
		return nil, err
	}
	return result.MakeExemplar(), nil
}

func (db *Store) readExemplar(result *samples.Samples, vs *view, start, end int64) error {

	return db.read(vs, start, end, true, func(tx *rbf.Tx, records *rbf.Records, ra *roaring.Bitmap, shard uint64) error {
		return readExemplars(result, tx, records, shard, ra)
	})
}
