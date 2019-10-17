package test_utils

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/kvanticoss/goutils/iterator"
	"github.com/kvanticoss/goutils/keyvaluelist"
)

type dummy struct {
	ID        string
	EventTime time.Time
}

// GetPartions allows data to be partitioned before saving to GCS
func (v dummy) GetPartions() (keyvaluelist.KeyValues, error) {
	return keyvaluelist.KeyValues{
		{Key: "id", Value: v.ID},
		{Key: "date", Value: v.EventTime.UTC().Format("2006-01-02")},
		//{Key: "hour", Value: v.EventTime.UTC().Format("15")},
	}, nil
}

func (v dummy) Less(other interface{}) bool {
	vo, ok := other.(dummy)
	if !ok {
		return false
	}
	return v.EventTime.Before(vo.EventTime) || (v.EventTime.Equal(vo.EventTime) && v.ID < vo.ID)
}

func DummyIterator(ids float64, days float64, records int) iterator.LesserIterator {
	yeilded := 0

	r := rand.New(rand.NewSource(42))
	testTime := time.Date(2000, 1, 1, 1, 1, 1, 0, time.UTC)
	return func() (iterator.Lesser, error) {
		if yeilded > records {
			return nil, iterator.ErrIteratorStop
		}
		yeilded++

		return dummy{
			ID:        fmt.Sprintf("%05d", int(r.NormFloat64()*math.Sqrt(ids)+ids/2)),
			EventTime: time.Unix(testTime.Add(time.Hour*time.Duration(r.Float64()*24*days)).Unix(), int64(r.Float64()*float64(records)*10)),
		}, nil
	}
}
