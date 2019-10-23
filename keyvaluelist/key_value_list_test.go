package keyvaluelist_test

import (
	"testing"

	keyvals "github.com/kvanticoss/goutils/keyvaluelist"
	"github.com/stretchr/testify/assert"
)

func TestKeyValueToPartition(t *testing.T) {
	// Handle simple 1 case value
	assert.Equal(t, "test1=value1", keyvals.KeyValues{
		keyvals.KeyValue{Key: "test1", Value: "value1"},
	}.ToPartitionKey())

	// Handle 2 values
	assert.Equal(t, "test1=value1/test2=value2", keyvals.KeyValues{
		keyvals.KeyValue{Key: "test1", Value: "value1"},
		keyvals.KeyValue{Key: "test2", Value: "value2"},
	}.ToPartitionKey())

	// Encode exotic chars
	assert.Equal(t, "test1=value1/test2=value2/test3=%C3%A5%C3%A4%C3%B6l%2F~%23%22%21%23%3D%29%28%2F%26%25%C2%A4%23%29%5C", keyvals.KeyValues{
		keyvals.KeyValue{Key: "test1", Value: "value1"},
		keyvals.KeyValue{Key: "test2", Value: "value2"},
		keyvals.KeyValue{Key: "test3", Value: "åäöl/~#\"!#=)(/&%¤#)\\"},
	}.ToPartitionKey())
}

func TestNewKeyValuesFromPath(t *testing.T) {
	// Handle simple 1 case value
	assert.Equal(t,
		keyvals.KeyValues{
			keyvals.KeyValue{Key: "test1", Value: "value1"},
		},
		keyvals.NewKeyValuesFromPath("test1=value1"))

	// Handle 2 values
	assert.Equal(t,
		keyvals.KeyValues{
			keyvals.KeyValue{Key: "test1", Value: "value1"},
			keyvals.KeyValue{Key: "test2", Value: "value2"},
		},
		keyvals.NewKeyValuesFromPath("test1=value1/test2=value2"))

	// Encode exotic chars
	assert.Equal(t,
		keyvals.KeyValues{
			keyvals.KeyValue{Key: "test1", Value: "value1"},
			keyvals.KeyValue{Key: "test2", Value: "value2"},
			keyvals.KeyValue{Key: "test3", Value: "åäöl/~#\"!#=)(/&%¤#)\\"},
		},
		keyvals.NewKeyValuesFromPath(
			"test1=value1/test2=value2/test3=%C3%A5%C3%A4%C3%B6l%2F~%23%22%21%23%3D%29%28%2F%26%25%C2%A4%23%29%5C",
		))
}

func TestKeyValueToPrefixFilter(t *testing.T) {
	// Handle 2 values
	tests := []struct {
		keyvals keyvals.KeyValues
		path    string
		res     bool
	}{
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
			},
			path: "gs://test_NUM/test/test/test",
			res:  false,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
			},
			path: "gs://test_NUM/test1/value1",
			res:  false,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
			},
			path: "gs://test_NUM/test1=value1",
			res:  false,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
			},
			path: "gs://test_NUM/test1=value1/",
			res:  true,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
			},
			path: "gs://test_NUM/test1=value1/data.json.gz",
			res:  true,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
				keyvals.KeyValue{Key: "test2", Value: "value2"},
			},
			path: "gs://test_NUM/test1=value1/",
			res:  false,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
				keyvals.KeyValue{Key: "test2", Value: "value2"},
			},
			path: "gs://test_NUM/test1=value1/test2=value2/",
			res:  true,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
				keyvals.KeyValue{Key: "test2", Value: "value2"},
			},
			path: "gs://test_NUM/test1=value1/randomcrap/test2=value2/",
			res:  true,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
				keyvals.KeyValue{Key: "test2", Value: "value2"},
			},
			path: "gs://test_NUM/test1=value1/randomcrap/test2=value2/file.json.gz",
			res:  true,
		},
		{
			keyvals: keyvals.KeyValues{
				keyvals.KeyValue{Key: "test1", Value: "value1"},
				keyvals.KeyValue{Key: "test2", Value: "value2"},
			},
			path: "gs://test_NUM/test2=value2/randomcrap/test1=value1/file.json.gz",
			res:  true,
		},
	}

	for index, test := range tests {
		assert.Equal(t, test.res, test.keyvals.ToFilter()(test.path), "Failed test %d on path %s", index, test.path)
	}
}
