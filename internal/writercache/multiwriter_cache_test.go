package writercache_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/kvanticoss/goutils/v2/eioutil"
	"github.com/kvanticoss/goutils/v2/internal/writercache"

	"github.com/stretchr/testify/assert"
)

func TestMultiFileStreamingWithNoTimeout(t *testing.T) {

	ctx := context.Background()
	db := map[string]*bytes.Buffer{}
	wf := func(path string) (wc io.WriteCloser, err error) {
		db[path] = &bytes.Buffer{}
		return eioutil.NewWriteNOPCloser(db[path]), nil
	}

	c := writercache.NewCache(ctx, wf, time.Second*10, 10)

	tests := []struct {
		name          string
		path          string
		data          []byte
		expectedBytes []byte
	}{
		{
			name:          "Add data to path_1",
			path:          "path_1",
			data:          []byte("some_data1"),
			expectedBytes: []byte("some_data1"),
		}, {
			name:          "Add additional data to path_1",
			path:          "path_1",
			data:          []byte("some_data2"),
			expectedBytes: []byte("some_data2"),
		},

		{
			name:          "Add data to path_2",
			path:          "path_2",
			data:          []byte("some_data2"),
			expectedBytes: []byte("some_data2"),
		},

		{
			name:          "Add data to path_3",
			path:          "path_3",
			data:          []byte("some_data2"),
			expectedBytes: []byte("some_data2"),
		},

		{
			name:          "Add late data to path_1",
			path:          "path_1",
			data:          []byte("some_late_data1"),
			expectedBytes: []byte("some_late_data1"),
		},

		{
			name:          "Add late data to path_3",
			path:          "path_3",
			data:          []byte("some_late_data3"),
			expectedBytes: []byte("some_late_data3"),
		},
	}

	t.Parallel()
	for _, test := range tests {
		t.Run(t.Name(), func(t *testing.T) {
			_, err := c.Write(test.path, test.data)
			assert.NoError(t, err)
			assert.True(t, bytes.Contains(db[test.path].Bytes(), test.expectedBytes))
		})
	}

	assert.Len(t, db, 3, "Expected 3 paths to be present after 4 writes to 3 keys")
}

func TestMultiFileStreamingWithTimeout(t *testing.T) {
	mutex := sync.Mutex{}
	ctx := context.Background()
	db := map[string]*bytes.Buffer{}
	wf := func(path string) (wc io.WriteCloser, err error) {
		db[path] = &bytes.Buffer{}
		return eioutil.NewWriteCloser(db[path], func() error {
			mutex.Lock()
			defer mutex.Unlock()
			delete(db, path)
			return nil
		}), nil
	}

	ttl := time.Millisecond * 10

	c := writercache.NewCache(ctx, wf, ttl, 10)

	tests := []struct {
		name string
		path string
		data []byte
	}{
		{
			name: "Add data to path_1",
			path: "path_1",
			data: []byte("some_data1"),
		}, {
			name: "Add additional data to path_1",
			path: "path_1",
			data: []byte("some_data1_2"),
		},

		{
			name: "Add data to path_2",
			path: "path_2",
			data: []byte("some_data2_1"),
		},

		{
			name: "Add data to path_3",
			path: "path_3",
			data: []byte("some_data3_1"),
		},

		{
			name: "Add late data to path_1",
			path: "path_1",
			data: []byte("some_late_data1"),
		},

		{
			name: "Add late data to path_3",
			path: "path_3",
			data: []byte("some_late_data3"),
		},
	}

	for _, test := range tests {
		t.Run(t.Name(), func(t *testing.T) {
			_, err := c.Write(test.path, test.data)
			assert.NoError(t, err)

			mutex.Lock()
			assert.True(t, bytes.Contains(db[test.path].Bytes(), test.data))
			mutex.Unlock()

			time.Sleep(ttl * 2)

			mutex.Lock()
			assert.Zero(t, db[test.path])
			mutex.Unlock()
		})
	}
}
