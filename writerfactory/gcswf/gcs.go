package gcswf

import (
	"context"

	"cloud.google.com/go/storage"
	"github.com/kvanticoss/goutils/eioutil"
	"github.com/kvanticoss/goutils/writerfactory"
)

// GetGCSWriterFactory returns a writerfactory, pointing to the root of the GCS bucket.
func GetGCSWriterFactory(ctx context.Context, bucket *storage.BucketHandle) writerfactory.WriterFactory {
	return func(path string) (wc eioutil.WriteCloser, err error) {
		return bucket.Object(path).NewWriter(ctx), nil
	}
}
