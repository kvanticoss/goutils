package gcswf

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/kvanticoss/goutils/v2/writerfactory"
)

// GetGCSWriterFactory returns a writerfactory, pointing to the root of the GCS bucket.
func GetGCSWriterFactory(ctx context.Context, bucket *storage.BucketHandle) writerfactory.WriterFactory {
	return func(path string) (wc io.WriteCloser, err error) {
		return bucket.Object(path).NewWriter(ctx), nil
	}
}
