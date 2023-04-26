// Package recordbuffer adds support for sorting large number of records by temporarly storing them in a
// byte store. This sorthing method is meant for when the size of the sorting will at some point exceed
// the available ram and disk-spooling will be needed.
// It is significantly slower than iterator.NewBufferedRecordIteratorBTree or iterator.NewBufferedClusterIteartor
// since recordbuffer will both JSON-marshal; Write; Read and Unmarshal the record before being returned. It is slower but
// more scalable as it can be backed by RAM (bytesbuffer), Disk (e.g. https://github.com/ShoshinNikita/go-disk-buffer/)
// or GCS/S3 by writing a simple wrapper for thier APIs.
package recordbuffer
