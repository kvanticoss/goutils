package keyvaluelist

import (
	"strings"
)

// KeyValues is an ordered list of KeyValue elements.
type KeyValues []KeyValue

// NewKeyValuesFromPath parses a hadoop/hive style path and returns any valid (if any) partition definitions as KeyValue pairs
func NewKeyValuesFromPath(path string) KeyValues {
	res := KeyValues{}
	for _, part := range strings.Split(path, "/") {
		if kv, err := FromHadoopPartition(part); err == nil {
			res = append(res, kv)
		}
	}
	return res
}

// ToPartitionKey formats KeyValues as key1=va1/key2=val2/.... as is common in hadoop file storage.
// keys and values are query escaped
func (keyvals KeyValues) ToPartitionKey() string {
	partitions := []string{}
	for _, kv := range keyvals {
		partitions = append(partitions, kv.ToHadoopPartition())
	}
	return strings.Join(partitions, "/")
}

// AsMap returns the ordered KeyValue list as an unordered map
func (keyvals KeyValues) AsMap() map[string]string {
	res := map[string]string{}
	for _, kv := range keyvals {
		res[kv.Key] = kv.Value
	}
	return res
}

// ToFilter turns the key values into AND:ed filename-predicates during ReadFilteredByPrefix() scans
// It will return a (filter) function(string) bool which will answers the question "Does all the key value pairs exist as
// hadoop encoded partition keys in the provided string". Only supports exact matches.
func (keyvals KeyValues) ToFilter() func(string) bool {
	return func(path string) bool {
		for _, kv := range keyvals {
			if !strings.Contains(path, kv.ToHadoopPartition()+"/") {
				return false
			}
		}
		return true
	}
}
