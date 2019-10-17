package keyvaluelist

// PartitionGetter is implemented by records that know how they want to be saved / clustered together.
type PartitionGetter interface {
	GetPartions() (KeyValues, error)
}

// MaybePartitions returns GetPartions().ToPartitionKey() if the record is of type PartitionGetter; Otherwise it returns ""
func MaybePartitions(record interface{}) string {
	if recordPartitioner, ok := record.(PartitionGetter); ok {
		maybeParts, err := recordPartitioner.GetPartions()
		if err != nil {
			return ""
		}
		return maybeParts.ToPartitionKey() + "/"
	}
	return ""
}
