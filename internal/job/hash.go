package job

import "hash/fnv"

func PartitionFor(key string, partitions int) int {
	if partitions <= 0 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(partitions))
}
