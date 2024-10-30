package main

const poolClusterIndex = "pool.cluster"

func indexPoolCluster(pool *Pool) []string {
	return []string{pool.GetCluster().String()}
}
