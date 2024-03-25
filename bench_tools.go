package main

import (
	"flag"
	"fmt"

	_ "github.com/go-sql-driver/mysql" //导入mysql包
)

var flagUser = flag.String("user", "root", "")
var flagPassword = flag.String("password", "", "")
var flagHost = flag.String("host", "", "")
var flagPort = flag.Int("port", 0, "")
var flagDatabase = flag.String("database", "", "")
var flagWorkload = flag.String("workload", "", "insert_and_check/check_memory_usage")
var flagThreadCount = flag.Int("thread_count", 1, "")
var flagInsertBatchCount = flag.Int("insert_batch_count", 10, "")
var flagTableRowsLimit = flag.Int("table_rows_limit", 50000000, "")
var flagPdAddress = flag.String("pd", "", "")
var flagReloadData = flag.Bool("reload_data", false, "")
var flagRegionSize = flag.String("region_size", "", "")

func main() {
	flag.Parse()

	if *flagWorkload == "check_consistency_003" {
		c := newCheckConsistency003()
		c.insertAndCheck()
	} else if *flagWorkload == "check_memory_usage" {
		m := newMemoryTracker()
		m.run()
	} else {
		fmt.Printf("workload %s is not support\n", *flagWorkload)
	}
}
