package main

import (
	"flag"

	_ "github.com/go-sql-driver/mysql" //导入mysql包
)

var flagUser = flag.String("user", "root", "")
var flagPassword = flag.String("password", "", "")
var flagHost = flag.String("host", "", "")
var flagPort = flag.Int("port", 0, "")
var flagDatabase = flag.String("database", "", "")
var flagWorkload = flag.String("workload", "", "insert_and_check")
var flagThreadCount = flag.Int("thread_count", 1, "")
var flagInsertBatchCount = flag.Int("insert_batch_count", 10, "")

func main() {
	flag.Parse()

	if *flagWorkload == "insert_and_check" {
		c := newCheckConsistency()
		c.insertAndCheck()
	}
}
