package main

import (
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

type MemoryTracker struct {
	sqlSelect   string
	threadCount int
	dbInfo      *Database
}

func newMemoryTracker() MemoryTracker {
	t := MemoryTracker{
		sqlSelect:   "select * from lineitem",
		threadCount: *flagThreadCount,
		dbInfo:      newDatabase(),
	}
	return t
}

func execCmd(name string, args ...string) {
	c := exec.Command(name, args...)
	out, err := c.CombinedOutput()
	fmt.Printf("%s\n%s\n", c.String(), string(out))
	if err != nil {
		panic(err)
	}
}

func (m *MemoryTracker) cleanup() {
	execCmd("tiup", "bench", "tpch", "cleanup",
		"-H", m.dbInfo.host,
		"-P", strconv.Itoa(m.dbInfo.port),
		"-D", m.dbInfo.database)
}

func (m *MemoryTracker) prepare() {
	execCmd("tiup", "br", "restore", "db",
		"--db", "tpch10",
		"--send-credentials-to-tikv=true",
		"--pd", *flagPdAddress,
		"--storage=s3://benchmark/tpch10",
		"--s3.endpoint=http://minio.pingcap.net:9000",
		"--check-requirements=false")
}

func (m *MemoryTracker) checkPrepare() {
	c := m.dbInfo.newConnection()
	c.setDatabaseTiFlashReplicaAndWaitAvailable("tpch10", 1)
	execCmd("tiup", "bench", "tpch", "--count=1", "--sf=10", "--check=true", "run",
		"-H", m.dbInfo.host,
		"-P", strconv.Itoa(m.dbInfo.port),
		"-D", m.dbInfo.database)
}

func (m *MemoryTracker) selectData() {
	c := m.dbInfo.newConnection()
	c.setReadEngines("tiflash")
	defer c.db.Close()
	for {
		rows := c.query(m.sqlSelect)
		count := 0
		for rows.Next() {
			rows.Scan()
			count++
		}
		rows.Close()
		fmt.Printf("select count: %d\n", count)
	}
}

// ./bench_tools --host 10.2.12.81 --port 8230 --database tpch10 --workload check_memory_usage --thread_count 3 --pd 10.2.12.81:6730
func (m *MemoryTracker) run() {
	if *flagReloadData {
		m.cleanup()
		m.prepare()
	}
	m.checkPrepare()
	for i := 0; i < m.threadCount; i++ {
		go m.selectData()
	}
	for {
		time.Sleep(1 * time.Second)
	}
}
