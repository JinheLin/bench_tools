package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type CheckConsistency003 struct {
	tableName      string
	sqlCreateTable string
	sqlInsert      string
	insertValue    string
	threadCount    int
	batchCount     int
	tableRowsLimit int
	dbInfo         *Database
	stop           int32
	wg             sync.WaitGroup
}

func newCheckConsistency003() CheckConsistency003 {
	return CheckConsistency003{
		tableName: "other_handle",
		sqlCreateTable: "CREATE TABLE IF NOT EXISTS `other_handle` (" +
			"`a` bigint(20) NOT NULL AUTO_INCREMENT," +
			"`b` int(11) DEFAULT NULL," +
			"`c` varchar(100) NOT NULL," +
			"`d` char(100) DEFAULT NULL," +
			"`e` float DEFAULT NULL," +
			"`g` double DEFAULT NULL," +
			"`h` decimal(8,4) DEFAULT NULL," +
			"`i` date DEFAULT NULL," +
			"`j` datetime DEFAULT NULL," +
			"PRIMARY KEY (`a`,`c`) /*T![clustered_index] NONCLUSTERED */)",
		sqlInsert: "insert into `other_handle` (`b`, `c`, `d`, `e`, `g`, `h`, `i`, `j`) values",
		insertValue: fmt.Sprintf("(%d, '%s', '%s', %f, %f, %f, '%s', '%s')",
			1,
			"0123456789./*-+~!@#$%ﬂ&*()__+=-][{}abcdefghijl;dsdhjhvqervpanzvbxmcnpoiqwieutlkahsfhmzncbvdhaqir411d",
			"0123456789./*-+~!@#$%ﬂ&*()__+=-][{}abcdefghijl;dsdhjhvqervpanzvbxmcnpoiqwieutlkahsfhmzncbvdhaqir411d",
			3.14,
			3.1415926,
			3.1415,
			"2024-01-01",
			"2024-01-01 00:00:00"),
		threadCount:    *flagThreadCount,
		batchCount:     *flagInsertBatchCount,
		tableRowsLimit: *flagTableRowsLimit,
		dbInfo:         newDatabase(),
		stop:           0,
	}
}

func (w *CheckConsistency003) insertData() {
	w.wg.Add(1)
	defer w.wg.Done()
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	sql := fmt.Sprintf("%s %s", w.sqlInsert, w.insertValue)
	for i := 0; i < w.batchCount-1; i++ {
		sql += "," + w.insertValue
	}
	for atomic.LoadInt32(&w.stop) == 0 {
		c.exec(sql)
	}
}

func (w *CheckConsistency003) checkConsistency() {
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	for {
		c.begin()
		tso := c.getCurrentTSO()
		t0 := time.Now()
		tikvCount := c.selectCount(w.tableName, "tikv")
		t1 := time.Now()
		tiflashCount := c.selectCount(w.tableName, "tiflash")
		t2 := time.Now()
		c.commit()
		fmt.Printf("tso: %d, tikv: %d(%f seconds), tiflash: %d(%f seconds)\n",
			tso, tikvCount, t1.Sub(t0).Seconds(), tiflashCount, t2.Sub(t1).Seconds())
		if tikvCount != tiflashCount {
			panic("checkConsistency failed")
		}
		if tikvCount >= uint64(w.tableRowsLimit) {
			fmt.Printf("count=%d is greater than limit=%d\n", tikvCount, w.tableRowsLimit)
			atomic.StoreInt32(&w.stop, 1)
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func (w *CheckConsistency003) createTable() {
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	c.exec(w.sqlCreateTable)
	c.setTableTiFlashReplicaAndWaitAvailable(w.tableName, 2)
	c.setRegionSize(*flagRegionSize)
}

func (w *CheckConsistency003) dropTable() {
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	c.dropTable(w.tableName)
}

func (w *CheckConsistency003) insertAndCheck() {
	for {
		atomic.StoreInt32(&w.stop, 0)
		w.createTable()
		for i := 0; i < w.threadCount; i++ {
			go w.insertData()
		}
		w.checkConsistency()
		w.wg.Wait()
		w.dropTable()
	}
}
