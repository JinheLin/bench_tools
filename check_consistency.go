package main

import (
	"fmt"
	"time"
)

type CheckConsistency struct {
	tableName      string
	sqlCreateTable string
	sqlAddTiflash  string
	sqlInsert      string
	sqlSelect      string
	value          string
	threadCount    int
	batchCount     int
	dbInfo         *Database
}

func newCheckConsistency() CheckConsistency {
	return CheckConsistency{
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
		sqlAddTiflash: "alter table other_handle set tiflash replica 2",
		sqlInsert:     "insert into `other_handle` (`b`, `c`, `d`, `e`, `g`, `h`, `i`, `j`) values",
		sqlSelect:     "select count(*) from other_handle",
		value:         fmt.Sprintf("(%d, '%s', '%s', %f, %f, %f, '%s', '%s')", 1, "Hello, World!", "One, Tow, Three, Four...", 3.14, 3.1415926, 3.1415, "2024-01-01", "2024-01-01 00:00:00"),
		threadCount:   *flagThreadCount,
		batchCount:    *flagInsertBatchCount,
		dbInfo:        newDatabase(),
	}
}

func (w *CheckConsistency) insertData() {
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	sql := fmt.Sprintf("%s %s", w.sqlInsert, w.value)
	for i := 0; i < w.batchCount-1; i++ {
		sql += "," + w.value
	}
	for {
		c.exec(sql)
	}
}

func (w *CheckConsistency) selectCount(c *Connection, engines string) (count uint64) {
	c.setReadEngines(engines)
	rows := c.query(w.sqlSelect)
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&count)
	}
	return
}

func (w *CheckConsistency) checkConsistency() {
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	for {
		c.begin()
		tso := c.getCurrentTSO()
		tikvCount := w.selectCount(c, "tikv")
		tiflashCount := w.selectCount(c, "tiflash")
		c.commit()
		fmt.Printf("tso: %d, tikv: %d, tiflash: %d\n", tso, tikvCount, tiflashCount)
		if tikvCount != tiflashCount {
			panic("checkConsistency failed")
		}
		time.Sleep(1 * time.Second)
	}
}

func (w *CheckConsistency) createTable() {
	c := w.dbInfo.newConnection()
	defer c.db.Close()
	c.exec(w.sqlCreateTable)
	c.exec(w.sqlAddTiflash)
	c.waitTiFlashAvailable(w.tableName)
	c.exec("set config tikv `coprocessor.region-split-size`='6MiB'")
	c.exec("set config tiflash `raftstore-proxy.coprocessor.region-split-size`='6MiB'")
	rows := c.query("show config where name like '%region-split-size%';")
	for rows.Next() {
		var t, i, n, v string
		rows.Scan(&t, &i, &n, &v)
		if v != "6MiB" {
			panic(v)
		}
	}
}

func (w *CheckConsistency) insertAndCheck() {
	w.createTable()
	for i := 0; i < w.threadCount; i++ {
		go w.insertData()
	}
	w.checkConsistency()
}
