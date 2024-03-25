package main

import (
	"database/sql"
	"fmt"
	"time"
)

type Database struct {
	host     string
	port     int
	user     string
	password string
	database string
}

func newDatabase() *Database {
	return &Database{
		host:     *flagHost,
		port:     *flagPort,
		user:     *flagUser,
		password: *flagPassword,
		database: *flagDatabase,
	}
}

func (d *Database) openDB() *sql.DB {
	start := time.Now()
	connCmd := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", d.user, d.password, d.host, d.port, d.database)
	db, err := sql.Open("mysql", connCmd)
	if err != nil {
		fmt.Printf("Connect database (%s) fail: %s\n", connCmd, err)
		panic(err)
	}
	fmt.Printf("Connect database succ: %s => %d ms\n", connCmd, time.Since(start).Milliseconds())
	return db
}

func (d *Database) newConnection() *Connection {
	c := Connection{
		Database: *d,
	}
	c.db = d.openDB()
	return &c
}

type Connection struct {
	Database
	db *sql.DB
}

func (c *Connection) exec(sql string) {
	_, err := c.db.Exec(sql)
	if err != nil {
		panic(fmt.Sprintf("%s => %v", sql, err))
	}
}

func (c *Connection) query(sql string) *sql.Rows {
	rows, err := c.db.Query(sql)
	if err != nil {
		panic(err)
	}
	return rows
}

func (c *Connection) begin() {
	c.exec("begin")
}

func (c *Connection) commit() {
	c.exec("commit")
}

func (c *Connection) setReadEngines(engines string) {
	sql := fmt.Sprintf("set tidb_isolation_read_engines='%s'", engines)
	c.exec(sql)
}

func (c *Connection) getCurrentTSO() (tso uint64) {
	rows := c.query("select @@tidb_current_ts")
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tso)
	}
	return
}

func (c *Connection) getTableAvailable(table string) (available int) {
	sql := fmt.Sprintf("select AVAILABLE from information_schema.tiflash_replica where TABLE_NAME='%s' and TABLE_SCHEMA='%s'", table, c.database)
	rows := c.query(sql)
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&available)
	}
	return
}

func (c *Connection) getDatabaseAvailable() (available int) {
	sql := fmt.Sprintf("select AVAILABLE from information_schema.tiflash_replica where TABLE_SCHEMA='%s'", c.database)
	rows := c.query(sql)
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&available)
		if available == 0 {
			return
		}
	}
	return
}

func (c *Connection) waitTableAvailable(table string) {
	for c.getTableAvailable(table) != 1 {
		fmt.Printf("%s is not available\n", table)
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("%s is available\n", table)
}

func (c *Connection) waitDatabaseAvailable() {
	for c.getDatabaseAvailable() != 1 {
		fmt.Printf("%s is not available\n", c.database)
		time.Sleep(1 * time.Second)
	}
	fmt.Printf("%s is available\n", c.database)
}

func (c *Connection) setTableTiFlashReplicaAndWaitAvailable(table string, count int) {
	sql := fmt.Sprintf("alter table %s set tiflash replica %d", table, count)
	c.exec(sql)
	c.waitTableAvailable(table)
}

func (c *Connection) setDatabaseTiFlashReplicaAndWaitAvailable(database string, count int) {
	sql := fmt.Sprintf("alter database %s set tiflash replica %d", database, count)
	c.exec(sql)
	c.waitDatabaseAvailable()
}

func (c *Connection) dropTable(table string) {
	sql := fmt.Sprintf("drop table `%s`", table)
	c.exec(sql)
}

func (c *Connection) selectCount(table string, engines string) (count uint64) {
	if len(engines) > 0 {
		c.setReadEngines(engines)
	}
	rows := c.query(fmt.Sprintf("select count(*) from `%s`", table))
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&count)
	}
	return
}

func (c * Connection) setRegionSize(regionSize string) {
	if len(regionSize) > 0 {
		c.exec(fmt.Sprintf("set config tikv `coprocessor.region-split-size`='%s'", regionSize))
		c.exec(fmt.Sprintf("set config tiflash `raftstore-proxy.coprocessor.region-split-size`='%s'", regionSize))
		rows := c.query("show config where name like '%region-split-size%';")
		for rows.Next() {
			var t, i, n, v string
			rows.Scan(&t, &i, &n, &v)
			if v != regionSize {
				panic(v)
			}
		}
	}
}
