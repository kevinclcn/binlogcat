package main

import (
	"github.com/siddontang/go-mysql/replication"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kevinclcn/binlogcat/src"
	"fmt"
	"flag"
	"os"
)

var (
	configFile = flag.String("c", "./config.yml", "the config file path, default is ./config.yml")
	host = flag.String("h", "", "the host address of mysql")
	port = flag.Int("p", 0, "the port of mysql")
	user = flag.String("u", "", "user name of mysql database")
	password = flag.String("P", "", "password of mysql database")
)

func main() {

	flag.Parse()

	if _, err := os.Stat(*configFile); err != nil {
		panic(err)
	}

	config, err := binlogcat.LoadFromFile(*configFile)
	if err != nil {
		panic(err)
	}


	myhost := *host
	if myhost == "" {
		myhost = config.Host
	}

	myport := uint16(*port)
	if myport == 0 {
		myport = config.Port
	}

	myuser := *user
	if myuser == "" {
		myuser = config.User
	}

	mypassword := *password
	if mypassword == "" {
		mypassword = config.Password
	}

	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/", myuser, mypassword, myhost, myport)

	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		panic(err)
	}

	schema, err := binlogcat.NewSchemaFromDB(db, config.SchemaName, config.ScanTables)
	if err != nil {
		panic(err)
	}

	myparser := binlogcat.NewParser(schema)

	parser := replication.NewBinlogParser()

	if len(config.Binlog) > 0 {
		parser.ParseFile(config.Binlog, 0, myparser.OnEvent)
	} else {

		b := make([]byte, 4)
		if _, err := os.Stdin.Read(b); err != nil {
			panic(err)
		}

		parser.ParseReader(os.Stdin, myparser.OnEvent)
	}

}
