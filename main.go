package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/kevinclcn/binlogcat/src"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"path/filepath"
)

var (
	configFile = flag.String("c", "./config.yml", "the config file path, default is ./config.yml")
	host       = flag.String("h", "", "the host address of mysql")
	port       = flag.Int("p", 0, "the port of mysql")
	user       = flag.String("u", "", "user name of mysql database")
	password   = flag.String("P", "", "password of mysql database")
)

func main() {

	flag.Parse()

	config, err := binlogcat.LoadFromFile(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file %s read error: %v\n", *configFile, err)
		os.Exit(1)
	}

	myhost := *host
	if myhost == "" {
		myhost = config.Host
	}

	if len(myhost) == 0 {
		myhost = "127.0.0.1"
	}

	myport := uint16(*port)
	if myport == 0 {
		myport = config.Port
	}
	if myport == 0 {
		myport = 3306
	}

	myuser := *user
	if myuser == "" {
		myuser = config.User
	}

	if len(myuser) == 0 {
		fmt.Fprintf(os.Stderr, "mysql user should not be empty.\n")
		os.Exit(1)
	}

	mypassword := *password
	if mypassword == "" {
		mypassword = config.Password
	}

	if len(mypassword) == 0 {
		fmt.Fprintf(os.Stderr, "mysql password should not be empty.\n")
		os.Exit(1)
	}

	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/", myuser, mypassword, myhost, myport)

	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mysql database connection open error: %v\n", err)
		os.Exit(1)
	}

	schema, err := binlogcat.NewSchemaFromDB(db, config.SchemaName, config.ScanTables)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mysql database schema fetch error: %v\n", err)
		os.Exit(1)
	}

	myparser := binlogcat.NewParser(schema)

	parser := replication.NewBinlogParser()

	if len(config.Binlog) > 0 {

		stat, err := os.Stat(config.Binlog)
		if err != nil {
			fmt.Fprintf(os.Stderr, "no file exists: %v\n", err)
			os.Exit(1)
		}

		if stat.Mode().IsRegular() {
			parser.ParseFile(config.Binlog, 0, myparser.OnEvent)
		} else {
			filepath.Walk(config.Binlog, func(path string, info os.FileInfo, err error) error {
				return parser.ParseFile(path, 0, myparser.OnEvent)
			})
		}
	} else {
		b := make([]byte, 4)
		if _, err := os.Stdin.Read(b); err != nil {
			fmt.Fprintf(os.Stderr, "failed to read from stdin: %v\n", err)
			os.Exit(1)
		}

		parser.ParseReader(os.Stdin, myparser.OnEvent)
	}

}
