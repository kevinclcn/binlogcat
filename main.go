package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"github.com/siddontang/go-mysql/replication"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	host      string
	port      int
	user      string
	password  string
	binlogdir string
	database  string
	tables    []string
	events    map[replication.EventType]bool
}

var conf Config

func initConfig() {
	flag.StringVar(&conf.host, "host", "127.0.0.1", "the host address of mysql")
	flag.IntVar(&conf.port, "port", 3306, "the port of mysql")

	flag.StringVar(&conf.user, "user", "root", "user name of mysql database")
	flag.StringVar(&conf.password, "password", "", "password of mysql database")
	flag.StringVar(&conf.binlogdir, "binlogdir", "", "binlog files")
	flag.StringVar(&conf.database, "schema", "", "only binlog for this schema will be read")
	tables := flag.String("tables", "customer,customer_event", "only binlog for these tables (comma separated) will be read")
	events := flag.String("events", "all", "comma separated event types: insert, update and delete")

	conf.tables = strings.Split(*tables, ",")
	if *events == "all" {
		*events = "insert,update,delete"
	}
	conf.events = make(map[replication.EventType]bool)
	for _, e := range strings.Split(*events, ",") {

		if e == "insert" {
			conf.events[replication.WRITE_ROWS_EVENTv1] = true
			conf.events[replication.WRITE_ROWS_EVENTv2] = true
		} else if e == "update" {
			conf.events[replication.UPDATE_ROWS_EVENTv1] = true
			conf.events[replication.UPDATE_ROWS_EVENTv2] = true
		} else if e == "delete" {
			conf.events[replication.DELETE_ROWS_EVENTv1] = true
			conf.events[replication.DELETE_ROWS_EVENTv2] = true
		}
	}

	flag.Parse()
}

func checkErr(err error, msg string) {
	if err != nil {
		log.Error().Msgf(msg+": %v", err)
		os.Exit(1)
	}

}

func main() {
	initConfig()

	dataSource := fmt.Sprintf("%s:%s@tcp(%s:%d)/", conf.user, conf.password, conf.host, conf.port)
	log.Info().Msg(dataSource)

	db, err := sql.Open("mysql", dataSource)
	checkErr(err, "mysql database connection open error")

	schema, err := NewSchemaFromDB(db, conf.database, conf.tables)
	checkErr(err, "mysql database schema fetch error")

	parser := replication.NewBinlogParser()
	eventParser := NewEventParser(schema, &conf)

	if len(conf.binlogdir) > 0 {

		err = filepath.Walk(conf.binlogdir, func(path string, info os.FileInfo, err error) error {
			stat, err := os.Stat(conf.binlogdir)
			if err != nil {
				return err
			}

			if stat.Mode().IsRegular() {
				log.Info().Msgf("walking %s\n", path)
				err = parser.ParseFile(path, 0, eventParser.OnEvent)
			}

			return err
		})

		checkErr(err, "failed to pass binlog")
		log.Info().Msg("finished binlog parsing")
	} else {
		b := make([]byte, 4)
		_, err := os.Stdin.Read(b)
		checkErr(err, "failed to read from stdin")

		err = parser.ParseReader(os.Stdin, eventParser.OnEvent)
		checkErr(err, "faild passing the binlog")
	}

}
