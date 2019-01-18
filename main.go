package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
	"github.com/siddontang/go-mysql/replication"
)

type config struct {
	host     string
	port     int
	user     string
	password string
	files    string
	database string
	tables   []string
	events   map[replication.EventType]bool
}

var conf config

func initConfig() {
	flag.StringVar(&conf.host, "host", "127.0.0.1", "the host address of mysql")
	flag.IntVar(&conf.port, "port", 3306, "the port of mysql")

	flag.StringVar(&conf.user, "user", "root", "user name of mysql database")
	flag.StringVar(&conf.password, "password", "", "password of mysql database")
	flag.StringVar(&conf.files, "files", "", "binlog files")
	flag.StringVar(&conf.database, "db", "", "only binlog for this schema will be read")
	tables := flag.String("tables", "customer,customer_event", "only binlog for these tables (comma separated) will be read")
	events := flag.String("events", "all", "comma separated event types: insert, update and delete")

	flag.Parse()

	// process flags
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

	db, err := sql.Open("mysql", dataSource)
	checkErr(err, "mysql database connection open error")

	schema, err := NewSchemaFromDB(db, conf.database, conf.tables)
	checkErr(err, "mysql database schema fetch error")

	parser := replication.NewBinlogParser()
	eventParser := NewEventParser(schema, &conf)

	if len(conf.files) > 0 {

		err = filepath.Walk(conf.files, func(path string, info os.FileInfo, err error) error {
			stat, err := os.Stat(conf.files)
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
		// read url from stdin

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			downloadLink := scanner.Text()
			log.Info().Msgf("processing binlog: %s", downloadLink)
			resp, err := http.Get(downloadLink)
			if err != nil {
				checkErr(err, "failed to download binlog")
			}

			b := make([]byte, 4)
			_, err = resp.Body.Read(b)
			checkErr(err, "failed to read from stdin")

			err = parser.ParseReader(resp.Body, eventParser.OnEvent)
			checkErr(err, "faild passing the binlog")
		}
	}

}
