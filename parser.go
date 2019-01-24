package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"

	"github.com/siddontang/go-mysql/replication"
)

type Parser struct {
	conf   *config
	Schema *Schema
}

func NewEventParser(schema *Schema, conf *config) *Parser {
	parser := Parser{conf, schema}

	return &parser
}

type RowData struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     string                 `json:"type"`
	Ts       uint32                 `json:"ts"`
	ServerId uint32                 `json:"server_id"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old,omitempty"`
}

func convertEventDataToRowData(eventData []interface{}, rowData map[string]interface{}, columnsDef map[int]*Column) {
	for i, d := range eventData {

		if d == nil {
			continue
		}

		if columnsDef[i] == nil {
			break
		}

		columnName := columnsDef[i].ColumnName
		rowData[columnName] = d
	}
}

func (p *Parser) OnEvent(event *replication.BinlogEvent) error {

	if _, ok := p.conf.events[event.Header.EventType]; !ok {
		return nil
	}

	re, ok := event.Event.(*replication.RowsEvent)
	if !ok {
		return nil
	}

	schema := string(re.Table.Schema)
	if strings.Compare(p.Schema.Name, schema) != 0 {
		return nil
	}

	table := string(re.Table.Table)
	tableDef, ok := p.Schema.Tables[table]
	if !ok {
		log.Info().Msgf("skip table %s", table)
		return nil
	}

	rowData := RowData{Database: schema, Table: table, Ts: event.Header.Timestamp, ServerId: event.Header.ServerID}

	rowData.Data = make(map[string]interface{})

	columns := tableDef.Columns
	data := re.Rows[0]

	switch event.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		rowData.Type = "insert"
		convertEventDataToRowData(data, rowData.Data, columns)

	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		rowData.Type = "delete"
		convertEventDataToRowData(data, rowData.Data, columns)

	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		rowData.Type = "update"
		rowData.Old = make(map[string]interface{})
		convertEventDataToRowData(data, rowData.Old, columns)

		data := re.Rows[1]
		convertEventDataToRowData(data, rowData.Data, columns)

	default:
		return nil
	}

	res, err := json.Marshal(rowData)
	if err != nil {
		return err
	}

	if conf.producer != nil {

		topic := strings.Replace(conf.kafkaTopic, "%{database}", rowData.Database, 1)
		topic = strings.Replace(topic, "%{table}", rowData.Table, 1)

		key := rowData.Data[conf.partitionColumn]
		if key == nil {
			key = rowData.Data["id"]
		}

		conf.producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("%s", key)),
			Value: sarama.StringEncoder(string(res)),
		}

	} else {
		os.Stdout.Write(res)
		os.Stdout.Write([]byte{'\n'})
	}

	return nil
}
