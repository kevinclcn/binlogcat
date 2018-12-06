package binlogcat

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/siddontang/go-mysql/replication"
)

type Parser struct {
	Schema     *Schema
	TableIdMap map[uint64]string
}

func NewParser(schema *Schema) *Parser {
	parser := Parser{schema, make(map[uint64]string)}

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
		return nil
	}

	rowData := RowData{Database: schema, Table: table, Ts: event.Header.Timestamp, ServerId: event.Header.ServerID}

	rowData.Data = make(map[string]interface{})

	columns := tableDef.Columns
	data := re.Rows[0]

	convertEventDataToRowData(data, rowData.Data, columns)

	switch event.Header.EventType {
	//case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
	//	rowData.Type = "insert"
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		rowData.Type = "delete"
	//case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
	//	rowData.Type = "update"
	//	rowData.Old = make(map[string]interface{})
	//
	//	data := re.Rows[1]
	//	convertEventDataToRowData(data, rowData.Old, columns)
	default:
		return nil
	}

	res, err := json.Marshal(rowData)
	if err != nil {
		return err
	}

	fmt.Println(string(res))

	fileName := fmt.Sprintf("./%s.txt", rowData.Table)
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f.Write(res)
	f.Write([]byte{'\n'})

	defer f.Close()

	return nil
}
