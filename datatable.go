package zdb

import (
    "sync"
    "github.com/ocdogan/rbt"
)

type DataTable struct {
    sync.Mutex
    name string
    rowSeq *sequence
    rows map[int]*dataRow
    columnCount int
    columns []*dataColumn
    columnsLayout []*dataLayout
    columnsByName map[string]*dataColumn
} 

func NewDataTable(name string) *DataTable {
    return &DataTable{
        name: name,
        rowSeq: &sequence{},
        rows: make(map[int]*dataRow),
        columnsByName: make(map[string]*dataColumn),
    }
}

func (table *DataTable) Name() string {
    return table.name
}

func (table *DataTable) ColumnCount() int {
    return table.columnCount
}

func (table *DataTable) AddColumn(name string, valueType ValueType) error {
    if name == "" {
        return ArgumentNilError("name")
    }
    table.Lock()
    defer table.Unlock()
    
    if _, ok := table.columnsByName[name]; ok {
        return ErrColumnAlreadyExists
    }
    
    col := &dataColumn{
        name: name,
        table: table,
        valueType: valueType,
        index: len(table.columns),
        btree: rbt.NewRbTree(),
        nilRows: make(map[int]*dataRow),
    }

    table.columnsByName[name] = col
    table.columns = append(table.columns, col)
    
    layout := &dataLayout{
        size: getValueSize(valueType),
    }
    if table.columnsLayout != nil {
        prev := table.columnsLayout[table.columnCount-1]
        layout.offset = prev.offset + prev.size
    } 
    
    table.columnsLayout = append(table.columnsLayout, layout)
    table.columnCount++
    
    return nil
}

func (table *DataTable) GetRowByColumn(rowid int, columnIndex int) (bool, interface{}) {
    if rowid < 0 || columnIndex < 0 || table.rows == nil ||
        columnIndex >= table.columnCount {
        return false, nil
    }
    
    if column := table.columns[columnIndex]; column != nil {
        if row, ok := table.rows[rowid]; ok {
            valueIndex := row.data[columnIndex]
            if valueIndex > -1 {
                result, ok := column.get(valueIndex)
                return ok, result
            }
        }
    }
    return false, nil
}

func (table *DataTable) GetRow(rowid int) (bool, []interface{}) {
    if rowid < 0 || table.rows == nil {
        return false, nil
    }
    
    if row, ok := table.rows[rowid]; ok {
        result := make([]interface{}, table.columnCount)
        for i := 0; i < table.columnCount; i++ {
            valueIndex := row.data[i]
            if valueIndex > -1 {
                value, ok := table.columns[i].get(valueIndex)
                if ok {
                    result[i] = value
                }
            }
        }
        return true, result
    }
    return false, nil
}

func (table *DataTable) InsertRow(values ...interface{}) int {
    if table.columnCount == 0 {
        return -1
    }
    
    row := &dataRow{
        id: table.rowSeq.getRowID(),
        data: make([]int, table.columnCount),
    }
    table.rows[row.id] = row
    
    if len(values) == 0 {
        for i := 0; i < table.columnCount; i++ {
            row.data[i] = -1
        }
    } else {
        for index, value := range values {
            if index == table.columnCount {
                break
            }
            
            if column := table.columns[index]; column != nil {
                row.data[index] = column.set(row, value)
            }
        }
    }
    return row.id
}

func (table *DataTable) UpdateRow(rowid int, values ...interface{}) {
    if rowid < 0 || table.rows == nil {
        return
    }

    if row, ok := table.rows[rowid]; ok {
        if len(values) == 0 {
            for i := 0; i < table.columnCount; i++ {
                row.data[i] = table.columns[i].update(row, nil)
            }
        } else {
            for index, value := range values {
                if index == table.columnCount {
                    break
                }
                
                if column := table.columns[index]; column != nil {
                    row.data[index] = column.set(row, value)
                }
            }
        }
    }
}

func (table *DataTable) DeleteRow(rowid int) {
    if rowid < 0 || table.rows == nil {
        return
    }
    
    if row, ok := table.rows[rowid]; ok {
        for _, column := range table.columns {
            column.remove(row)
        }
    }
}
