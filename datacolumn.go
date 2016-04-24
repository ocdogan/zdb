package zdb

import (
    "github.com/ocdogan/rbt"
)

type dataColumn struct {
    valueList
    index int
    name string
    valueCount int
    valueType ValueType
    table *DataTable
    btree *rbt.RbTree
    nilRows map[int]*dataRow
}

func (column *dataColumn) get(valueIndex int) (interface{}, bool) {
    if valueIndex > -1 && valueIndex < column.valueCount {
        switch column.valueType {
        case ValString:
            return *column.strings[valueIndex], true
        case ValInt:
            return column.integers[valueIndex], true
        case ValLong:
            return column.longs[valueIndex], true
        case ValFloat:
            return column.floats[valueIndex], true
        case ValDate:
            return column.dates[valueIndex].Clone(), true
        }
    } 
    return nil, false
}

func (column *dataColumn) set(row *dataRow, value interface{}) int {
    convertedVal, ok := toValueType(value, column.valueType)
    if !ok || convertedVal == nil {
        column.nilRows[row.id] = row
        return nilValueInt
    }
    
    key, ok := toValueKey(convertedVal, column.valueType)
    if !ok || key == nil || key == nilKey {
        column.nilRows[row.id] = row
        return nilValueInt
    }

    if irows, ok := column.btree.Get(key); ok {
        node := irows.(*dataNode)
        node.add(row)
        return node.valueIndex
    }
    
    result := nilValueInt
    switch column.valueType {
    case ValString:
        data := convertedVal.(string)
        column.strings = append(column.strings, &data)
        result = len(column.strings)-1
    case ValInt:
        column.integers = append(column.integers, convertedVal.(int))
        result = len(column.integers)-1
    case ValLong:
        column.longs = append(column.longs, convertedVal.(int64))
        result = len(column.longs)-1
    case ValFloat:
        column.floats = append(column.floats, convertedVal.(float64))
        result = len(column.floats)-1
    case ValDate:
        data := convertedVal.(Date)
        column.dates = append(column.dates, &data)
        result = len(column.dates)-1
    }
    column.valueCount++
    
    node := &dataNode{
        valueIndex: result,
    }
    node.add(row)
    column.btree.Insert(key, node)
    
    return result
}

func (column *dataColumn) remove(row *dataRow) {
    valueIndex := row.data[column.index]
    if valueIndex < 0 {
        delete(column.nilRows, row.id)
        return
    }

    var ivalue interface{}
    
    switch column.valueType {
    case ValString:
        ivalue = *column.strings[valueIndex]
    case ValInt:
        ivalue = column.integers[valueIndex]
    case ValLong:
        ivalue = column.longs[valueIndex]
    case ValFloat:
        ivalue = column.floats[valueIndex]
    case ValDate:
        ivalue = *column.dates[valueIndex]
    }
    
    key, _ := toValueKey(ivalue, column.valueType)
    if irows, ok := column.btree.Get(key); ok {
        irows.(*dataNode).remove(row)
    }
}

func (column *dataColumn) update(row *dataRow, value interface{}) int {
    column.remove(row)
    return column.set(row, value)
}
