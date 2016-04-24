package zdb

import (
    "unsafe"    
    "github.com/ocdogan/rbt"
)

type ValueType byte

const (
    ValString ValueType = iota
    ValInt
    ValLong
    ValFloat
    ValDate
)

var (
    nilKey rbt.RbKey = &rbt.NilKey{}
)

var (
    dot = "."
    sizeOfInt = int(unsafe.Sizeof(1))
    sizeOfDate = int(unsafe.Sizeof(int64(1)))
    sizeOfLong = int(unsafe.Sizeof(int64(1)))
    sizeOfFloat = int(unsafe.Sizeof(float64(1)))
    sizeOfString = int(unsafe.Sizeof(&dot))
)

func getValueSize(valueType ValueType) int {
    switch valueType {
    case ValString:
        return sizeOfString
    case ValInt:
        return sizeOfInt
    case ValLong:
        return sizeOfLong
    case ValFloat:
        return sizeOfFloat
    case ValDate:
        return sizeOfDate
    }
    return 0
}

func toValueType(value interface{}, toType ValueType) (interface{}, bool) {
    switch toType {
    case ValString:
        return toString(value)
    case ValInt:
        return toInt(value)
    case ValLong:
        return toLong(value)
    case ValFloat:
        return toFloat64(value)
    case ValDate:
        return toLong(value)
    }
    return nil, false
}

func toValueKey(value interface{}, toType ValueType) (rbt.RbKey, bool) {
    switch toType {
    case ValString:
        val, ok := toString(value)
        if ok {
            if val == nil {
                return nilKey, true
            }
            key := rbt.StringKey(val.(string))
            return &key, ok
        }
        return nil, false
    case ValInt:
        val, ok := toInt(value)
        if ok {
            if val == nil {
                return nilKey, true
            }
            key := rbt.IntKey(val.(int))
            return &key, true
        }
        return nil, false
    case ValLong:
        val, ok := toLong(value)
        if ok {
            if val == nil {
                return nilKey, true
            }
            key := rbt.Int64Key(val.(int64))
            return &key, true
        }
        return nil, false
    case ValFloat:
        val, ok := toFloat64(value)
        if ok {
            if val == nil {
                return nilKey, true
            }
            key := rbt.Float64Key(val.(float64))
            return &key, true
        }
        return nil, false
    case ValDate:
        val, ok := toLong(value)
        if ok {
            if val == nil {
                return nilKey, true
            }
            return &Date{sec: val.(int64)}, true
        }
        return nil, false
    }    
    return nil, false
}