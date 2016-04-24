package zdb

import (
    "encoding/json"
    "strconv"
    "time"
)

const (
    emptyString = ""
    nilValueInt = -1
    defaultInt = int(0)
    defaultInt64 = int64(0)
    defaultFloat64 = float64(0)
)


func toString(value interface{}) (interface{}, bool) {
    switch value.(type) {
    case nil:
        return "", true
    case string:
        return value, true
    case int:
        return strconv.Itoa(value.(int)), true
    case int32:
        return strconv.Itoa(int(value.(int32))), true
    case int64:
        return strconv.FormatInt(value.(int64), 10), true
    case int16:
        return strconv.Itoa(int(value.(int16))), true
    case int8:
        return strconv.Itoa(int(value.(int8))), true
    case uint:
        return strconv.FormatUint(uint64(value.(uint)), 10), true
    case uint32:
        return strconv.FormatUint(uint64(value.(uint32)), 10), true
    case uint64:
        return strconv.FormatUint(value.(uint64), 10), true
    case uint16:
        return strconv.FormatUint(uint64(value.(uint16)), 10), true
    case uint8:
        return strconv.FormatUint(uint64(value.(uint8)), 10), true
    case float64:
        return strconv.FormatFloat(value.(float64), 'E', -1, 64), true
    case float32:
        return strconv.FormatFloat(float64(value.(float32)), 'E', -1, 64), true
    case bool:
        if value.(bool) {
            return "true", true
        }
        return "false", true
    default:
        mdata, err := json.Marshal(value)
        if err != nil {
            return "", false
        }
        return string(mdata), true
    }
}

func toInt(value interface{}) (interface{}, bool) {
    switch value.(type) {
    case nil:
        return nil, true
    case int:
        return value.(int), true
    case int32:
        return int(value.(int32)), true
    case int64:
        valTmp := value.(int64)
        val := int(valTmp)            
        if valTmp == int64(val) {
            return val, true
        }
        return defaultInt, false
    case int16:
        return int(value.(int16)), true
    case int8:
        return int(value.(int8)), true
    case uint:
        valTmp := value.(uint)
        val := int(valTmp)            
        if valTmp == uint(val) {
            return val, true
        }
        return defaultInt, false
    case uint32:
        valTmp := value.(uint32)
        val := int(valTmp)            
        if valTmp == uint32(val) {
            return val, true
        }
        return defaultInt, false
    case uint64:
        valTmp := value.(uint64)
        val := int(valTmp)            
        if valTmp == uint64(val) {
            return val, true
        }
        return defaultInt, false
    case uint16:
        valTmp := value.(uint16)
        val := int(valTmp)            
        if valTmp == uint16(val) {
            return val, true
        }
        return defaultInt, false
    case uint8:
        valTmp := value.(uint8)
        val := int(valTmp)            
        if valTmp == uint8(val) {
            return val, true
        }
        return defaultInt, false
    default:
        return defaultInt, false
    }
}

func toLong(value interface{}) (interface{}, bool) {
    switch value.(type) {
    case nil:
        return nil, true
    case int64:
        return value.(int64), true
    case int:
        return int64(value.(int)), true
    case int32:
        return int64(value.(int32)), true
    case int16:
        return int64(value.(int16)), true
    case int8:
        return int64(value.(int8)), true
    case uint:
        return int64(value.(uint)), true
    case uint32:
        return int64(value.(uint32)), true
    case uint64:
        valTmp := value.(uint64)
        val := int64(valTmp)            
        if valTmp == uint64(val) {
            return val, true
        }
        return defaultInt64, false
    case uint16:
        return int64(value.(uint16)), true
    case uint8:
        return int64(value.(uint8)), true
    default:  
        return defaultInt64, false
    }
}

func toFloat64(value interface{}) (interface{}, bool) {
    switch value.(type) {
    case nil:
        return nil, true
    case float64:
        return value.(float64), true
    case float32:
        return float64(value.(float32)), true
    case int:
        return float64(value.(int)), true
    case int32:
        return float64(value.(int32)), true
    case int64:
        valTmp := value.(int64)
        val := float64(valTmp)            
        if valTmp == int64(val) {
            return val, true
        }
        return defaultFloat64, false
    case int16:
        return float64(value.(int16)), true
    case int8:
        return float64(value.(int8)), true
    case uint:
        return float64(value.(uint)), true
    case uint32:
        return float64(value.(uint32)), true
    case uint64:
        valTmp := value.(uint64)
        val := float64(valTmp)            
        if valTmp == uint64(val) {
            return val, true
        }
        return defaultFloat64, false
    case uint16:
        return float64(value.(uint16)), true
    case uint8:
        return float64(value.(uint8)), true
    default:
        return defaultFloat64, false
    }
}

func toDate(value interface{}) (interface{}, bool) {
    switch value.(type) {
    case nil:
        return nil, true
    case time.Time:
        t := value.(time.Time)
        d := ToDate(&t)
        return *d, true
    case float32:
        return time.Unix(int64(value.(float32)), defaultInt64), true
    case int:
        return time.Unix(int64(value.(int)), defaultInt64), true
    case int32:
        return time.Unix(int64(value.(int32)), defaultInt64), true
    case int64:
        return time.Unix(int64(value.(int64)), defaultInt64), true
    case int16:
        return time.Unix(int64(value.(int16)), defaultInt64), true
    case int8:
        return time.Unix(int64(value.(int8)), defaultInt64), true
    case uint:
        return time.Unix(int64(value.(uint)), defaultInt64), true
    case uint32:
        return time.Unix(int64(value.(uint32)), defaultInt64), true
    case uint64:
        return time.Unix(int64(value.(uint64)), defaultInt64), true
    case uint16:
        return time.Unix(int64(value.(uint16)), defaultInt64), true
    case uint8:
        return time.Unix(int64(value.(uint8)), defaultInt64), true
    case Date:    
        return value.(Date), true
    default:
        return time.Time{}, false
    }
}
