package zdb

import (
    "time"
    "github.com/ocdogan/rbt"
)

type Date struct {
    sec int64
    nsec int
}

const (
	secondsPerMinute = 60
	secondsPerHour   = 60 * secondsPerMinute
	secondsPerDay    = 24 * secondsPerHour
    unixToInternal int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
)

// ComparedTo compares the given RbKey with its self
func (dkey *Date) ComparedTo(key rbt.RbKey) rbt.KeyComparison {
    dkey2 := key.(*Date)
    diff1 := int64(dkey.sec - dkey2.sec)
    switch {
    case diff1 > defaultInt64:
        return rbt.KeyIsGreater
    case diff1 < defaultInt64:
        return rbt.KeyIsLess
    default:
        diff2 := int(dkey.nsec - dkey2.nsec)
        switch {
        case diff2 > 0:
            return rbt.KeyIsGreater
        case diff2 < 0:
            return rbt.KeyIsLess
        default:
            return rbt.KeysAreEqual
        }
    }
}

func ToDate(date *time.Time) *Date {
    return &Date{
        sec: date.Unix() + unixToInternal,
        nsec: date.Nanosecond(),
    }
}

func (date *Date) ToTime() time.Time {
    return time.Unix(date.sec - unixToInternal, int64(date.nsec))
}

func (date *Date) Clone() Date {
    return Date{
        sec: date.sec,
        nsec: date.nsec,
    }
}