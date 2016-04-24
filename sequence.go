package zdb

import (
    "sync"
)

type sequence struct {
    sync.Mutex
    id int
    spares []int
    spareNdx int
}

func (seq *sequence) getRowID() int {
    seq.Lock()
    defer seq.Unlock()
    
    if seq.spareNdx > 0 {
        seq.spareNdx--
        return seq.spares[seq.spareNdx]
    }
    seq.id++
    return seq.id
}

func (seq *sequence) releaseRowID(ndx int) {
    seq.Lock()
    defer seq.Unlock()
    
    seq.spareNdx++
    if seq.spareNdx > len(seq.spares) {
        seq.spares = append(seq.spares, ndx)
    } else {
        seq.spares[seq.spareNdx] = ndx
    }
}