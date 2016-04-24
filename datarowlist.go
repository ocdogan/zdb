package zdb

type dataRowListItem struct {
    row *dataRow
    next *dataRowListItem
}

type dataRowList struct {
    count int
    rowMap map[int]*dataRow
    root *dataRowListItem
}

func (list *dataRowList) Count() int {
    c := len(list.rowMap)
    if c == 0 {
        return list.count
    }
    return c
}

func (list *dataRowList) add(row *dataRow) {
    if list.rowMap != nil {
        list.rowMap[row.id] = row
        return
    }
    if list.count == 10 {
        rm := make(map[int]*dataRow, 10)
        list.rowMap = rm        
        for item := list.root; item != nil; {
            rm[item.row.id] = item.row
            item.row = nil
            item, item.next = item.next, nil
        }
        list.root = nil
        list.count = 0
        return
    }
    item := &dataRowListItem{
        row: row,
    }
    item.next = list.root
    list.root = item
    list.count++
}

func (list *dataRowList) remove(row *dataRow) {
    if list.rowMap != nil {
        delete(list.rowMap, row.id)
        return
    }
    var prev *dataRowListItem
    for item := list.root; item != nil; {
        if row.id == item.row.id {
            if prev != nil {
                prev.next = item.next
            } else {
                list.root = item.next
            }
            item.row = nil
            item.next = nil
            list.count--
            break
        }
        prev = item
        item = item.next
    }
}
