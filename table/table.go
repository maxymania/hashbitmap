/*
   Copyright 2018 Simon Schmidt

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package table

import "encoding/binary"
import "github.com/vmihailenco/msgpack"
import sstable "github.com/syndtr/goleveldb/leveldb/table"
import ldbstor "github.com/syndtr/goleveldb/leveldb/storage"
import "io"
import "hash"
import "hash/fnv"
import "github.com/RoaringBitmap/roaring"
import "github.com/spf13/cast"
import "github.com/maxymania/hashbitmap/multistream"
import "fmt"
import "bufio"
import "github.com/lytics/confl"
import "sync"
import "io/ioutil"

type meta struct{
	Columns int
}

type indexColumn struct{
	sets [256]*roaring.Bitmap
}
func newIndex() *indexColumn {
	ic := new(indexColumn)
	for i := range ic.sets { ic.sets[i] = roaring.New() }
	return ic
}
func (ic *indexColumn) add(buf []byte,i uint32) {
	for _,b := range buf {
		ic.sets[b].Add(i)
	}
}

type Writer struct{
	i uint32
	wr *sstable.Writer
	buf [16]byte
	hsh hash.Hash
	indeces []*indexColumn
	idx io.Writer
}
func NewWriter(idx,tab io.Writer,cols int) *Writer {
	w := &Writer{
		wr:      sstable.NewWriter(tab,nil),
		hsh:     fnv.New128a(),
		indeces: make([]*indexColumn,cols),
		idx:     idx,
	}
	for i := range w.indeces { w.indeces[i] = newIndex() }
	return w
}

//func (w *Writer)
func (w *Writer) WriteRecord(record []interface{}) error {
	i := w.i
	key := w.buf[:4]
	binary.BigEndian.PutUint32(key,i)
	val,err := msgpack.Marshal(record)
	if err!=nil { return err }
	err = w.wr.Append(key,val)
	if err!=nil { return err }
	
	for j,v := range record {
		if len(w.indeces) <= j { break }
		w.hsh.Reset()
		fmt.Fprint(w.hsh,cast.ToString(v))
		sum := w.hsh.Sum(w.buf[:0])
		w.indeces[j].add(sum,i)
	}
	
	w.i++
	return nil
}
func (w *Writer) Close() error {
	err := w.wr.Close()
	if err!=nil { return err }
	nw := multistream.NewWriter(w.idx)
	bw := bufio.NewWriterSize(nw,1<<25)
	
	data,_ := confl.Marshal(meta{Columns:len(w.indeces)})
	_,err = nw.Write(data)
	if err!=nil { return err }
	nw.Next()
	for _,x := range w.indeces {
		for _,y := range x.sets {
			y.RunOptimize()
			_,err = y.WriteTo(bw)
			if err!=nil { return err }
			err = bw.Flush()
			if err!=nil { return err }
			nw.Next()
		}
	}
	return nil
}

type hasher struct{
	hsh hash.Hash
	buf [16]byte
}
func mkhasher() interface{} { return &hasher{hsh:fnv.New128a()} }
var hashers = sync.Pool{New:mkhasher}
func (h *hasher) hash(v interface{}) []byte {
	h.hsh.Reset()
	fmt.Fprint(h.hsh,cast.ToString(v))
	return h.hsh.Sum(h.buf[:0])
}
func (h *hasher) release() { hashers.Put(h) }

type Reader struct{
	indeces  []*indexColumn
	rd       *sstable.Reader
}
func NewReader(idx io.Reader,tab io.ReaderAt,tablen int64) (r *Reader,e error) {
	r = new(Reader)
	r.rd,e = sstable.NewReader(tab,tablen,ldbstor.FileDesc{},nil,nil,nil)
	if e!=nil { return }
	nr := multistream.NewReader(idx)
	e = nr.Next()
	if e!=nil { return }
	m := new(meta)
	dec,err := ioutil.ReadAll(nr)
	if err!=nil { e=err; return }
	e = confl.Unmarshal(dec,m)
	if e!=nil { return }
	r.indeces = make([]*indexColumn,m.Columns)
	for i := range r.indeces { r.indeces[i] = newIndex() }
	for _,index := range r.indeces {
		for _,y := range index.sets {
			e = nr.Next()
			if e!=nil { return }
			_,e = y.ReadFrom(nr)
			if e!=nil { return }
		}
	}
	return
}

var EmptyBitMap = roaring.New()

/*
Performs a lookup and returns the resulting bitmap.
The bitmap is potentially shared and must not be modified.
*/
func (r *Reader) Lookup(parallelism,column int,v interface{}) *roaring.Bitmap {
	if len(r.indeces) <= column { return EmptyBitMap }
	var used [256]bool
	var arr [16]*roaring.Bitmap
	var n int
	h := hashers.Get().(*hasher)
	defer h.release()
	
	for _,b := range h.hash(v) {
		if used[b] { continue }
		arr[n] = r.indeces[column].sets[b]
		used[b] = true
		n++
	}
	switch n {
	case 0: return EmptyBitMap
	case 1: return arr[0]
	}
	if parallelism==0 {
		return roaring.FastAnd(arr[:n]...)
	}
	return roaring.ParAnd(parallelism,arr[:n]...)
}
type Iterator struct{
	rea *Reader
	ii  roaring.IntIterable
	buf [4]byte
}
func (r *Reader) ReadRecords(rb *roaring.Bitmap) *Iterator {
	return &Iterator{rea:r,ii:rb.Iterator()}
}

func (i *Iterator) HasNext() bool{
	return i.ii.HasNext()
}
func (i *Iterator) Next() (r []interface{},e error) {
	j := i.ii.Next()
	binary.BigEndian.PutUint32(i.buf[:],j)
	raw,err := i.rea.rd.Get(i.buf[:],nil)
	if err!=nil { e = err; return }
	e = msgpack.Unmarshal(raw,&r)
	return
}

