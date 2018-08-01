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

package multistream


import "io"
import xdr "github.com/davecgh/go-xdr/xdr2"

type packet struct{
	Start bool
	Data  []byte
}

type Writer struct{
	start bool
	enc   *xdr.Encoder
}
func NewWriter(w io.Writer) (*Writer) {
	return &Writer{
		start:true,
		enc:xdr.NewEncoder(w),
	}
}

func (w *Writer) Next() {
	w.start = true
}

/*
Writes a chunk to the output stream. This method is going to yield a frame.
Therefore, it is a good idea to use a buffered writer:

	msw := multistream.NewWriter(dest)
	outpt := bufio.NewWriterSize(msw,1<<20) // 1MB buffer or more.
*/
func (w *Writer) Write(p []byte) (n int, err error) {
	n = len(p)
	_,err = w.enc.Encode(packet{w.start,p})
	if err!=nil { n = 0 }
	w.start = false
	return
}

type Reader struct{
	dec *xdr.Decoder
	buf []byte
	pkt packet
	eof bool
	err error
}
func NewReader(r io.Reader) (*Reader) {
	return &Reader{
		dec: xdr.NewDecoder(r),
	}
}
func (r *Reader) Next() error {
	for {
		if r.err!=nil { return r.err }
		if r.eof { return io.EOF }
		if r.pkt.Start {
			r.pkt.Start = false
			return nil
		}
		r.pkt.Data = r.buf
		_,err := r.dec.Decode(&r.pkt)
		if err==io.EOF { r.eof = true } else { r.err = err }
		if cap(r.buf) < cap(r.pkt.Data) {
			r.buf = r.pkt.Data
		}
	}
}
func (r *Reader) Read(p []byte) (n int, err error) {
	for len(p)>0 {
		if r.pkt.Start {
			err = io.EOF
			break
		}
		if len(r.pkt.Data)>0 {
			g := copy(p,r.pkt.Data)
			n += g
			p = p[g:]
			r.pkt.Data = r.pkt.Data[g:]
			continue
		}
		if r.err!=nil {
			err = r.err
			break
		}
		if r.eof {
			err = io.EOF
			break
		}
		r.pkt.Data = r.buf
		_,err := r.dec.Decode(&r.pkt)
		if err==io.EOF { r.eof = true } else { r.err = err }
		if err!=nil { r.pkt.Data = nil }
		if cap(r.buf) < cap(r.pkt.Data) {
			r.buf = r.pkt.Data
		}
	}
	if n>0 && err==io.EOF { err = nil }
	return
}


