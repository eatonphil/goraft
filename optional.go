package goraft

import (
	"bytes"
	"encoding/gob"
)

type Optional[T any] struct {
	set   bool
	value T
}

func (o *Optional[T]) Reset() {
	o.set = false
}

func (o *Optional[T]) Value() T {
	return o.value
}

func (o *Optional[T]) Put(v T) {
	o.set = true
	o.value = v
}

func (o *Optional[T]) Set() bool {
	return o.set
}

type OptionalGob[T any] struct {
	Set   bool
	Value T
}

func (o Optional[T]) GobEncode() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(OptionalGob[T]{
		Set:   o.set,
		Value: o.value,
	})
	return buf.Bytes(), err
}

func (o Optional[T]) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(nil)
	dec := gob.NewDecoder(buf)
	var og OptionalGob[T]
	err := dec.Decode(&og)
	o.set = og.Set
	o.value = og.Value
	return err
}
