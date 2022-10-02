package main

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
