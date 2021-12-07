// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package ubjson

type options struct {
	tagName string
}

type UnmarshalOption interface {
	unmarshalApply(*decodeState)
}

type MarshalOption interface {
	marshalApply(*encodeState)
}

type Option interface {
	UnmarshalOption
	MarshalOption
}

type tagNameOption string

func (n tagNameOption) unmarshalApply(d *decodeState) {
	d.tagName = string(n)
}

func (n tagNameOption) marshalApply(e *encodeState) {
	e.tagName = string(n)
}

func WithTagName(s string) Option {
	return tagNameOption(s)
}
