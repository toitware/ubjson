package ubjson

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

func Marshal(v interface{}, options ...MarshalOption) ([]byte, error) {
	e := newEncodeState()
	for _, o := range options {
		o.marshalApply(e)
	}

	err := e.marshal(v, encOpts{})
	if err != nil {
		return nil, err
	}
	buf := append([]byte(nil), e.Bytes()...)

	encodeStatePool.Put(e)

	return buf, nil
}

// Marshaler is the interface implemented by types that
// can marshal themselves into valid UBJSON.
type Marshaler interface {
	MarshalUBJSON() ([]byte, error)
}

// An UnsupportedTypeError is returned by Marshal when attempting
// to encode an unsupported value type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return "ubjson: unsupported type: " + e.Type.String()
}

type UnsupportedValueError struct {
	Value reflect.Value
	Str   string
}

func (e *UnsupportedValueError) Error() string {
	return "ubjson: unsupported value: " + e.Str
}

// A MarshalerError represents an error from calling a MarshalUBJSON or MarshalText method.
type MarshalerError struct {
	Type reflect.Type
	Err  error
}

func (e *MarshalerError) Error() string {
	return "ubjson: error calling MarshalUBJSON for type " + e.Type.String() + ": " + e.Err.Error()
}

func (e *MarshalerError) Unwrap() error { return e.Err }

// An encodeState encodes UBJSON into a bytes.Buffer.
type encodeState struct {
	bytes.Buffer // accumulated output
	scratch      [9]byte
	tagName      string
}

var encodeStatePool sync.Pool

func newEncodeState() *encodeState {
	if v := encodeStatePool.Get(); v != nil {
		e := v.(*encodeState)
		e.Reset()
		e.tagName = "ubjson"
		return e
	}
	e := new(encodeState)
	e.tagName = "ubjson"
	return e
}

// ubjsonError is an error wrapper type for internal use only.
// Panics with errors are wrapped in ubjsonError so that the top-level recover
// can distinguish intentional panics from this package.
type ubjsonError struct{ error }

func (e *encodeState) marshal(v interface{}, opts encOpts) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if je, ok := r.(ubjsonError); ok {
				err = je.error
			} else {
				panic(r)
			}
		}
	}()
	e.reflectValue(reflect.ValueOf(v), opts)
	return nil
}

// error aborts the encoding by panicking with err wrapped in ubjsonError.
func (e *encodeState) error(err error) {
	panic(ubjsonError{err})
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}

func (e *encodeState) reflectValue(v reflect.Value, opts encOpts) {
	valueEncoder(v, e.tagName)(e, v, opts)
}

type encOpts struct {
}

type encoderFunc func(e *encodeState, v reflect.Value, opts encOpts)

var typeEncoderCache sync.Map // map[tagName]map[reflect.Type]encoderFunc

func valueEncoder(v reflect.Value, tagName string) encoderFunc {
	if !v.IsValid() {
		return invalidValueEncoder
	}
	return typeEncoder(v.Type(), tagName)
}

func typeEncoder(t reflect.Type, tagName string) encoderFunc {
	m, ok := typeEncoderCache.Load(tagName)
	if !ok {
		m, _ = typeEncoderCache.LoadOrStore(tagName, &sync.Map{})
	}
	encoderCache := m.(*sync.Map)

	if fi, ok := encoderCache.Load(t); ok {
		return fi.(encoderFunc)
	}

	// To deal with recursive types, populate the map with an
	// indirect func before we build it. This type waits on the
	// real func (f) to be ready and then calls it. This indirect
	// func is only used for recursive types.
	var (
		wg sync.WaitGroup
		f  encoderFunc
	)
	wg.Add(1)
	fi, loaded := encoderCache.LoadOrStore(t, encoderFunc(func(e *encodeState, v reflect.Value, opts encOpts) {
		wg.Wait()
		f(e, v, opts)
	}))
	if loaded {
		return fi.(encoderFunc)
	}

	// Compute the real encoder and replace the indirect func with it.
	f = newTypeEncoder(t, true, tagName)
	wg.Done()
	encoderCache.Store(t, f)
	return f
}

var (
//marshalerType = reflect.TypeOf((*Marshaler)(nil)).Elem()
//textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
)

// newTypeEncoder constructs an encoderFunc for a type.
// The returned encoder only checks CanAddr when allowAddr is true.
func newTypeEncoder(t reflect.Type, allowAddr bool, tagName string) encoderFunc {
	/*
		if t.Implements(marshalerType) {
			return marshalerEncoder
		}
		if t.Kind() != reflect.Ptr && allowAddr && reflect.PtrTo(t).Implements(marshalerType) {
			return newCondAddrEncoder(addrMarshalerEncoder, newTypeEncoder(t, false))
		}

		if t.Implements(textMarshalerType) {
			return textMarshalerEncoder
		}
		if t.Kind() != reflect.Ptr && allowAddr && reflect.PtrTo(t).Implements(textMarshalerType) {
			return newCondAddrEncoder(addrTextMarshalerEncoder, newTypeEncoder(t, false))
		}
	*/

	switch t.Kind() {
	case reflect.Bool:
		return boolEncoder
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intEncoder
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return uintEncoder
	case reflect.Float32:
		return float32Encoder
	case reflect.Float64:
		return float64Encoder
	case reflect.String:
		return stringEncoder
	case reflect.Interface:
		return interfaceEncoder
	case reflect.Struct:
		return newStructEncoder(t, tagName)
	case reflect.Map:
		return newMapEncoder(t, tagName)
	case reflect.Slice:
		return newSliceEncoder(t, tagName)
	case reflect.Array:
		return newArrayEncoder(t, tagName)
	case reflect.Ptr:
		return newPtrEncoder(t, tagName)
	default:
		return unsupportedTypeEncoder
	}
}

func invalidValueEncoder(e *encodeState, v reflect.Value, _ encOpts) {
	e.WriteByte(markerNullLiteral)
}

func boolEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	if v.Bool() {
		e.WriteByte(markerTrueLiteral)
	} else {
		e.WriteByte(markerFalseLiteral)
	}
}

func writeInt(e *encodeState, v int64) {
	if v >= 0 {
		if v <= math.MaxUint8 {
			e.WriteByte(markerUint8Literal)
			e.WriteByte(byte(v))
		} else if v <= math.MaxInt16 {
			e.scratch[0] = markerInt16Literal
			binary.BigEndian.PutUint16(e.scratch[1:], uint16(v))
			e.Write(e.scratch[:3])
		} else if v <= math.MaxInt32 {
			e.scratch[0] = markerInt32Literal
			binary.BigEndian.PutUint32(e.scratch[1:], uint32(v))
			e.Write(e.scratch[:5])
		} else {
			e.scratch[0] = markerInt64Literal
			binary.BigEndian.PutUint64(e.scratch[1:], uint64(v))
			e.Write(e.scratch[:9])
		}
	} else {
		if v >= math.MinInt8 {
			e.WriteByte(markerInt8Literal)
			e.WriteByte(uint8(int8(v)))
		} else if v >= math.MinInt16 {
			e.scratch[0] = markerInt16Literal
			binary.BigEndian.PutUint16(e.scratch[1:], uint16(int16(v)))
			e.Write(e.scratch[:3])
		} else if v >= math.MinInt32 {
			e.scratch[0] = markerInt32Literal
			binary.BigEndian.PutUint32(e.scratch[1:], uint32(int32(v)))
			e.Write(e.scratch[:5])
		} else {
			e.scratch[0] = markerInt64Literal
			binary.BigEndian.PutUint64(e.scratch[1:], uint64(v))
			e.Write(e.scratch[:9])
		}
	}
}

func intEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	writeInt(e, v.Int())
}

func uintEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	u := v.Uint()
	if u > math.MaxInt64 {
		e.error(fmt.Errorf("integer overflow writing %v", u))
	}
	writeInt(e, int64(u))
}

func float32Encoder(e *encodeState, v reflect.Value, opts encOpts) {
	f := v.Float()
	if math.IsInf(f, 0) || math.IsNaN(f) {
		e.error(&UnsupportedValueError{v, strconv.FormatFloat(f, 'g', -1, 32)})
	}
	e.scratch[0] = markerFloat32Literal
	binary.BigEndian.PutUint32(e.scratch[1:], math.Float32bits(float32(f)))
	e.Write(e.scratch[:5])
}

func writeFloat(e *encodeState, f float64) {
	e.scratch[0] = markerFloat64Literal
	binary.BigEndian.PutUint64(e.scratch[1:], math.Float64bits(f))
	e.Write(e.scratch[:9])
}

func float64Encoder(e *encodeState, v reflect.Value, opts encOpts) {
	f := v.Float()
	if math.IsInf(f, 0) || math.IsNaN(f) {
		e.error(&UnsupportedValueError{v, strconv.FormatFloat(f, 'g', -1, 64)})
	}
	writeFloat(e, f)
}

func writeString(e *encodeState, s string) {
	writeInt(e, int64(len(s)))
	e.WriteString(s)
}

func stringEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	if v.Type() == jsonNumberType {
		str := v.String()
		n, err := strconv.ParseInt(str, 10, 64)
		if err == nil {
			writeInt(e, n)
			return
		}

		f, err := strconv.ParseFloat(str, 64)
		if err == nil {
			writeFloat(e, f)
			return
		}

		e.error(&UnsupportedValueError{v, str})
		return
	}

	e.WriteByte(markerStringLiteral)
	writeString(e, v.String())
}

func interfaceEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		e.WriteByte(markerNullLiteral)
		return
	}
	e.reflectValue(v.Elem(), opts)
}

type structEncoder struct {
	fields structFields
}

type structFields struct {
	list      []field
	nameIndex map[string]int
}

func (se structEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	e.WriteByte(markerObjectBegin)
FieldLoop:
	for i := range se.fields.list {
		f := &se.fields.list[i]

		// Find the nested struct field by following f.index.
		fv := v
		for _, i := range f.index {
			if fv.Kind() == reflect.Ptr {
				if fv.IsNil() {
					continue FieldLoop
				}
				fv = fv.Elem()
			}
			fv = fv.Field(i)
		}

		if f.omitEmpty && isEmptyValue(fv) {
			continue
		}

		writeString(e, f.name)
		f.encoder(e, fv, opts)
	}
	e.WriteByte(markerObjectEnd)
}

func newStructEncoder(t reflect.Type, tagName string) encoderFunc {
	se := structEncoder{fields: cachedTypeFields(t, tagName)}
	return se.encode
}

type mapEncoder struct {
	elemEnc encoderFunc
}

func (me mapEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		e.WriteByte(markerNullLiteral)
		return
	}
	e.WriteByte(markerObjectBegin)

	// Extract and sort the keys.
	keys := v.MapKeys()
	sv := make([]reflectWithString, len(keys))
	for i, v := range keys {
		sv[i].v = v
		if err := sv[i].resolve(); err != nil {
			e.error(&MarshalerError{v.Type(), err})
		}
	}
	sort.Slice(sv, func(i, j int) bool { return sv[i].s < sv[j].s })

	for _, kv := range sv {
		writeString(e, kv.s)
		me.elemEnc(e, v.MapIndex(kv.v), opts)
	}
	e.WriteByte(markerObjectEnd)
}

func newMapEncoder(t reflect.Type, tagName string) encoderFunc {
	switch t.Key().Kind() {
	case reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
	default:
		//if !t.Key().Implements(textMarshalerType) {
		return unsupportedTypeEncoder
		//}
	}
	me := mapEncoder{typeEncoder(t.Elem(), tagName)}
	return me.encode
}

func encodeByteSlice(e *encodeState, v reflect.Value, _ encOpts) {
	if v.IsNil() {
		e.WriteByte(markerNullLiteral)
		return
	}
	s := v.Bytes()
	e.WriteByte(markerArrayBegin)
	e.WriteByte(markerType)
	e.WriteByte(markerUint8Literal)
	e.WriteByte(markerCount)
	writeInt(e, int64(len(s)))
	e.Write(s)
}

// sliceEncoder just wraps an arrayEncoder, checking to make sure the value isn't nil.
type sliceEncoder struct {
	arrayEnc encoderFunc
}

func (se sliceEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		e.WriteByte(markerNullLiteral)
		return
	}
	se.arrayEnc(e, v, opts)
}

func newSliceEncoder(t reflect.Type, tagName string) encoderFunc {
	// Byte slices get special treatment; arrays don't.
	if t.Elem().Kind() == reflect.Uint8 {
		//p := reflect.PtrTo(t.Elem())
		// if !p.Implements(marshalerType) && !p.Implements(textMarshalerType) {
		return encodeByteSlice
		//}
	}
	enc := sliceEncoder{newArrayEncoder(t, tagName)}
	return enc.encode
}

type arrayEncoder struct {
	elemEnc encoderFunc
}

func (ae arrayEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	e.WriteByte(markerArrayBegin)
	n := v.Len()
	e.WriteByte(markerCount)
	writeInt(e, int64(n))
	for i := 0; i < n; i++ {
		ae.elemEnc(e, v.Index(i), opts)
	}
}

func newArrayEncoder(t reflect.Type, tagName string) encoderFunc {
	enc := arrayEncoder{typeEncoder(t.Elem(), tagName)}
	return enc.encode
}

type ptrEncoder struct {
	elemEnc encoderFunc
}

func (pe ptrEncoder) encode(e *encodeState, v reflect.Value, opts encOpts) {
	if v.IsNil() {
		e.WriteByte(markerNullLiteral)
		return
	}
	pe.elemEnc(e, v.Elem(), opts)
}

func newPtrEncoder(t reflect.Type, tagName string) encoderFunc {
	enc := ptrEncoder{typeEncoder(t.Elem(), tagName)}
	return enc.encode
}

func unsupportedTypeEncoder(e *encodeState, v reflect.Value, _ encOpts) {
	e.error(&UnsupportedTypeError{v.Type()})
}

func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:<=>?@[]^_{|}~ ", c):
			// Backslash and quote chars are reserved, but
			// otherwise any punctuation chars are allowed
			// in a tag name.
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}

func typeByIndex(t reflect.Type, index []int) reflect.Type {
	for _, i := range index {
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		t = t.Field(i).Type
	}
	return t
}

type reflectWithString struct {
	v reflect.Value
	s string
}

func (w *reflectWithString) resolve() error {
	if w.v.Kind() == reflect.String {
		w.s = w.v.String()
		return nil
	}
	if tm, ok := w.v.Interface().(encoding.TextMarshaler); ok {
		if w.v.Kind() == reflect.Ptr && w.v.IsNil() {
			return nil
		}
		buf, err := tm.MarshalText()
		w.s = string(buf)
		return err
	}
	switch w.v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		w.s = strconv.FormatInt(w.v.Int(), 10)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		w.s = strconv.FormatUint(w.v.Uint(), 10)
		return nil
	}
	panic("unexpected map key type")
}

// A field represents a single field found in a struct.
type field struct {
	name      string
	nameBytes []byte                 // []byte(name)
	equalFold func(s, t []byte) bool // bytes.EqualFold or equivalent

	tag       bool
	index     []int
	typ       reflect.Type
	omitEmpty bool

	encoder encoderFunc
}

// byIndex sorts field by index sequence.
type byIndex []field

func (x byIndex) Len() int { return len(x) }

func (x byIndex) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byIndex) Less(i, j int) bool {
	for k, xik := range x[i].index {
		if k >= len(x[j].index) {
			return false
		}
		if xik != x[j].index[k] {
			return xik < x[j].index[k]
		}
	}
	return len(x[i].index) < len(x[j].index)
}

// typeFields returns a list of fields that UBJSON should recognize for the given type.
// The algorithm is breadth-first search over the set of structs to include - the top struct
// and then any reachable anonymous structs.
func typeFields(t reflect.Type, tagName string) structFields {
	// Anonymous fields to explore at the current level and the next.
	current := []field{}
	next := []field{{typ: t}}

	// Count of queued names for current level and the next.
	var count, nextCount map[reflect.Type]int

	// Types already visited at an earlier level.
	visited := map[reflect.Type]bool{}

	// Fields found.
	var fields []field

	for len(next) > 0 {
		current, next = next, current[:0]
		count, nextCount = nextCount, map[reflect.Type]int{}

		for _, f := range current {
			if visited[f.typ] {
				continue
			}
			visited[f.typ] = true

			// Scan f.typ for fields to include.
			for i := 0; i < f.typ.NumField(); i++ {
				sf := f.typ.Field(i)
				isUnexported := sf.PkgPath != ""
				if sf.Anonymous {
					t := sf.Type
					if t.Kind() == reflect.Ptr {
						t = t.Elem()
					}
					if isUnexported && t.Kind() != reflect.Struct {
						// Ignore embedded fields of unexported non-struct types.
						continue
					}
					// Do not ignore embedded fields of unexported struct types
					// since they may have exported fields.
				} else if isUnexported {
					// Ignore unexported non-embedded fields.
					continue
				}
				tag := sf.Tag.Get(tagName)
				if tag == "-" {
					continue
				}
				name, opts := parseTag(tag)
				if !isValidTag(name) {
					name = ""
				}
				index := make([]int, len(f.index)+1)
				copy(index, f.index)
				index[len(f.index)] = i

				ft := sf.Type
				if ft.Name() == "" && ft.Kind() == reflect.Ptr {
					// Follow pointer.
					ft = ft.Elem()
				}

				// Record found field and index sequence.
				if name != "" || !sf.Anonymous || ft.Kind() != reflect.Struct {
					tagged := name != ""
					if name == "" {
						name = sf.Name
					}
					field := field{
						name:      name,
						tag:       tagged,
						index:     index,
						typ:       ft,
						omitEmpty: opts.Contains("omitempty"),
					}
					field.nameBytes = []byte(field.name)
					field.equalFold = foldFunc(field.nameBytes)

					fields = append(fields, field)
					if count[f.typ] > 1 {
						// If there were multiple instances, add a second,
						// so that the annihilation code will see a duplicate.
						// It only cares about the distinction between 1 or 2,
						// so don't bother generating any more copies.
						fields = append(fields, fields[len(fields)-1])
					}
					continue
				}

				// Record new anonymous struct to explore in next round.
				nextCount[ft]++
				if nextCount[ft] == 1 {
					next = append(next, field{name: ft.Name(), index: index, typ: ft})
				}
			}
		}
	}

	sort.Slice(fields, func(i, j int) bool {
		x := fields
		// sort field by name, breaking ties with depth, then
		// breaking ties with "name came from ubjson tag", then
		// breaking ties with index sequence.
		if x[i].name != x[j].name {
			return x[i].name < x[j].name
		}
		if len(x[i].index) != len(x[j].index) {
			return len(x[i].index) < len(x[j].index)
		}
		if x[i].tag != x[j].tag {
			return x[i].tag
		}
		return byIndex(x).Less(i, j)
	})

	// Delete all fields that are hidden by the Go rules for embedded fields,
	// except that fields with UBJSON tags are promoted.

	// The fields are sorted in primary order of name, secondary order
	// of field index length. Loop over names; for each name, delete
	// hidden fields by choosing the one dominant field that survives.
	out := fields[:0]
	for advance, i := 0, 0; i < len(fields); i += advance {
		// One iteration per name.
		// Find the sequence of fields with the name of this first field.
		fi := fields[i]
		name := fi.name
		for advance = 1; i+advance < len(fields); advance++ {
			fj := fields[i+advance]
			if fj.name != name {
				break
			}
		}
		if advance == 1 { // Only one field with this name
			out = append(out, fi)
			continue
		}
		dominant, ok := dominantField(fields[i : i+advance])
		if ok {
			out = append(out, dominant)
		}
	}

	fields = out
	sort.Sort(byIndex(fields))

	for i := range fields {
		f := &fields[i]
		f.encoder = typeEncoder(typeByIndex(t, f.index), tagName)
	}

	nameIndex := make(map[string]int, len(fields))
	for i, field := range fields {
		nameIndex[field.name] = i
	}
	return structFields{fields, nameIndex}
}

// dominantField looks through the fields, all of which are known to
// have the same name, to find the single field that dominates the
// others using Go's embedding rules, modified by the presence of
// UBJSON tags. If there are multiple top-level fields, the boolean
// will be false: This condition is an error in Go and we skip all
// the fields.
func dominantField(fields []field) (field, bool) {
	// The fields are sorted in increasing index-length order, then by presence of tag.
	// That means that the first field is the dominant one. We need only check
	// for error cases: two fields at top level, either both tagged or neither tagged.
	if len(fields) > 1 && len(fields[0].index) == len(fields[1].index) && fields[0].tag == fields[1].tag {
		return field{}, false
	}
	return fields[0], true
}

var typeFieldCache sync.Map // map[tagName]map[reflect.Type]structFields

// cachedTypeFields is like typeFields but uses a cache to avoid repeated work.
func cachedTypeFields(t reflect.Type, tagName string) structFields {
	m, ok := typeFieldCache.Load(tagName)
	if !ok {
		m, _ = typeFieldCache.LoadOrStore(tagName, &sync.Map{})
	}
	fieldCache := m.(*sync.Map)

	if f, ok := fieldCache.Load(t); ok {
		return f.(structFields)
	}
	f, _ := fieldCache.LoadOrStore(t, typeFields(t, tagName))
	return f.(structFields)
}
