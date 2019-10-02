package ubjson

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// phasePanicMsg is used as a panic message when we end up with something that
// shouldn't happen. It can indicate a bug in the JSON decoder, or that
// something is editing the data slice while the decoder executes.
const phasePanicMsg = "UBJSON decoder out of sync - data changing underfoot?"

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

func Unmarshal(data []byte, v interface{}) error {
	// Check for well-formedness.
	// Avoids filling out half a data structure
	// before discovering a JSON syntax error.

	var d decodeState
	/*
		err := checkValid(data, &d.scan)
		if err != nil {
			return err
		}
	*/

	d.init(data)
	return d.unmarshal(v)
}

// Unmarshaler is the interface implemented by types
// that can unmarshal a UBJSON description of themselves.
// The input can be assumed to be a valid encoding of
// a UBJSON value. UnmarshalUBJSON must copy the UBJSON data
// if it wishes to retain the data after returning.
//
// By convention, to approximate the behavior of Unmarshal itself,
// Unmarshalers implement UnmarshalUBJSON([]byte("null")) as a no-op.
type Unmarshaler interface {
	UnmarshalUBJSON([]byte) error
}

// An UnmarshalTypeError describes a UBJSON value that was
// not appropriate for a value of a specific Go type.
type UnmarshalTypeError struct {
	Value  string       // description of UBJSON value - "bool", "array", "number -5"
	Type   reflect.Type // type of Go value it could not be assigned to
	Offset int64        // error occurred after reading Offset bytes
	Struct string       // name of the struct type containing the field
	Field  string       // the full path from root node to the field
}

func (e *UnmarshalTypeError) Error() string {
	if e.Struct != "" || e.Field != "" {
		return "ubjson: cannot unmarshal " + e.Value + " into Go struct field " + e.Struct + "." + e.Field + " of type " + e.Type.String()
	}
	return "ubjson: cannot unmarshal " + e.Value + " into Go value of type " + e.Type.String()
}

// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "ubjson: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "ubjson: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "ubjson: Unmarshal(nil " + e.Type.String() + ")"
}

// decodeState represents the state while decoding a UBJSON value.
type decodeState struct {
	data         []byte
	off          int // next read offset in data
	scan         scanner
	errorContext struct { // provides context for type errors
		Struct     reflect.Type
		FieldStack []string
	}
	savedError            error
	useNumber             bool
	disallowUnknownFields bool
}

// readIndex returns the position of the last byte read.
func (d *decodeState) readIndex() int {
	return d.off - 1
}

func (d *decodeState) unmarshal(v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return &InvalidUnmarshalError{reflect.TypeOf(v)}
	}

	d.scan.reset()

	// We decode rv not rv.Elem because the Unmarshaler interface
	// test must be applied at the top level of the value.
	err := d.value(d.scanNext(), rv)
	if err != nil {
		return d.addErrorContext(err)
	}
	return d.savedError
}

// TODO: Remove!
// A Number represents a JSON number literal.
type Number string

func (d *decodeState) init(data []byte) *decodeState {
	d.data = data
	d.off = 0
	d.savedError = nil
	d.errorContext.Struct = nil

	// Reuse the allocated space for the FieldStack slice.
	d.errorContext.FieldStack = d.errorContext.FieldStack[:0]
	return d
}

// saveError saves the first err it is called with,
// for reporting at the end of the unmarshal.
func (d *decodeState) saveError(err error) {
	if d.savedError == nil {
		d.savedError = d.addErrorContext(err)
	}
}

// addErrorContext returns a new error enhanced with information from d.errorContext
func (d *decodeState) addErrorContext(err error) error {
	if d.errorContext.Struct != nil || len(d.errorContext.FieldStack) > 0 {
		switch err := err.(type) {
		case *UnmarshalTypeError:
			err.Struct = d.errorContext.Struct.Name()
			err.Field = strings.Join(d.errorContext.FieldStack, ".")
			return err
		}
	}
	return err
}

// value consumes a UBJSON value from d.data[d.off-1:], decoding into v, and
// reads the following byte ahead. If v is invalid, the value is discarded.
// The first byte of the value has been read already.
func (d *decodeState) value(marker byte, v reflect.Value) error {
	switch d.scan.readOpcode(marker) {
	default:
		return d.syntaxError(marker, "looking for beginning of value")

	case scanBeginLiteral:
		start := d.off

		if err := d.skipLiteral(marker); err != nil {
			return err
		}

		if v.IsValid() {
			if err := d.literalStore(marker, d.data[start:d.off], v); err != nil {
				return err
			}
		}

	case scanBeginObject:
		if v.IsValid() {
			if err := d.object(v); err != nil {
				return err
			}
		} else {
			d.skip()
		}

	}

	return nil
}

// skip scans to the end of what was started.
func (d *decodeState) skip() {
	panic("unimplemented")

	s, data, i := &d.scan, d.data, d.off
	depth := len(s.parseState)
	for {
		op := s.step(s, data[i])
		i++
		if len(s.parseState) < depth {
			d.off = i
			d.opcode = op
			return
		}
	}
}

func (d *decodeState) skipLiteral(marker byte) error {
	switch marker {
	default:
		panic(phasePanicMsg)
	case markerNullLiteral:
	case markerTrueLiteral:
	case markerFalseLiteral:

	case markerInt8Literal, markerUint8Literal:
		d.off++
	case markerInt16Literal:
		d.off += 2
	case markerInt32Literal:
		d.off += 4
	case markerInt64Literal:
		d.off += 8
	case markerStringLiteral:
		fmt.Println("string")
		length, err := d.readLength()
		fmt.Println(length, err)
		if err != nil {
			return err
		}

		d.off += length
	}

	return nil
}

func (d *decodeState) readLength() (int, error) {
	marker := d.scanNext()

	switch marker {
	case markerInt8Literal, markerUint8Literal, markerInt16Literal, markerInt32Literal, markerInt64Literal:
		start := d.off
		if err := d.skipLiteral(marker); err != nil {
			return 0, err
		}
		v, _ := extractNumber(marker, d.data[start:d.off])
		return int(v), nil
	}

	return 0, d.syntaxError(marker, "expected string length")
}

func (d *decodeState) eof() error {
	if d.off > len(d.data) {
		return &SyntaxError{"unexpected end of UBJSON input", int64(d.off)}
	}

	return nil
}

func (d *decodeState) syntaxError(c byte, context string) error {
	return &SyntaxError{"invalid character " + quoteChar(c) + " " + context, int64(d.readIndex())}
}

// scanNext processes the byte at d.data[d.off].
func (d *decodeState) scanNext() byte {
	if d.off < len(d.data) {
		v := d.data[d.off]
		d.off++
		return v
	} else {
		d.off = len(d.data) + 1 // mark processed EOF with len+1
		return 0
	}
}

// object consumes an object from d.data[d.off-1:], decoding into v.
// The first byte ('{') of the object has been read already.
func (d *decodeState) object(v reflect.Value) error {
	fmt.Println("obj", d.data[d.readIndex():])
	// Check for unmarshaler.
	u, ut, pv := indirect(v, false)
	if u != nil {
		start := d.readIndex()
		d.skip()
		return u.UnmarshalUBJSON(d.data[start:d.off])
	}
	if ut != nil {
		d.saveError(&UnmarshalTypeError{Value: "object", Type: v.Type(), Offset: int64(d.off)})
		d.skip()
		return nil
	}
	v = pv
	t := v.Type()

	// Decoding into nil interface? Switch to non-reflect code.
	if v.Kind() == reflect.Interface && v.NumMethod() == 0 {
		oi := d.objectInterface()
		v.Set(reflect.ValueOf(oi))
		return nil
	}

	var fields structFields

	// Check type of target:
	//   struct or
	//   map[T1]T2 where T1 is string, an integer type,
	//             or an encoding.TextUnmarshaler
	switch v.Kind() {
	case reflect.Map:
		// Map key must either have string kind, have an integer kind,
		// or be an encoding.TextUnmarshaler.
		switch t.Key().Kind() {
		case reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		default:
			if !reflect.PtrTo(t.Key()).Implements(textUnmarshalerType) {
				d.saveError(&UnmarshalTypeError{Value: "object", Type: t, Offset: int64(d.off)})
				d.skip()
				return nil
			}
		}
		if v.IsNil() {
			v.Set(reflect.MakeMap(t))
		}
	case reflect.Struct:
		fields = cachedTypeFields(t)
		// ok
	default:
		d.saveError(&UnmarshalTypeError{Value: "object", Type: t, Offset: int64(d.off)})
		d.skip()
		return nil
	}

	var mapElem reflect.Value
	origErrorContext := d.errorContext

	fmt.Println(d.data[d.readIndex():])

	for {
		if err := d.eof(); err != nil {
			return err
		}
		// Read opening " of string key or closing }.
		if d.data[d.off] == markerObjectEnd {
			break
		}

		// Read key.
		start := d.off
		fmt.Println(d.data[start:])
		if err := d.skipLiteral(markerStringLiteral); err != nil {
			return err
		}
		item := d.data[start:d.off]
		fmt.Println(item)
		key := extractString(item)
		fmt.Println("key", key)
		fmt.Println(d.data[d.off:])

		// Figure out field corresponding to key.
		var subv reflect.Value

		if v.Kind() == reflect.Map {
			elemType := t.Elem()
			if !mapElem.IsValid() {
				mapElem = reflect.New(elemType).Elem()
			} else {
				mapElem.Set(reflect.Zero(elemType))
			}
			subv = mapElem
		} else {
			var f *field
			if i, ok := fields.nameIndex[string(key)]; ok {
				// Found an exact name match.
				f = &fields.list[i]
			} else {
				// Fall back to the expensive case-insensitive
				// linear search.
				for i := range fields.list {
					ff := &fields.list[i]
					if ff.equalFold(ff.nameBytes, key) {
						f = ff
						break
					}
				}
			}
			if f != nil {
				subv = v
				for _, i := range f.index {
					if subv.Kind() == reflect.Ptr {
						if subv.IsNil() {
							// If a struct embeds a pointer to an unexported type,
							// it is not possible to set a newly allocated value
							// since the field is unexported.
							//
							// See https://golang.org/issue/21357
							if !subv.CanSet() {
								d.saveError(fmt.Errorf("ubjson: cannot set embedded pointer to unexported struct: %v", subv.Type().Elem()))
								// Invalidate subv to ensure d.value(subv) skips over
								// the JSON value without assigning it to subv.
								subv = reflect.Value{}
								break
							}
							subv.Set(reflect.New(subv.Type().Elem()))
						}
						subv = subv.Elem()
					}
					subv = subv.Field(i)
				}
				d.errorContext.FieldStack = append(d.errorContext.FieldStack, f.name)
				d.errorContext.Struct = t
			} else if d.disallowUnknownFields {
				d.saveError(fmt.Errorf("ubjson: unknown field %q", key))
			}
		}

		if err := d.value(d.scanNext(), subv); err != nil {
			return err
		}

		// Write value back to map;
		// if using struct, subv points into struct already.
		if v.Kind() == reflect.Map {
			kt := t.Key()
			var kv reflect.Value
			switch {
			case kt.Kind() == reflect.String:
				kv = reflect.ValueOf(key).Convert(kt)
			case reflect.PtrTo(kt).Implements(textUnmarshalerType):
				kv = reflect.New(kt)
				// TODO: Not right
				if err := d.literalStore(d.scanNext(), item, kv); err != nil {
					return err
				}
				kv = kv.Elem()
			default:
				switch kt.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					s := string(key)
					n, err := strconv.ParseInt(s, 10, 64)
					if err != nil || reflect.Zero(kt).OverflowInt(n) {
						d.saveError(&UnmarshalTypeError{Value: "number " + s, Type: kt, Offset: int64(start + 1)})
						break
					}
					kv = reflect.ValueOf(n).Convert(kt)
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
					s := string(key)
					n, err := strconv.ParseUint(s, 10, 64)
					if err != nil || reflect.Zero(kt).OverflowUint(n) {
						d.saveError(&UnmarshalTypeError{Value: "number " + s, Type: kt, Offset: int64(start + 1)})
						break
					}
					kv = reflect.ValueOf(n).Convert(kt)
				default:
					panic("json: Unexpected key type") // should never occur
				}
			}
			if kv.IsValid() {
				v.SetMapIndex(kv, subv)
			}
		}

		// Next token must be , or }.

		// Reset errorContext to its original state.
		// Keep the same underlying array for FieldStack, to reuse the
		// space and avoid unnecessary allocs.
		d.errorContext.FieldStack = d.errorContext.FieldStack[:len(origErrorContext.FieldStack)]
		d.errorContext.Struct = origErrorContext.Struct
	}

	return nil
}

func (d *decodeState) literalStore(marker byte, item []byte, v reflect.Value) error {
	fmt.Println("store", string([]byte{marker}), item)
	// Check for unmarshaler.
	if marker == 0 {
		//Empty string given
		d.saveError(fmt.Errorf("ubjson: invalid use of ,string struct tag, trying to unmarshal %q into %v", item, v.Type()))
		return nil
	}
	u, ut, pv := indirect(v, marker == markerNullLiteral)
	if u != nil {
		return u.UnmarshalUBJSON(item)
	}

	if ut != nil {
		if item[0] != '"' {
			val := "number"
			switch item[0] {
			case 'n':
				val = "null"
			case 't', 'f':
				val = "bool"
			}
			d.saveError(&UnmarshalTypeError{Value: val, Type: v.Type(), Offset: int64(d.readIndex())})
			return nil
		}

		return fmt.Errorf("unmarhsal text??")
		// TODO: Makes sense???
		// return ut.UnmarshalText(s)
	}

	v = pv

	switch marker {
	case markerNullLiteral: // null
		switch v.Kind() {
		case reflect.Interface, reflect.Ptr, reflect.Map, reflect.Slice:
			v.Set(reflect.Zero(v.Type()))
			// otherwise, ignore null for primitives/string
		}
	case markerTrueLiteral, markerFalseLiteral:
		value := marker == markerTrueLiteral
		switch v.Kind() {
		default:
			d.saveError(&UnmarshalTypeError{Value: "bool", Type: v.Type(), Offset: int64(d.readIndex())})
		case reflect.Bool:
			v.SetBool(value)
		case reflect.Interface:
			if v.NumMethod() == 0 {
				v.Set(reflect.ValueOf(value))
			} else {
				d.saveError(&UnmarshalTypeError{Value: "bool", Type: v.Type(), Offset: int64(d.readIndex())})
			}
		}
	case markerInt8Literal, markerUint8Literal, markerInt16Literal, markerInt32Literal, markerInt64Literal:
		n, _ := extractNumber(marker, item)
		switch v.Kind() {
		default:
			d.saveError(&UnmarshalTypeError{Value: "number", Type: v.Type(), Offset: int64(d.readIndex())})
		case reflect.Interface:
			if v.NumMethod() != 0 {
				d.saveError(&UnmarshalTypeError{Value: "number", Type: v.Type(), Offset: int64(d.readIndex())})
				break
			}
			v.Set(reflect.ValueOf(n))

		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if v.OverflowInt(n) {
				d.saveError(&UnmarshalTypeError{Value: "number " + strconv.FormatInt(n, 10), Type: v.Type(), Offset: int64(d.readIndex())})
				break
			}
			v.SetInt(n)

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			u := uint64(n)
			if v.OverflowUint(u) {
				d.saveError(&UnmarshalTypeError{Value: "number " + strconv.FormatUint(u, 10), Type: v.Type(), Offset: int64(d.readIndex())})
				break
			}
			v.SetUint(u)
			/*
				case reflect.Float32, reflect.Float64:
					n, err := strconv.ParseFloat(s, v.Type().Bits())
					if err != nil || v.OverflowFloat(n) {
						d.saveError(&UnmarshalTypeError{Value: "number " + s, Type: v.Type(), Offset: int64(d.readIndex())})
						break
					}
					v.SetFloat(n)
			*/
		}

	case markerStringLiteral:
		s := extractString(item)

		switch v.Kind() {
		default:
			d.saveError(&UnmarshalTypeError{Value: "string", Type: v.Type(), Offset: int64(d.readIndex())})
		case reflect.Slice:
			if v.Type().Elem().Kind() != reflect.Uint8 {
				d.saveError(&UnmarshalTypeError{Value: "string", Type: v.Type(), Offset: int64(d.readIndex())})
				break
			}
			// TODO: Copy?
			v.SetBytes(s)
		case reflect.String:
			/*
				if v.Type() == numberType && !isValidNumber(string(s)) {
					return fmt.Errorf("json: invalid number literal, trying to unmarshal %q into Number", item)
				}
			*/
			v.SetString(string(s))
		case reflect.Interface:
			if v.NumMethod() == 0 {
				v.Set(reflect.ValueOf(string(s)))
			} else {
				d.saveError(&UnmarshalTypeError{Value: "string", Type: v.Type(), Offset: int64(d.readIndex())})
			}
		}

	default:
		panic(phasePanicMsg)
	}
	return nil
}

// indirect walks down v allocating pointers as needed,
// until it gets to a non-pointer.
// If it encounters an Unmarshaler, indirect stops and returns that.
// If decodingNull is true, indirect stops at the first settable pointer so it
// can be set to nil.
func indirect(v reflect.Value, decodingNull bool) (Unmarshaler, encoding.TextUnmarshaler, reflect.Value) {
	// Issue #24153 indicates that it is generally not a guaranteed property
	// that you may round-trip a reflect.Value by calling Value.Addr().Elem()
	// and expect the value to still be settable for values derived from
	// unexported embedded struct fields.
	//
	// The logic below effectively does this when it first addresses the value
	// (to satisfy possible pointer methods) and continues to dereference
	// subsequent pointers as necessary.
	//
	// After the first round-trip, we set v back to the original value to
	// preserve the original RW flags contained in reflect.Value.
	v0 := v
	haveAddr := false

	// If v is a named type and is addressable,
	// start with its address, so that if the type has pointer methods,
	// we find them.
	if v.Kind() != reflect.Ptr && v.Type().Name() != "" && v.CanAddr() {
		haveAddr = true
		v = v.Addr()
	}
	for {
		// Load value from interface, but only if the result will be
		// usefully addressable.
		if v.Kind() == reflect.Interface && !v.IsNil() {
			e := v.Elem()
			if e.Kind() == reflect.Ptr && !e.IsNil() && (!decodingNull || e.Elem().Kind() == reflect.Ptr) {
				haveAddr = false
				v = e
				continue
			}
		}

		if v.Kind() != reflect.Ptr {
			break
		}

		if decodingNull && v.CanSet() {
			break
		}

		// Prevent infinite loop if v is an interface pointing to its own address:
		//     var v interface{}
		//     v = &v
		if v.Elem().Kind() == reflect.Interface && v.Elem().Elem() == v {
			v = v.Elem()
			break
		}
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		if v.Type().NumMethod() > 0 && v.CanInterface() {
			if u, ok := v.Interface().(Unmarshaler); ok {
				return u, nil, reflect.Value{}
			}
			if !decodingNull {
				if u, ok := v.Interface().(encoding.TextUnmarshaler); ok {
					return nil, u, reflect.Value{}
				}
			}
		}

		if haveAddr {
			v = v0 // restore original value after round-trip Value.Addr().Elem()
			haveAddr = false
		} else {
			v = v.Elem()
		}
	}
	return nil, nil, v
}

// The xxxInterface routines build up a value to be stored
// in an empty interface. They are not strictly necessary,
// but they avoid the weight of reflection in this common case.

// valueInterface is like value but returns interface{}
func (d *decodeState) valueInterface() (val interface{}) {
	switch d.opcode {
	default:
		panic(phasePanicMsg)
	case scanBeginArray:
		val = d.arrayInterface()
		d.scanNext()
	case scanBeginObject:
		val = d.objectInterface()
		d.scanNext()
	case scanBeginLiteral:
		val = d.literalInterface()
	}
	return
}

// arrayInterface is like array but returns []interface{}.
func (d *decodeState) arrayInterface() []interface{} {
	var v = make([]interface{}, 0)
	for {
		// Look ahead for ] - can only happen on first iteration.
		if d.opcode == scanEndArray {
			break
		}

		v = append(v, d.valueInterface())

		// Next token must be , or ].
		if d.opcode == scanEndArray {
			break
		}
		if d.opcode != scanArrayValue {
			panic(phasePanicMsg)
		}
	}
	return v
}

// objectInterface is like object but returns map[string]interface{}.
func (d *decodeState) objectInterface() map[string]interface{} {
	m := make(map[string]interface{})
	for {
		// Read opening " of string key or closing }.
		if d.opcode == scanEndObject {
			// closing } - can only happen on first iteration.
			break
		}
		if d.opcode != scanBeginLiteral {
			panic(phasePanicMsg)
		}

		// Read string key.
		start := d.readIndex()
		if err := d.skipLiteral(markerStringLiteral); err != nil {
			panic(err)
		}
		item := d.data[start:d.readIndex()]
		key := extractString(item)

		// Read : before value.

		if d.opcode != scanObjectKey {
			panic(phasePanicMsg)
		}

		// Read value.
		m[string(key)] = d.valueInterface()

		// Next token must be , or }.
		if d.opcode == scanEndObject {
			break
		}
		if d.opcode != scanObjectValue {
			panic(phasePanicMsg)
		}
	}
	return m
}

// literalInterface consumes and returns a literal from d.data[d.off-1:] and
// it reads the following byte ahead. The first byte of the literal has been
// read already (that's how the caller knows it's a literal).
func (d *decodeState) literalInterface() interface{} {
	panic("unimplemented")
	// All bytes inside literal return scanContinue op code.
	start := d.readIndex()
	if err := d.skipLiteral(d.scanNext()); err != nil {
		d.saveError(err)
		return nil
	}

	item := d.data[start:d.readIndex()]

	switch c := item[0]; c {
	case markerNullLiteral: // null
		return nil

	case markerTrueLiteral, markerFalseLiteral: // true, false
		return c == markerTrueLiteral

	case '"': // string
		s := extractString(item)
		return string(s)

	default: // number
		n, _ := extractNumber(0, item)
		return n
	}
}

func extractNumber(marker byte, item []byte) (int64, int) {
	switch marker {
	case markerInt8Literal:
		return int64(int8(item[0])), 1
	case markerUint8Literal:
		return int64(item[0]), 1
	case markerInt16Literal:
		return int64(int16(binary.BigEndian.Uint16(item))), 2
	case markerInt32Literal:
		return int64(int32(binary.BigEndian.Uint32(item))), 4
	case markerInt64Literal:
		return int64(binary.BigEndian.Uint64(item)), 8
	}

	panic(phasePanicMsg)
}

func extractString(item []byte) []byte {
	_, bytes := extractNumber(item[0], item[1:])
	return item[1+bytes:]
}
