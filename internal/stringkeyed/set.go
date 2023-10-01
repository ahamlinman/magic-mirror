package stringkeyed

import (
	"encoding/ascii85"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

const (
	shiftOut      = "\x0e"
	unitSeparator = "\x1f"
)

type assertComparable[T comparable] struct{}

var _ assertComparable[Set]

// Set is a set of strings that is comparable with == and !=. The zero value is
// a valid and empty set.
type Set struct {
	// The internal representation of a Set is formed by sorting its raw elements,
	// encoding each one, and concatenating them with the byte 0x1F (the ASCII
	// Unit Separator character) as a separator.
	//
	// The per-element encoding has two forms. If the encoded element begins with
	// the byte 0x0E (the ASCII Shift Out character), the remaining bytes of the
	// encoded element are an Ascii85 encoding of the original raw element.
	// Otherwise, the encoded element is equivalent to the original raw element.
	//
	// As a special case, the set containing only the empty string is represented
	// by the string containing only the separator byte.
	//
	// The representation of a particular set of elements is not guaranteed to
	// remain stable over time. This internal representation must not be stored or
	// transmitted outside of the process that created it.
	joined string
}

// SetOf returns a new [Set] containing the provided elements.
func SetOf(elems ...string) (s Set) {
	s.Add(elems...)
	return
}

// Add adds the provided elements to s if it does not already contain them. In
// other words, it makes s the union of the elements already in s and the
// elements provided.
func (s *Set) Add(elems ...string) {
	all := append(s.ToSlice(), elems...)
	slices.Sort(all)
	all = slices.Compact(all)
	if len(all) == 1 && all[0] == "" {
		s.joined = unitSeparator
	} else {
		encodeAll(all)
		s.joined = strings.Join(all, unitSeparator)
	}
}

// Cardinality returns the cardinality of s; that is, the number of elements it
// contains. It is more efficient than computing the length of the slice
// returned by ToSlice.
func (s Set) Cardinality() int {
	switch s.joined {
	case "":
		return 0
	case unitSeparator:
		return 1
	default:
		return 1 + strings.Count(s.joined, unitSeparator)
	}
}

// ToSlice returns a sorted slice of the elements in s.
func (s Set) ToSlice() []string {
	switch s.joined {
	case "":
		return nil
	case unitSeparator:
		return []string{""}
	default:
		all := strings.Split(s.joined, unitSeparator)
		decodeAll(all)
		return all
	}
}

func (s Set) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.ToSlice())
}

func (s *Set) UnmarshalJSON(b []byte) error {
	var elems []string
	if err := json.Unmarshal(b, &elems); err != nil {
		return err
	}
	var newset Set
	newset.Add(elems...)
	if newset.Cardinality() < len(elems) {
		return errors.New("cannot unmarshal duplicate elements in a set")
	}
	*s = newset
	return nil
}

func encodeAll(elems []string) {
	for i, elem := range elems {
		// Note that the per-element encoding is defined only in terms of the final
		// encoded form, so we can be flexible about when we choose to encode. For
		// now, we only encode in the two cases where the properties of the encoding
		// absolutely require it: when the raw element contains a Unit Separator
		// (which conflicts with the higher-level Set representation) or starts with
		// a Shift Out (which conflicts with the Ascii85 marker in the encoding).
		if strings.Contains(elem, unitSeparator) || strings.HasPrefix(elem, shiftOut) {
			elems[i] = encodeAscii85Element(elem)
		}
	}
}

func encodeAscii85Element(elem string) string {
	out := make([]byte, 1+ascii85.MaxEncodedLen(len(elem)))
	out[0] = shiftOut[0]
	outlen := ascii85.Encode(out[1:], []byte(elem))
	return string(out[:1+outlen])
}

func decodeAll(elems []string) {
	for i, elem := range elems {
		if strings.HasPrefix(elem, shiftOut) {
			elems[i] = decodeAscii85Element(elem)
		}
	}
}

func decodeAscii85Element(elem string) string {
	var builder strings.Builder
	encoded := elem[1:] // Strip off the Shift Out byte used to mark the Ascii85 encoding.
	_, err := io.Copy(&builder, ascii85.NewDecoder(strings.NewReader(encoded)))
	if err != nil {
		panic(fmt.Errorf("invalid stringkeyed.Set encoding: %q: %v", elem, err))
	}
	return builder.String()
}
