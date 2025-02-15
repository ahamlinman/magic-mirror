package stringkeyed

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
)

const (
	shiftOut      = "\x0e"
	unitSeparator = "\x1f"
)

type assertComparable[T comparable] struct{}

var _ assertComparable[Set]

// Set is a set of strings that supports comparison with == and != by foregoing
// O(1) insertion and membership tests. The zero value is the empty set.
//
// This implementation of Set may handle elements containing the byte values of
// ASCII control characters less efficiently than elements without such bytes.
type Set struct {
	// The internal representation of a Set is formed by sorting its raw elements,
	// encoding each one, and concatenating them with the byte 0x1F (the ASCII
	// Unit Separator character) as a separator.
	//
	// The per-element encoding has two forms. If the encoded element begins with
	// the byte 0x0E (the ASCII Shift Out character), the remaining bytes of the
	// encoded element are the base64 encoding of the original raw element.
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

// Add turns s into the union of s and the provided elements.
//
// In the worst case, the time complexity of Add is O(n log n) in the size of
// the combined set. Typical implementations of Go can Add a single element in
// O(n) time.
func (s *Set) Add(elems ...string) {
	all := make([]string, 0, s.Cardinality()+len(elems))
	all = slices.AppendSeq(all, s.All())
	all = append(all, elems...)
	slices.Sort(all)
	all = slices.Compact(all)
	if len(all) == 1 && all[0] == "" {
		s.joined = unitSeparator
		return
	}
	for i, elem := range all {
		all[i] = encode(elem)
	}
	s.joined = strings.Join(all, unitSeparator)
}

// Cardinality returns the number of elements in s in O(n) time. It is more
// efficient than computing the length of the slice returned by ToSlice.
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

// All returns an iterator over the sorted elements in s.
func (s Set) All() iter.Seq[string] {
	return func(yield func(string) bool) {
		switch s.joined {
		case "":
			return
		case unitSeparator:
			yield("")
			return
		}
		for elem := range strings.SplitSeq(s.joined, unitSeparator) {
			if !yield(decode(elem)) {
				return
			}
		}
	}
}

// ToSlice returns a sorted slice of the elements in s.
func (s Set) ToSlice() []string {
	if card := s.Cardinality(); card > 0 {
		return slices.AppendSeq(make([]string, 0, card), s.All())
	}
	return nil
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

func encode(elem string) string {
	// The per-element encoding is defined in terms of the final encoded form,
	// so we can be flexible about when we apply base64. For now, we do so in
	// the two cases that absolutely require it: when the raw element contains
	// a Unit Separator (confusable with the element separator) or starts with
	// a Shift Out (confusable with the base64 marker).
	if strings.Contains(elem, unitSeparator) || strings.HasPrefix(elem, shiftOut) {
		return encodeBase64Element(elem)
	}
	return elem
}

func encodeBase64Element(elem string) string {
	// Note that AppendEncode grows the slice once based on EncodeLen;
	// no further work is required of us to make this allocation efficient.
	return string(base64.StdEncoding.AppendEncode([]byte(shiftOut), []byte(elem)))
}

func decode(elem string) string {
	if strings.HasPrefix(elem, shiftOut) {
		return decodeBase64Element(elem)
	}
	return elem
}

func decodeBase64Element(elem string) string {
	out, err := base64.StdEncoding.DecodeString(elem[1:]) // Strip the Shift Out byte.
	if err != nil {
		panic(fmt.Errorf("invalid stringkeyed.Set encoding: %q: %v", elem, err))
	}
	return string(out)
}
