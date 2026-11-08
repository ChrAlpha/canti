package hostrpc

import (
	"errors"
	"reflect"
	"unicode/utf8"
)

// CheckText refuses strings JSON would silently replace with U+FFFD. This is
// transport validity only: runtime still owns identities, limits and admission.
// Reflection follows this concrete request schema so new string fields inherit
// the same guarantee; it never formats or returns field contents (including keys).
func (r Request) CheckText() error {
	if !validText(reflect.ValueOf(r)) {
		return errors.New("hostrpc: request strings must be valid UTF-8")
	}
	return nil
}

func validText(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return utf8.ValidString(v.String())
	case reflect.Pointer:
		return v.IsNil() || validText(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !validText(v.Field(i)) {
				return false
			}
		}
	}
	return true
}
