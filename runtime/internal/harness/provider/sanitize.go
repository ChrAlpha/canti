package provider

import "reflect"

// SanitizeResult applies RedactValue to every string reachable from a model
// result, RedactJSON to byte bodies, and the error sanitizer to error fields.
// It is the final boundary before the harness sees an adapter result.
func SanitizeResult(secret string, result any) {
	if secret == "" || result == nil {
		return
	}
	visitResult(reflect.ValueOf(result), secret, map[uintptr]bool{})
}

func visitResult(value reflect.Value, secret string, seen map[uintptr]bool) {
	if !value.IsValid() {
		return
	}
	switch value.Kind() {
	case reflect.Interface:
		visitInterface(value, secret, seen)
	case reflect.Pointer:
		visitPointer(value, secret, seen)
	case reflect.Struct:
		visitStruct(value, secret, seen)
	case reflect.String:
		visitString(value, secret)
	case reflect.Slice:
		visitSlice(value, secret, seen)
	case reflect.Map:
		visitMap(value, secret, seen)
	}
}

func visitInterface(value reflect.Value, secret string, seen map[uintptr]bool) {
	if value.IsNil() {
		return
	}
	if value.CanInterface() {
		if err, ok := value.Interface().(error); ok && value.CanSet() {
			value.Set(reflect.ValueOf(sanitizeError(secret, err)))
			return
		}
	}
	visitResult(value.Elem(), secret, seen)
}

func visitPointer(value reflect.Value, secret string, seen map[uintptr]bool) {
	if value.IsNil() {
		return
	}
	if value.CanInterface() {
		if err, ok := value.Interface().(error); ok && value.CanSet() {
			value.Set(reflect.ValueOf(sanitizeError(secret, err)))
			return
		}
	}
	ptr := value.Pointer()
	if ptr != 0 && seen[ptr] {
		return
	}
	seen[ptr] = true
	visitResult(value.Elem(), secret, seen)
}

func visitStruct(value reflect.Value, secret string, seen map[uintptr]bool) {
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if field.CanSet() {
			visitResult(field, secret, seen)
		}
	}
}

func visitString(value reflect.Value, secret string) {
	if value.CanSet() {
		value.SetString(RedactValue(secret, value.String()))
	}
}

func visitSlice(value reflect.Value, secret string, seen map[uintptr]bool) {
	if value.Type().Elem().Kind() == reflect.Uint8 {
		if value.CanSet() {
			value.SetBytes(RedactJSON(secret, value.Bytes()))
		}
		return
	}
	for i := 0; i < value.Len(); i++ {
		visitResult(value.Index(i), secret, seen)
	}
}

func visitMap(value reflect.Value, secret string, seen map[uintptr]bool) {
	if value.IsNil() {
		return
	}
	for _, key := range value.MapKeys() {
		if item := value.MapIndex(key); item.IsValid() {
			visitResult(item, secret, seen)
		}
	}
}
