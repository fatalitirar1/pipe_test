package pipeline

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type FuncInSute struct {
	Fn            any
	Arg_sequance  []string
	Type_sequance []string
}

func GetFuncInSute(fun any, args []string, types []string) (*FuncInSute, error) {

	if len(args) != len(types) {
		return nil, errors.New("bad sequance of arguments and types")
	}
	fis := new(FuncInSute)
	fis.Fn = fun
	fis.Arg_sequance = args
	fis.Type_sequance = types

	return fis, nil
}

func (fin FuncInSute) Execute(arg map[string]any) (any, error) {

	function := reflect.ValueOf(fin.Fn)
	var fakeVariables []reflect.Value

	switch {
	case len(fin.Arg_sequance) == 1 &&
		strings.Contains(fin.Arg_sequance[0], "..."):

		fakeVariables = make([]reflect.Value, len(arg))
		index := 0
		for _, v := range arg {
			fakeVariables[index] = reflect.ValueOf(v)
			index++
		}

	default:

		fakeVariables = fin.GenerateArguments()
		for k, v := range fin.Arg_sequance {

			fakeVariables[k] = reflect.ValueOf(arg[v])

		}
	}

	result := function.Call(fakeVariables)
	if len(result) == 0 {
		return "", fmt.Errorf("no result in fuction")
	}

	reflected_result := result[0]

	switch reflected_result.Kind() {
	case reflect.Float64, reflect.Float32:
		return reflected_result.Float(), nil
	case reflect.String:
		return reflected_result.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflected_result.Int(), nil
	case reflect.Ptr, reflect.Interface:
		if reflected_result.IsNil() {
			return nil, nil
		}
		return reflected_result.Elem().Interface(), nil
	default:
		return reflected_result.Interface(), nil
	}

}

func (fis *FuncInSute) GenerateArguments() []reflect.Value {

	values := make([]reflect.Value, len(fis.Arg_sequance))
	for i, v := range fis.Type_sequance {
		var r_v any
		switch v {
		case "float64":
			r_v = 0
		default:
			r_v = ""
		}

		values[i] = reflect.ValueOf(r_v)
	}
	return values

}
