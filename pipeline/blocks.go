package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sync"
)

type block struct {
	Name   string         `json:"name"`
	Fn     string         `json:"function"`
	Input  map[string]any `json:"input"`
	Output []string       `json:"Output"`

	Error            error
	calculatedResult any
	mu               sync.Mutex
	InProcess        bool
}

func CreateBlocks(data []byte) {
	elems := make([]*block, 4)
	err := json.Unmarshal(data, &elems)
	if err != nil {
		panic(err)
	}

	for _, bl := range elems {
		Pipe.AddBlock(bl)
	}
}

func itsBlockName(name string) bool {
	Logger.Printf("[BLOCK_WANT_ANATHER_BLOCK_name_var] %s", name)
	matched, _ := regexp.MatchString(`^block\d+$`, name)
	return matched
}

func (b *block) Calculate() {
	Pipe.MoveRowBlock(b)
	b.mu.Lock()
	defer b.mu.Unlock()
	defer func() {
		if err := recover(); err != nil {
			b.Error = fmt.Errorf("error in block: %s : \n %s", b.Name, err)
			Pipe.MoveCalculatedErrorBlock(b)
		}
	}()

	v, ok := Pipe.Functions[b.Fn]

	if ok {

		for k, value := range b.Input {
			r := reflect.ValueOf(value)
			if r.Kind() == reflect.String &&
				itsBlockName(r.String()) {

				Logger.Printf("[BLOCK_WANT_ANATHER_BLOCK] %s", b.Name)
				val, found := Pipe.GetCalculatedResult(r.String())

				if found {
					b.Input[k] = val
				} else {

					Pipe.pipeWorker <- b
					Logger.Printf("[RETURN_BLOCK_TO_POOL] %s values %v", b.Name, b.Input)
					return
				}
			}
		}
		Logger.Printf("[EXECUTE_BLOCK] %s values %v", b.Name, b.Input)
		v, err := v.Execute(b.Input)

		if err != nil {
			b.Error = fmt.Errorf("error in block: %s : \n %s", b.Name, err)
			Pipe.MoveCalculatedErrorBlock(b)
			return
		}
		b.calculatedResult = v
		Pipe.MoveCalculatedBlock(b)
	} else {
		b.Error = fmt.Errorf("error in block: %s : \n function bad name %s", b.Name, b.Fn)
		Pipe.MoveCalculatedErrorBlock(b)
	}

}

func (b *block) GetResult() any {
	return b.calculatedResult
}
