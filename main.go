package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"pip/pipeline"
	"strings"
	"syscall"
	"time"
)

func add(a float64, b float64) float64 {
	return a + b
}

func multiply(a float64, b float64) float64 {
	return a * b
}
func concatenate(arg ...string) string {
	var buffer strings.Builder
	for _, v := range arg {
		buffer.WriteString(v)
	}

	return buffer.String()
}

func conditional(a float64, b float64) float64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func main() {
	logFile, _ := os.OpenFile("./log.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	pipeline.Logger = log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)
	pipeline.Logger.Println("[START] New init")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	addf, _ := pipeline.GetFuncInSute(add, []string{"a", "b"}, []string{"float64", "float64"})
	multiplyf, _ := pipeline.GetFuncInSute(multiply, []string{"a", "b"}, []string{"float64", "float64"})
	concatenatef, _ := pipeline.GetFuncInSute(concatenate, []string{"...arg"}, []string{"string"})
	conditionalf, _ := pipeline.GetFuncInSute(conditional, []string{"a", "b"}, []string{"float64", "float64"})

	functions := map[string]pipeline.FuncInSute{
		"add":         *addf,
		"multiply":    *multiplyf,
		"concatenate": *concatenatef,
		"conditional": *conditionalf,
	}

	data, _ := os.ReadFile("json_req_gen.json")
	pipe := pipeline.GetPipeline()
	pipe.SetFunctions(functions)

	pipe.Run()
	pipe.Input <- data

	close(pipe.Input)
	go func() {
		<-sigs
		fmt.Println()
		fmt.Println(pipe.UncountedBlocks)
		fmt.Println(pipe.RowBlocks)
		os.Exit(1)

	}()
	for r := range pipe.Output {
		time.Sleep(time.Microsecond)
		fmt.Print(r)
		if len(pipe.RowBlocks) == 0 &&
			len(pipe.UncountedBlocks) == 0 {
			close(pipe.Output)
		}

	}

	fmt.Println(pipe)

}
