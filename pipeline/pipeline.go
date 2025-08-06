package pipeline

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

var (
	once   sync.Once
	Pipe   *pipeLine
	Logger *log.Logger
)

const (
	QUEVE_SIZE = 32
	WORKERS    = 8
)

type pipeLine struct {
	RowBlocks        map[string]*block
	UncountedBlocks  map[string]*block
	CalculatedBlocks map[string]*block
	ErrorsBlocks     map[string]*block
	Functions        map[string]FuncInSute
	Input            chan []byte
	Output           chan any
	pipeWorker       chan *block
	Mu               sync.RWMutex
	Wg               sync.WaitGroup
}

func (p *pipeLine) String() string {
	var buffer strings.Builder

	buffer.WriteString(fmt.Sprintf("calculated: %v \n", p.CalculatedBlocks))
	for _, v := range p.CalculatedBlocks {
		buffer.WriteString(fmt.Sprintf("%v : %v\n", v.Name, v.GetResult()))
	}
	buffer.WriteString(fmt.Sprintf("uncalculated:%v \n", p.UncountedBlocks))
	buffer.WriteString(fmt.Sprintf("errors: %v \n", p.ErrorsBlocks))

	for _, v := range p.ErrorsBlocks {
		buffer.WriteString(fmt.Sprintf("name: %v error: %v\n", v.Name, v.Error))
	}
	return buffer.String()
}
func GetPipeline() *pipeLine {
	once.Do(func() {
		Pipe = new(pipeLine)
		Pipe.Input = make(chan []byte, QUEVE_SIZE)
		Pipe.Output = make(chan any)
		Pipe.UncountedBlocks = make(map[string]*block, QUEVE_SIZE)
		Pipe.CalculatedBlocks = make(map[string]*block, QUEVE_SIZE)
		Pipe.ErrorsBlocks = make(map[string]*block, QUEVE_SIZE)
		Pipe.RowBlocks = make(map[string]*block, 8)
		Pipe.Wg = sync.WaitGroup{}
		Pipe.pipeWorker = make(chan *block)
	})

	return Pipe
}

func (p *pipeLine) AddBlock(Block *block) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	p.RowBlocks[Block.Name] = Block
	p.Wg.Add(1)

}

func (p *pipeLine) MoveRowBlock(Block *block) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	if _, ok := p.RowBlocks[Block.Name]; ok {
		delete(p.RowBlocks, Block.Name)
		p.UncountedBlocks[Block.Name] = Block
		Logger.Printf("[MoveRowBlock] %s values %v", Block.Name, Block.Input)
	}

}

func (p *pipeLine) MoveCalculatedBlock(Block *block) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	if _, ok := p.UncountedBlocks[Block.Name]; ok {
		delete(p.UncountedBlocks, Block.Name)
		p.CalculatedBlocks[Block.Name] = Block
		p.Wg.Done()
		Logger.Printf("[MoveCalculatedBlock] %s values %v", Block.Name, Block.Input)
		p.Output <- fmt.Sprintln("block:", Block.Name, " calculated")

	}

}

func (p *pipeLine) MoveCalculatedErrorBlock(Block *block) {
	p.Mu.Lock()
	defer p.Mu.Unlock()
	if _, ok := p.UncountedBlocks[Block.Name]; ok {
		delete(p.UncountedBlocks, Block.Name)
		p.ErrorsBlocks[Block.Name] = Block
		p.Wg.Done()
		Logger.Printf("[MoveCalculatedErrorBlock] %s values %v", Block.Name, Block.Input)
		p.Output <- fmt.Sprintln("block:", Block.Name, " got an error")
	}

}

func (p *pipeLine) SetFunctions(functions map[string]FuncInSute) {
	p.Functions = functions
}

func (p *pipeLine) CalculateBlocks() {
	for {
		p.Mu.RLock()
		blocks := make([]*block, 0, len(p.RowBlocks))
		for _, b := range p.RowBlocks {
			blocks = append(blocks, b)
		}
		p.Mu.RUnlock()

		if len(blocks) == 0 {
			time.Sleep(time.Millisecond)
			continue
		}
		var mutex sync.Mutex
		for _, block := range blocks {

			mutex.Lock()
			Logger.Printf("[START_BLOCK_CALCULATE] %s values %v", block.Name, block.Input)
			if !block.InProcess {
				p.pipeWorker <- block
			}
			mutex.Unlock()
		}
	}
}

func (p *pipeLine) Run() {

	for i := 0; i <= WORKERS; i++ {
		go initWorker(p.pipeWorker)
	}

	go func() {
		for p := range p.Input {
			CreateBlocks(p)
		}
	}()

	go p.CalculateBlocks()
	p.Wg.Wait()

}

func initWorker(ch chan *block) {
	for block := range ch {
		block.InProcess = true
		block.Calculate()
	}
}

func (p *pipeLine) GetCalculatedResult(key string) (any, bool) {
	p.Mu.RLock()
	defer p.Mu.RUnlock()
	v, ok := p.CalculatedBlocks[key]
	if ok {
		return v.calculatedResult, ok
	}
	return "", false
}
