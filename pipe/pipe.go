package pipe

//Pipe for handling movement through pipeline. 3 use cases
//1. Source has only an out because it just sends messages
//2. Sink has only an in because it just receieves messages
//3. Transformer will have an in, and an out.
//Not sure about the types right now. 
type Pipe struct {
	In  chan interface{}
	Out chan interface{}
}

//NewPipe creates a new Pipe 
func NewPipe(stage string) *Pipe {
	p := new(Pipe)

	switch {
	case stage == "source" || stage == "transformer":
		p.In = make(chan interface{}, 1)
	case stage == "sink" || stage == "transformer":
		p.Out = make(chan interface{}, 1)
	}
	
	return p
}