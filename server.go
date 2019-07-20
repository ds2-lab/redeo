package redeo

import (
	"fmt"
	"github.com/cornelk/hashmap"
	"github.com/wangaoone/redeo/resp"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	LambdaMem int = 3000
)

var metaMap = hashmap.New(1024)

// Server configuration
type Server struct {
	config *Config
	info   *ServerInfo

	cmds map[string]interface{}
	mu   sync.RWMutex
}

type objKey struct {
	key     []byte
	chunkId int64
}

type Chunk struct {
	Id   int
	Body []byte
}

type Index struct {
	ClientId int
	ReqId    int
	Counter  int
}

type Id struct {
	ClientId int
	ReqId    int
	ChunkId  int
}

type Req struct {
	Id  Id
	Cmd string
	Key []byte
	Val []byte
}

type Response struct {
	Id Id
	//ChunkId int64
	Key  string
	Body []byte
}

type Group struct {
	Arr        []LambdaInstance
	MemCounter uint64
}

type LambdaInstance struct {
	Name      string
	Id        int
	Alive     bool
	Cn        net.Conn
	W         *resp.RequestWriter
	R         resp.ResponseReader
	C         chan Req
	AliveLock sync.Mutex
	Counter   uint64
}

//func (group Group) IncrementCounter() {
//	group.ChunkCounter += 1
//}

// NewServer creates a new server instance
func NewServer(config *Config) *Server {
	if config == nil {
		config = new(Config)
	}

	return &Server{
		config: config,
		info:   newServerInfo(),
		cmds:   make(map[string]interface{}),
	}
}

// Info returns the server info registry
func (srv *Server) Info() *ServerInfo { return srv.info }

// Handle registers a handler for a command.
func (srv *Server) Handle(name string, h Handler) {
	srv.mu.Lock()
	srv.cmds[strings.ToLower(name)] = h
	srv.mu.Unlock()
}

// HandleFunc registers a handler func for a command.
func (srv *Server) HandleFunc(name string, fn HandlerFunc) {
	srv.Handle(name, fn)
}

// HandleStream registers a handler for a streaming command.
func (srv *Server) HandleStream(name string, h StreamHandler) {
	srv.mu.Lock()
	srv.cmds[strings.ToLower(name)] = h
	srv.mu.Unlock()
}

// HandleStreamFunc registers a handler func for a command
func (srv *Server) HandleStreamFunc(name string, fn StreamHandlerFunc) {
	srv.HandleStream(name, fn)
}

// Serve accepts incoming connections on a listener, creating a
// new service goroutine for each.
func (srv *Server) Serve(lis net.Listener) error {
	for {
		cn, err := lis.Accept()
		if err != nil {
			return err
		}
		fmt.Println("Accept", cn.RemoteAddr())

		if ka := srv.config.TCPKeepAlive; ka > 0 {
			if tc, ok := cn.(*net.TCPConn); ok {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(ka)
			}
		}

		go srv.serveClient(newClient(cn))
	}
}

func (srv *Server) serveClient(c *Client) {
	// Release client on exit
	defer c.release()

	// Register client
	srv.info.register(c)
	defer srv.info.deregister(c.id)

	// Create perform callback
	perform := func(name string) error {
		return srv.perform(c, name)
	}
	// Init request/response loop
	for !c.closed {
		// set deadline
		if d := srv.config.Timeout; d > 0 {
			c.cn.SetDeadline(time.Now().Add(d))
		}

		// perform pipeline
		if err := c.pipeline(perform); err != nil {
			c.wr.AppendError("ERR " + err.Error())

			if !resp.IsProtocolError(err) {
				_ = c.wr.Flush()
				return
			}
		}

		// flush buffer, return on errors
		if err := c.wr.Flush(); err != nil {
			return
		}
	}
}

func (srv *Server) perform(c *Client, name string) (err error) {
	norm := strings.ToLower(name)

	// find handler
	srv.mu.RLock()
	h, ok := srv.cmds[norm]
	srv.mu.RUnlock()

	if !ok {
		c.wr.AppendError(UnknownCommand(name))
		_ = c.rd.SkipCmd()
		return
	}

	// register call
	srv.info.command(c.id, norm)

	switch handler := h.(type) {
	case Handler:
		if c.cmd, err = c.readCmd(c.cmd); err != nil {
			return
		}
		handler.ServeRedeo(c.wr, c.cmd)

	case StreamHandler:
		if c.scmd, err = c.streamCmd(c.scmd); err != nil {
			return
		}
		defer c.scmd.Discard()

		handler.ServeRedeoStream(c.wr, c.scmd)
	}

	// flush when buffer is large enough
	if n := c.wr.Buffered(); n > resp.MaxBufferSize/2 {
		err = c.wr.Flush()
	}
	return
}

// new serve with channel initial，creating a
// new service goroutine for each.
func (srv *Server) MyServe(lis net.Listener, cMap map[int]chan interface{}, mappingTable *hashmap.HashMap) error {
	// start counter to record client id, initial with 0
	id := 0
	for {
		cn, err := lis.Accept()
		if err != nil {
			return err
		}
		fmt.Println("Accept", cn.RemoteAddr())

		if ka := srv.config.TCPKeepAlive; ka > 0 {
			if tc, ok := cn.(*net.TCPConn); ok {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(ka)
			}
		}

		// make channel for every new client
		c := make(chan interface{}, 1024*1024)
		// store the new client channel to the channel map
		cMap[id] = c
		go srv.myServeClient(newClient(cn), c, id, mappingTable)
		// id increment by 1
		id = id + 1
	}
}

// client handler
func (srv *Server) myServeClient(c *Client, clientChannel chan interface{}, clientId int, mappingTable *hashmap.HashMap) {
	// make helper channel for every client

	// Release client on exit
	defer c.release()
	// close client and helper channel
	//defer close(helper)
	//defer close(clientChannel)

	// Register client
	srv.info.register(c)
	defer srv.info.deregister(c.id)

	helper := make(chan string, 1024*1024)
	// Create perform callback
	perform := func(name string) error {
		return srv.myPerform(c, name)
	}
	// go routine peeking cmd
	go myPeekCmd(c, perform, helper)

	//reqId := 0
	// Init request/response loop
	for !c.closed {
		// set deadline
		if d := srv.config.Timeout; d > 0 {
			c.cn.SetDeadline(time.Now().Add(d))
		}
		group, ok := mappingTable.Get(0)
		if ok == false {
			fmt.Println("get lambda instance failed")
		}
		select {
		//
		/* blocking on helper channel while peeking cmd*/
		//
		case cmd := <-helper:
			// receive request from client
			key := c.cmd.Arg(0)
			chunkId, _ := c.cmd.Arg(2).Int()
			lambdaId, _ := c.cmd.Arg(3).Int()
			val := c.cmd.Arg(4)
			if val != nil { /* val != nil, SET handler */
				// check if the key is existed
				_, ok := metaMap.Get(key)
				if ok == false {
					fmt.Println("clientId is", clientId, "chunkId is", chunkId, "lambdaStore Id is", lambdaId)
					// send shard to the corresponding lambda instance in group
					newReq := Req{Id{ClientId: clientId, ChunkId: int(chunkId)}, cmd, key, val}
					// send new request to lambda channel
					group.(*Group).Arr[lambdaId].C <- newReq
					metaMap.Set(objKey{key: key, chunkId: chunkId}, lambdaId)
				} else {
					// update the existed key
				}
			} else { /* val == nil, GET handler */
				lambdaDestiny, ok := metaMap.Get(objKey{key: key, chunkId: chunkId})
				if ok {
					fmt.Println("not found key in lambda store, please set first")
				}
				newReq := Req{Id{ClientId: clientId}, cmd, key, nil}
				// send new request to lambda channel
				group.(*Group).Arr[lambdaDestiny.(int64)].C <- newReq
			}
			//reqId += 1
		//
		/* blocking on receive final result from lambda store*/
		//
		case result := <-clientChannel:
			temp := result.(Chunk)
			fmt.Println("chunk body len is ", len(temp.Body))
			t0 := time.Now()
			c.wr.AppendInt(int64(temp.Id))
			fmt.Println("server append Int time is", time.Since(t0))
			t1 := time.Now()
			c.wr.AppendBulk(temp.Body[0:len(temp.Body)])
			fmt.Println("server append Bulk time is", time.Since(t1))
			t2 := time.Now()
			// flush buffer, return on errors
			if err := c.wr.MyFlush(); err != nil {
				return
			}
			fmt.Println("server flush to client time is", time.Since(t2))
		}

		// flush buffer, return on errors
		if err := c.wr.Flush(); err != nil {
			return
		}
	}
}

// client peeking Cmd
func myPeekCmd(c *Client, fn func(string) error, channel chan string) {
	for {
		if err := c.peekCmd(fn, channel); err != nil {
			c.wr.AppendError("ERR " + err.Error())
			if !resp.IsProtocolError(err) {
				_ = c.wr.Flush()
				return
			}
		}
	}
}

func (srv *Server) myPerform(c *Client, name string) (err error) {
	norm := strings.ToLower(name)

	// register call
	srv.info.command(c.id, norm)

	if c.cmd, err = c.readCmd(c.cmd); err != nil {
		return
	}
	// flush when buffer is large enough
	if n := c.wr.Buffered(); n > resp.MaxBufferSize/2 {
		err = c.wr.Flush()
	}
	return
}

/*
 * Lambda store part
 */

// Accept conn
func (srv *Server) Accept(lis net.Listener) net.Conn {
	cn, err := lis.Accept()
	if err != nil {
		fmt.Println(err)
	}
	return cn
}

// Lambda facing serve client
func (srv *Server) Serve_client(cn net.Conn) {
	//fmt.Println("in the lambda server")
	srv.serveClient(newClient(cn))
}

func RandomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
	}
	return string(bytes)
}
