package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type opType int

const (
	PUT opType = iota + 1
	DELETE
	JOIN
	RETIRE
)

type Data struct {
	Op      opType
	Id      int
	Key     string
	Value   string
	Stable  bool
	CSN     int
	AccTime int
	Replica string
	Rid     int
}

type Sret struct {
	R  string
	VV []int
}

type Server struct {
	clientId int
	serverId int
	maxNum   int
	log      map[string][]int
}

func newServer(n int, m int) *Server {
	s := new(Server)
	s.serverId = m
	s.clientId = n
	s.maxNum = 100
	s.log = make(map[string][]int)
	return s
}

func (t *Server) Break(s *int, r *int) error {
	if *s == t.serverId {
		t.serverId = -1
	}
	return nil
}

func (t *Server) Restore(s *int, r *int) error {
	t.serverId = *s
	return nil
}

func (t *Server) Put(s *Data, r *string) error {
	n := t.serverId
	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(8800+n))
	if err != nil {
		// error will never happen based on the assumption
	}
	defer client.Close()
	ret := new(Sret)
	ret.VV = make([]int, t.maxNum)
	err = client.Call("Server.Sput", s, &ret)
	if _, ok := t.log[s.Key]; !ok {
		t.log[s.Key] = make([]int, t.maxNum)
	}
	for i := 0; i < len(t.log[s.Key]); i++ {
		if t.log[s.Key][i] < ret.VV[i] {
			t.log[s.Key][i] = ret.VV[i]
		}
	}
	return err
}

func (t *Server) Get(s *string, r *string) error {
	n := t.serverId
	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(8800+n))
	if err != nil {
		// error will never happen based on the assumption
	}
	defer client.Close()
	ret := new(Sret)
	ret.VV = make([]int, t.maxNum)
	err = client.Call("Server.Sget", s, &ret)
	if ret.R == "" {
		ret.R = "ERR_KEY"
	}
	if _, ok := t.log[*s]; !ok {
		t.log[*s] = make([]int, t.maxNum)
	}
	for i := 0; i < len(ret.VV); i++ {
		if ret.VV[i] < t.log[*s][i] {
			ret.R = "ERR_DEP"
			break
		}
	}
	*r = ret.R
	for i := 0; i < len(t.log[*s]); i++ {
		if t.log[*s][i] < ret.VV[i] {
			t.log[*s][i] = ret.VV[i]
		}
	}
	return err
}

func (t *Server) Delete(s *string, r *string) error {
	n := t.serverId
	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(8800+n))
	if err != nil {
		// error will never happen based on the assumption
	}
	defer client.Close()
	ret := new(Sret)
	ret.VV = make([]int, t.maxNum)
	err = client.Call("Server.Sdelete", s, &ret)
	if _, ok := t.log[*s]; !ok {
		t.log[*s] = make([]int, t.maxNum)
	}
	for i := 0; i < len(t.log[*s]); i++ {
		if t.log[*s][i] < ret.VV[i] {
			t.log[*s][i] = ret.VV[i]
		}
	}
	return err
}

func main() {
	n, _ := strconv.Atoi(os.Args[1])
	m, _ := strconv.Atoi(os.Args[2])
	port := strconv.Itoa(n + 8800)

	server := newServer(n, m)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln("listen error:", err)
	}

	rpc.Register(server)
	rpc.HandleHTTP()
	http.Serve(listener, nil)
}
