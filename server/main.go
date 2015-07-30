package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
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

type Log struct {
	Stable   []*Data
	Unstable []*Data
	VV       []int
}

func newLog() Log {
	ret := Log{}
	ret.Stable = make([]*Data, 0)
	ret.Unstable = make([]*Data, 0)
	return ret
}

type Server struct {
	serverId   int
	primary    bool
	maxNum     int
	breakList  map[int]bool
	serverList map[int]bool
	serverName map[string]int
	stable     []*Data
	unstable   []*Data
	database   map[string]string
	accTime    int
	name       string
	pause      bool
	mutex      *sync.Mutex
	update     bool
	vv         []int
}

func newServer(n int) *Server {
	s := new(Server)
	s.serverId = n
	s.primary = false
	if n == 0 {
		s.primary = true
	}
	s.maxNum = 100
	s.breakList = make(map[int]bool)
	s.serverList = make(map[int]bool)
	s.serverName = make(map[string]int)
	s.database = make(map[string]string)
	s.stable = make([]*Data, 0)
	s.unstable = make([]*Data, 0)
	s.accTime = 1
	s.pause = false
	s.mutex = &sync.Mutex{}
	s.update = false
	s.vv = make([]int, s.maxNum)

	for i := 0; i < len(s.vv); i++ {
		s.vv[i] = 0
	}

	for i := 0; i < s.maxNum; i++ {
		s.breakList[i] = false
		s.serverList[i] = false
	}
	return s
}

func (t *Server) Break(s *int, r *int) error {
	t.breakList[*s] = true
	return nil
}

func (t *Server) BePrimary(s *int, r *int) error {
	t.primary = true
	return nil
}

func (t *Server) Restore(s *int, r *int) error {
	t.update = true
	t.breakList[*s] = false
	return nil
}

func (t *Server) Retire(s *int, r *int) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	l := &Data{Op: RETIRE, Id: t.accTime*t.maxNum + t.serverId,
		Stable: t.primary, AccTime: t.accTime, Replica: t.name, Rid: *s}
	if t.primary {
		l.CSN = len(t.stable) + 1
		t.vv[t.serverId]++
		t.stable = append(t.stable, l)
	} else {
		t.vv[t.serverId]++
		t.unstable = append(t.unstable, l)
	}

	for i := 0; i < t.maxNum; i++ {
		if i == t.serverId {
			continue
		}
		if (!t.breakList[i]) && t.serverList[i] {
			// sync with replica i
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+i))
			if err != nil {
				continue
			}
			tmp := Log{Stable: t.stable,
				Unstable: t.unstable}
			tmp.VV = make([]int, t.maxNum)
			copy(tmp.VV, t.vv)
			r := newLog()
			r.VV = make([]int, t.maxNum)
			tt := t.serverId
			if t.primary {
				err = client.Call("Server.BePrimary", &tt, nil)
				t.primary = false
			}
			err = client.Call("Server.Sync", &tmp, &r)
			if err != nil {
				client.Close()
				continue
			}
			if len(t.stable) < len(r.Stable) ||
				len(t.unstable) != len(r.Unstable) {
				t.update = true
				t.stable = r.Stable
				t.unstable = r.Unstable
				r.VV = make([]int, t.maxNum)
				copy(t.vv, r.VV)
				// rebuild
				t.rebuild()
			}
			client.Close()
			break
		}
	}

	return nil
}

func (t *Server) Pause(s *int, r *int) error {
	t.pause = true
	return nil
}

func (t *Server) Start(s *int, r *int) error {
	t.pause = false
	return nil
}

func (t *Server) Stabilize(s *int, r *int) error {
	for {
		if t.update == false {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (t *Server) PrintLog(s *int, r *string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for _, k := range t.stable {
		var ot string
		var ov string
		if k.Op == PUT {
			ot = "PUT"
			ov = k.Key + ", " + k.Value
		} else if k.Op == DELETE {
			ot = "DELETE"
			ov = k.Key
		} else if k.Op == JOIN {
			ot = "JOIN"
			ov = fmt.Sprintf("%d, %s", k.AccTime, k.Replica)
		} else if k.Op == RETIRE {
			ot = "RETIRE"
			ov = fmt.Sprintf("%d, %s", k.AccTime, k.Replica)
		} else {
		}
		os := "FALSE"
		if k.Stable {
			os = "TRUE"
		}
		if k.Op == PUT || k.Op == DELETE {
			*r = fmt.Sprintf("%s%s:(%s):%s\n", *r, ot, ov, os)
		}
	}
	for _, k := range t.unstable {
		var ot string
		var ov string
		if k.Op == PUT {
			ot = "PUT"
			ov = k.Key + ", " + k.Value
		} else if k.Op == DELETE {
			ot = "PUT"
			ov = k.Key
		} else {
			continue
		}
		os := "FALSE"
		if k.Stable {
			os = "TRUE"
		}
		*r = fmt.Sprintf("%s%s:(%s):%s\n", *r, ot, ov, os)
	}
	return nil
}

func (t *Server) Sjoin(s *int, r *string) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	*r = fmt.Sprintf("%d, %s", t.accTime, t.name)

	l := &Data{Op: JOIN, Id: t.accTime*t.maxNum + t.serverId,
		Stable: t.primary, AccTime: t.accTime, Replica: t.name, Rid: *s}
	if t.primary {
		l.CSN = len(t.stable) + 1
		t.stable = append(t.stable, l)
	} else {
		t.unstable = append(t.unstable, l)
	}
	t.serverList[*s] = true

	t.accTime++
	t.vv[t.serverId]++
	t.update = true
	t.rebuild()
	return nil
}

func (t *Server) Sput(s *Data, r *Sret) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	l := &Data{Op: PUT, Key: s.Key, Value: s.Value, Stable: t.primary}
	l.AccTime = t.accTime
	t.accTime++
	t.vv[t.serverId]++
	l.Replica = t.name
	l.Id = l.AccTime*t.maxNum + t.serverId
	if t.primary {
		l.CSN = len(t.stable) + 1
		t.stable = append(t.stable, l)
		t.database[s.Key] = s.Value
	} else {
		t.unstable = append(t.unstable, l)
	}
	t.update = true
	r.VV = make([]int, t.maxNum)
	copy(r.VV, t.vv)
	t.rebuild()
	return nil
}

func (t *Server) Sget(s *string, r *Sret) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	a := t.database[*s]
	r.R = a
	r.VV = make([]int, t.maxNum)
	copy(r.VV, t.vv)
	return nil
}

func (t *Server) Sdelete(s *string, r *Sret) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	l := &Data{Op: DELETE, Key: *s, Value: "", Stable: t.primary}
	l.AccTime = t.accTime
	t.accTime++
	t.vv[t.serverId]++
	l.Replica = t.name
	l.Id = l.AccTime*t.maxNum + t.serverId
	if t.primary {
		t.stable = append(t.stable, l)
		delete(t.database, *s)
	} else {
		t.unstable = append(t.unstable, l)
	}
	t.update = true
	r.VV = make([]int, t.maxNum)
	copy(r.VV, t.vv)
	t.rebuild()
	return nil
}

func (t *Server) rebuild() {
	t.database = make(map[string]string)
	for i := 0; i < t.maxNum; i++ {
		t.serverList[i] = false
	}
	for _, k := range t.stable {
		if k.Op == PUT {
			t.database[k.Key] = k.Value
		} else if k.Op == DELETE {
			delete(t.database, k.Key)
		} else if k.Op == JOIN {
			t.serverList[k.Rid] = true
			t.serverName[k.Replica] = k.Rid
		} else if k.Op == RETIRE {
			t.serverList[k.Rid] = false
			delete(t.serverName, k.Replica)
		}
	}
	for _, k := range t.unstable {
		if k.Op == PUT {
			t.database[k.Key] = k.Value
		} else if k.Op == DELETE {
			delete(t.database, k.Key)
		} else if k.Op == JOIN {
			t.serverList[k.Rid] = true
			t.serverName[k.Replica] = k.Rid
		} else if k.Op == RETIRE {
			t.serverList[k.Rid] = false
			delete(t.serverName, k.Replica)
		}
	}
}

func (t *Server) join() {
	if t.primary {
		d := &Data{Op: JOIN, Id: t.maxNum, CSN: 1, Stable: true,
			AccTime: 1, Replica: "0", Rid: 0}
		t.accTime++
		t.vv[t.serverId]++
		t.name = "1, 0"
		t.stable = append(t.stable, d)
	} else {
		for i := 0; i < t.maxNum; i++ {
			if t.breakList[i] {
				continue
			}
			// run join algorithm
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+i))
			if err != nil {
				continue
			}
			defer client.Close()
			err = client.Call("Server.Sjoin", &t.serverId, &t.name)
			if err != nil {
				continue
			}
			t.serverList[i] = true
			break
		}
	}
	for {
		if !t.update {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		t.update = false
		for i := 0; i < t.maxNum; i++ {
			if i == t.serverId {
				continue
			}
			if (!t.breakList[i]) && t.serverList[i] {
				// sync with replica i
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+i))
				if err != nil {
					continue
				}
				tmp := Log{Stable: t.stable,
					Unstable: t.unstable}
				tmp.VV = make([]int, t.maxNum)
				copy(tmp.VV, t.vv)
				r := newLog()
				r.VV = make([]int, t.maxNum)
				err = client.Call("Server.Sync", &tmp, &r)
				if err != nil {
					client.Close()
					continue
				}
				if len(t.stable) < len(r.Stable) ||
					len(t.unstable) != len(r.Unstable) {
					t.update = true
					t.stable = r.Stable
					t.unstable = r.Unstable
					copy(t.vv, r.VV)
					// rebuild
					t.rebuild()
				}
				client.Close()
			}
		}
	}
}

func (t *Server) Sync(s *Log, r *Log) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.primary {
		log := make(map[int]*Data)
		for _, u := range t.stable {
			log[u.Id] = u
		}
		for _, u := range s.Unstable {
			if _, ok := log[u.Id]; !ok {
				u.Stable = true
				u.CSN = len(t.stable) + 1
				t.stable = append(t.stable, u)
				t.update = true
			}

		}
		r.Stable = t.stable
		r.Unstable = t.unstable
	} else {
		// sync stable log
		{
			log := make(map[int]*Data)
			for _, u := range t.stable {
				log[u.CSN] = u
			}
			for _, u := range s.Stable {
				log[u.CSN] = u
			}
			tmp := make([]*Data, 0)
			for i := 1; i <= len(log); i++ {
				tmp = append(tmp, log[i])
			}
			if len(t.stable) < len(tmp) {
				t.update = true
				t.stable = tmp
			}
			r.Stable = tmp
		}

		// sync unstable log
		{
			log := make(map[int]*Data)
			for _, u := range t.stable {
				log[u.Id] = u
			}
			tmp := make([]*Data, 0)
			for _, u := range t.unstable {
				if _, ok := log[u.Id]; !ok {
					log[u.Id] = u
					tmp = append(tmp, u)
				}
			}
			for _, u := range s.Unstable {
				if _, ok := log[u.Id]; !ok {
					log[u.Id] = u
					tmp = append(tmp, u)
				}
			}
			if len(t.unstable) != len(tmp) {
				t.update = true
				t.unstable = tmp
			}
			r.Unstable = tmp
		}
	}
	{
		r.VV = make([]int, t.maxNum)
		for i := 0; i < len(t.vv); i++ {
			if s.VV[i] > t.vv[i] {
				t.vv[i] = s.VV[i]
			}
			r.VV[i] = t.vv[i]
		}
	}

	// rebuild the log
	t.rebuild()

	return nil
}

func main() {
	n, _ := strconv.Atoi(os.Args[1])
	port := strconv.Itoa(n + 8800)

	server := newServer(n)
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalln("listen error:", err)
	}

	rpc.Register(server)
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	go server.join()

	for {
		time.Sleep(time.Second)
	}
}
