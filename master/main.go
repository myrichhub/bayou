package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type Data struct {
	Key   string
	Value string
}

func main() {

	srvIds := make(map[int]bool)
	processes := make([]*os.Process, 0)

	// read from command line
	bio := bufio.NewReader(os.Stdin)
	for {
		// print the command line starting char
		// fmt.Printf("> ")

		line, _, err := bio.ReadLine()
		if err != nil {
			for _, p := range processes {
				p.Kill()
			}
			break
		}

		// handle the command line parameters
		cmd := strings.Split(string(line), " ")
		switch cmd[0] {
		case "joinServer":
			n, _ := strconv.Atoi(cmd[1])
			srvIds[n] = true
			var procAttr os.ProcAttr
			procAttr.Files = []*os.File{nil, os.Stdout,
				os.Stderr}
			p, err := os.StartProcess("./srv",
				[]string{"server", cmd[1]}, &procAttr)
			if err != nil {
				log.Fatalln("start server:", err)
			}
			processes = append(processes, p)
		case "retireServer":
			n, _ := strconv.Atoi(cmd[1])
			delete(srvIds, n)
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+n))
			err = client.Call("Server.Retire", &n, nil)
			if err != nil {
				log.Fatalln(err)
			}
			client.Close()
		case "joinClient":
			var procAttr os.ProcAttr
			procAttr.Files = []*os.File{nil, os.Stdout,
				os.Stderr}
			p, err := os.StartProcess("./cli",
				[]string{"client", cmd[1], cmd[2]}, &procAttr)
			if err != nil {
				log.Fatalln("start client:", err)
			}
			processes = append(processes, p)
		case "breakConnection":
			n, _ := strconv.Atoi(cmd[1])
			m, _ := strconv.Atoi(cmd[2])
			{
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+n))
				err = client.Call("Server.Break", &m, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
			{
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+m))
				err = client.Call("Server.Break", &n, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
		case "restoreConnection":
			n, _ := strconv.Atoi(cmd[1])
			m, _ := strconv.Atoi(cmd[2])
			{
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+n))
				err = client.Call("Server.Restore", &m, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
			{
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+m))
				err = client.Call("Server.Restore", &n, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
		case "pause":
			for n, _ := range srvIds {
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+n))
				err = client.Call("Server.Pause", &n, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
		case "start":
			for n, _ := range srvIds {
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+n))
				err = client.Call("Server.Start", &n, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
		case "stabilize":
			for n, _ := range srvIds {
				client, err := rpc.DialHTTP("tcp",
					"localhost:"+strconv.Itoa(8800+n))
				err = client.Call("Server.Stabilize", &n, nil)
				if err != nil {
					log.Fatalln(err)
				}
				client.Close()
			}
		case "printLog":
			n, _ := strconv.Atoi(cmd[1])
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+n))
			var r string
			err = client.Call("Server.PrintLog", &n, &r)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%s", r)
			client.Close()
		case "put":
			n, _ := strconv.Atoi(cmd[1])
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+n))
			data := &Data{Key: cmd[2], Value: cmd[3]}
			err = client.Call("Server.Put", data, nil)
			if err != nil {
				log.Fatalln(err)
			}
			client.Close()
		case "get":
			n, _ := strconv.Atoi(cmd[1])
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+n))
			var r string
			err = client.Call("Server.Get", &cmd[2], &r)
			if err != nil {
				log.Fatalln(err)
			}
			fmt.Printf("%s:%s\n", cmd[2], r)
			client.Close()
		case "delete":
			n, _ := strconv.Atoi(cmd[1])
			client, err := rpc.DialHTTP("tcp",
				"localhost:"+strconv.Itoa(8800+n))
			err = client.Call("Server.Delete", &cmd[2], nil)
			if err != nil {
				log.Fatalln(err)
			}
			client.Close()
		default:
		}
		time.Sleep(time.Second)
	}
}
