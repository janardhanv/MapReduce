/*
 * Paxos
 * CS 3410
 * Ren Quinn
 *
 */

package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
)

type Request struct {
	Message string
	Address string
}

type Response struct {
	Message string
}

type Temp struct {
	Address string
}

func (self *Temp) Ping(_ Request, response *Response) error {
	response.Message = "Successfully Pinged " + self.Address
	log.Println("I Got Pinged")
	return nil
}

func call(address, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", getAddress(address))
	if err != nil {
		log.Println("rpc dial: ", err)
		return err
	}
	defer client.Close()

	err = client.Call("Temp."+method, request, response)
	if err != nil {
		log.Println("rpc call: ", err)
		return err
	}

	return nil
}

func getAddress(v string) string {
	return net.JoinHostPort("localhost", v)
}

func failure(f string) {
	log.Println("Call",f,"has failed.")
}

func help() {
	fmt.Println("==============================================================")
	fmt.Println("                          COMMANDS")
	fmt.Println("==============================================================")
	fmt.Println("help               - Display this message.")
	fmt.Println("dump               - Display info about the current node.")
	fmt.Println("put <key> <value>  - Put a value.")
	fmt.Println("get <key>          - Get a value.")
	fmt.Println("delete <key>       - Delete a value.")
	fmt.Println("quit               - Quit the program.")
	fmt.Println("==============================================================")
}

func usage() {
	fmt.Println("Usage: ", os.Args[0], "[-v=<false>], [-l=<n>] <local_port> [<port1>...<portn>] ")
	fmt.Println("     -v        Verbose. Dispay the details of the paxos messages. Default is false")
	fmt.Println("     -l        Latency. Sets the latency between messages as a random duration between [n,2n)")
}

func readLine(readline chan string) {
	// Get command
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal("READLINE ERROR:", err)
	}
	readline <- line
}

func logM(message string) {
	if true {
		log.Println(message)
	}
}

func main() {
	/*
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	*/

	me := new(Temp)
	rpc.Register(me)
	rpc.HandleHTTP()

	go func() {
		err := http.ListenAndServe(getAddress("3000"), nil)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	}()

	readline := make(chan string, 1)

	mainloop: for {
		go readLine(readline)
		line := <-readline
		l := strings.Split(strings.TrimSpace(line), " ")
		if strings.ToLower(l[0]) == "quit" {
			fmt.Println("QUIT")
			fmt.Println("Goodbye. . .")
			break mainloop
		} else if strings.ToLower(l[0]) == "ping" {
			address := "3000"
			fmt.Println("PINGING",address[1:])
			var resp Response
			var req Request
			err := call(address, "Ping", req, &resp)
			if err != nil {
				failure("Ping")
				continue
			}
			log.Println(resp.Message)
		} else {
			help()
		}
	}
}
