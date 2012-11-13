/*
 * MapReduce
 * CS 3410
 * Ren Quinn
 *
 */

package main

import (
	"bufio"
	"flag"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/rpc"
	"os"
)

type Request struct {
	Message string
	Address string
}

type Response struct {
	Message string
}

type Master struct {
	Address string
}

func (self *Master) Ping(_ Request, response *Response) error {
	response.Message = "Successfully Pinged " + self.Address
	log.Println("I Got Pinged")
	return nil
}

func call(address, method string, request Request, response *Response) error {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Println("rpc dial: ", err)
		return err
	}
	defer client.Close()

	err = client.Call("Master."+method, request, response)
	if err != nil {
		log.Println("rpc call: ", err)
		return err
	}

	return nil
}

func failure(f string) {
	log.Println("Call",f,"has failed.")
}

func usage() {
	fmt.Println("Usage: ", os.Args[0], "[-dataset=data.sql] [-l=<n>] <local_port> [<port1>...<portn>] ")
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

func logf(format string, args ...interface{}) {
	if true {
		log.Printf(format, args...)
	}
}

func main() {
	var data, master string
	var m, r int
	var ismaster bool
	flag.BoolVar(&ismaster, "ismaster", false, "True for master, false for worker")
	flag.StringVar(&master, "master", "localhost:3410", "True for master, false for worker")
	flag.StringVar(&data, "dataset", "db.sql", "The data set to load from file.")
	flag.IntVar(&m, "m", 1, "The number of map tasks to run.")
	flag.IntVar(&r, "r", 1, "The number of reduce tasks to run.")
	flag.Parse()
	logf("Master? %t, File: %s, Maps: %d, Reduces: %d", ismaster, data, m, r)

	if ismaster {

		db, err := sql.Open("sqlite3", "./" + data)
		if err != nil {
			log.Println(err)
				failure("sql.Open")
				return
		}
		defer db.Close()

		/*
		 * // MASTER - counts the data
		 * select count(*) from data;
		// Divide the count up among the number of mapper
		 *
		 * // MAPPER - reads its range (limit, offset)
		 * select * from data order by key limit 2 offset 1;
		 *
		 * select distinct key .....
		 */
		query, err := db.Query("select count(*) from data;",)
		if err != nil {
			fmt.Println(err)
				return
		}
		defer query.Close()

		// Split up the data
		var count int
		var chunksize int
		query.Next()
		query.Scan(&count)
		fmt.Println(count)
		chunksize = int(math.Ceil(float64(count)/float64(m)))
		fmt.Println(chunksize)

		// Set up the RPC server to listen for workers
		me := new(Master)
		rpc.Register(me)
		rpc.HandleHTTP()

		go func() {
			err := http.ListenAndServe(master, nil)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}()
	} else {
		for {
			// Call master, asking for work

			var resp Response
			var req Request
			err := call(master, "GetWork", req, &resp)
			if err != nil {
				failure("GetWork")
				continue
			}
			log.Println(resp.Message)
		}
	}
/*
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
			var resp Response
			var req Request
			//net.JoinHostPort(host, port)
			err := call(master, "Ping", req, &resp)
			if err != nil {
				failure("Ping")
				continue
			}
			log.Println(resp.Message)
		}
	}
	*/
}
