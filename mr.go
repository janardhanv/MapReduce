/*
 * MapReduce
 * CS 3410
 * Ren Quinn
 *
 */

package mapreduce

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
	"time"
)

const (
	WORK_DONE	= "1"
	WORK_MAP	= "2"
	WORK_REDUCE = "3"
)

const (
	TYPE_MAP = iota
	TYPE_REDUCE
)

type Request struct {
	Message string
	Address string
}

type Response struct {
	Message string
	Database string
	Offset int
	Chunksize int
	Type int
}

type Master struct {
	Address string
	Database string
	Offset int
	Chunksize int
	Count int
	M int
	R int
}

func (self *Master) GetWork(req Request, response *Response) error {
	if req.Message == WORK_DONE || self.Offset > self.Count {
		response.Message = WORK_DONE
		return nil
	}
	response.Database = self.Database
	response.Offset = self.Offset
	response.Chunksize = self.Chunksize
	response.Type = TYPE_MAP
	self.Offset = self.Offset + self.Chunksize

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
	flag.BoolVar(&ismaster, "ismaster", false, "True for master, false for worker.")
	flag.StringVar(&master, "master", "localhost:3410", "The location of the master.")
	flag.StringVar(&data, "dataset", "db.sql", "The data set to load from file.")
	flag.StringVar(&output, "output", "output", "Where to save the output.")
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
		 * // Divide the count up among the number of mapper
		 *
		 * // MAPPER - reads its range (limit, offset)
		 * select * from data order by key limit 2 offset 1;
		 *
		 * select distinct key .....
		 */
		query, err := db.Query("select count(*) from data;")
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
		me.Chunksize = chunksize
		me.M = m
		me.R = r
		me.Offset = 0
		me.Database = data
		me.Count = count

		rpc.Register(me)
		rpc.HandleHTTP()

		go func() {
			err := http.ListenAndServe(master, nil)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
		}()
		for {
			time.Sleep(1e9)
			fmt.Print(".")
		}
	} else {
		for {
			/*
			 * Call master, asking for work
			 */

			var resp Response
			var req Request
			err := call(master, "GetWork", req, &resp)
			if err != nil {
				failure("GetWork")
				continue
			}
			if resp.Message == WORK_DONE {
				log.Println("Finished Working")
				break
			}

			/*
			 * Do work
			 */

			log.Println(resp.Message)

			if resp.Type == TYPE_MAP {
				// Load data
				db, err := sql.Open("sqlite3", "./" + resp.Database)
				if err != nil {
					log.Println(err)
					failure("sql.Open")
					return
				}
				defer db.Close()

				// Query
				rows, err := db.Query(fmt.Sprintf("select key, value from data limit %d offset %d;", resp.Chunksize, resp.Offset))
				if err != nil {
					fmt.Println(err)
					failure("sql.Query")
					return
				}
				defer rows.Close()

				// Temp storage
				// TODO: How do I change directories to save the tmp files?
				db_tmp, err := sql.Open("sqlite3", "./tmp.sql")
				if err != nil {
					log.Println(err)
					failure("sql.Open")
					return
				}

				// Prepare tmp database
				sqls := []string{
					"create table if not exists data (key text not null, value text not null)",
					"create index if not exists data_key on data (key asc, value asc);",
				}
				for _, sql := range sqls {
					_, err = db_tmp.Exec(sql)
					if err != nil {
						fmt.Printf("%q: %s\n", err, sql)
						return
					}
				}

				for rows.Next() {
					var key string
					var value string
					rows.Scan(&key, &value)
					// Write the data locally
					sql := fmt.Sprintf("insert into data values ('%s', '%s');", key, value)
					_, err = db_tmp.Exec(sql)
					if err != nil {
						fmt.Printf("%q: %s\n", err, sql)
						return
					} else {
						fmt.Println(key, value)
					}
				}
				db_tmp.Close()
			}

			/*
			 * Notify the master when I'm done
			 */

			req.Message = WORK_DONE
			err = call(master, "GetWork", req, &resp)
			if err != nil {
				failure("GetWork")
				continue
			}
			if resp.Message == WORK_DONE {
				log.Println("Finished Working")
				break
			}
		}
	}
}
