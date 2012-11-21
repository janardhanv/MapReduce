/*
 * MapReduce
 * CS 3410
 * Ren Quinn
 *
 */

package mapreduce

import (
	"crypto/sha1"
	"bufio"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
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

type Pair struct {
	Key string
	Value string
}

type Config struct {
	Master string
	InputData string
	Output string
	M int
	R int
}


type MapFunc func(key, value string, output chan<- Pair) error
type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error


type Request struct {
	Message string
	Address string
	Type int
	Work Work
}

type Response struct {
	Message string
	Database string
	Offset int
	Chunksize int
	Type int
	Mapper int
	Reducer int
	Work Work
}

type Master struct {
	Address string
// TODO: The following 4 might not be necessary
/*
	Database string
	Offset int
	Chunksize int
	Count int
*/
	M int
	R int
	Maps []Work
	ReduceCount int
}

type Work struct {
	WorkerID int
	Type int
	Filename string
	Offset int
	Size int
	M int
	R int
}

func (self *Master) GetWork(_ Request, response *Response) error {
	if len(self.Maps) > 0 { // MAP
		response.Type = TYPE_MAP
		work := self.Maps[0]
		work.M = self.M
		work.R = self.R
		response.Work = work
		self.Maps = self.Maps[1:]
	} else if self.ReduceCount < self.R { // REDUCE
		response.Type = TYPE_REDUCE
		var work Work
		work.WorkerID = self.ReduceCount
		self.ReduceCount = self.ReduceCount + 1
		response.Work = work
	} else { // DONE
		response.Message = WORK_DONE
	}

	return nil
}

func (self *Master) Notify(request Request, response *Response) error {
	work := request.Work
	if request.Type == TYPE_MAP {
		work.Type = TYPE_REDUCE
		work.WorkerID = 0 // Maybe?
	} else if request.Type == TYPE_REDUCE {
		// TODO: What do I do when a reducer is done?
	} else {
		log.Println("INVALID TYPE")
	}

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

func hash(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

/*
 * // MASTER - counts the data
 * select count(*) from data;
 * // Divide the count up among the number of mappers
 */
func StartMaster(config *Config) error {
	// Config variables
	master := config.Master
	input := config.InputData
	//output := config.Output //TODO: Not important to do now
	m := config.M
	r := config.R

	// Load the input data
	db, err := sql.Open("sqlite3", input)
	if err != nil {
		log.Println(err)
		failure("sql.Open")
		return err
	}
	defer db.Close()

	// Count the work to be done
	query, err := db.Query("select count(*) from data;")
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer query.Close()

	// Split up the data per m
	var count int
	var chunksize int
	query.Next()
	query.Scan(&count)
	chunksize = int(math.Ceil(float64(count)/float64(m)))
	var works []Work
	for i:=0; i<m; i++ {
		var work Work
		work.Type = TYPE_MAP
		work.Filename = input
		work.Offset = i * chunksize
		work.Size = chunksize
		work.WorkerID = i
		works = append(works, work)
	}

	// Set up the RPC server to listen for workers
	me := new(Master)
	me.Maps = works
	me.M = m
	me.R = r
	me.ReduceCount = 0
// TODO: The following 4 might not be necessary
/*
	me.Offset = 0
	me.Database = input
	me.Count = count
	me.Chunksize = chunksize
*/

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
		// TODO: What do I do now?
		time.Sleep(1e9)
		fmt.Print(".")
	}

	return nil
}

 /*
 * // MAPPER - reads its range (limit, offset)
 * select * from data order by key limit 2 offset 1;
 *
 * select distinct key .....
 */
func StartWorker(m MapFunc, r ReduceFunc, master string) error {
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
		work := resp.Work

		/*
		 * Do work
		 */
		// Walks through the assigned sql records
		// Call the given mapper function
		// Receive from the output channel in a go routine
		// Feed them to the reducer through its own sql files
		// Close the sql files

		if resp.Type == TYPE_MAP {
			// Load data
			db, err := sql.Open("sqlite3", work.Filename)
			if err != nil {
				log.Println(err)
				failure("sql.Open")
				return err
			}
			defer db.Close()

			// Query
			rows, err := db.Query(fmt.Sprintf("select key, value from data limit %d offset %d;", work.Size, work.Offset))
			if err != nil {
				fmt.Println(err)
				failure("sql.Query")
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var key string
				var value string
				rows.Scan(&key, &value)

				// Temp storage
				// Each time the map function emits a key/value pair, you should figure out which reduce task that pair will go to.
				var reducer *big.Int
				reducer.Mod(hash(key), big.NewInt(int64(work.R)))
				db_tmp, err := sql.Open("sqlite3", fmt.Sprintf("/tmp/map_output/%d/map_out_%d.sql", work.WorkerID, reducer.Int64()))
				if err != nil {
					log.Println(err)
					failure(fmt.Sprintf("sql.Open - /tmp/map_output/%d/map_out_%d.sql", work.WorkerID, reducer.Int64()))
					return err
				}
				defer db_tmp.Close()

				// Prepare tmp database
				sqls := []string{
					"create table if not exists data (key text not null, value text not null)",
					"create index if not exists data_key on data (key asc, value asc);",
				}
				for _, sql := range sqls {
					_, err = db_tmp.Exec(sql)
					if err != nil {
						fmt.Printf("%q: %s\n", err, sql)
						return err
					}
				}

				//type MapFunc func(key, value string, output chan<- Pair) error
				outChan := make(chan Pair)
				err = m(key, value, outChan)
				if err != nil {
					failure("map")
					return err
				}

				// Get the output from the map function's output channel
				//var pairs []Pair
				pair := <-outChan
				for pair.Key != "" {
					key, value = pair.Key, pair.Value
					// Write the data locally
					sql := fmt.Sprintf("insert into data values ('%s', '%s');", key, value)
					_, err = db_tmp.Exec(sql)
					if err != nil {
						fmt.Printf("%q: %s\n", err, sql)
						return err
					}
					//fmt.Println(key, value)
					pair = <-outChan
				}
			}

			// Serve the files so each reducer can get them
			// we use StripPrefix so that /tmpfiles/somefile will access /tmp/somefile
			// /tmp/map_output/%d/tmp_map_out_%d.sql
			go func() {
				// TODO: Serve this incrementally starting at port 4000
				// (4000 + work.WorkerID)
				http.Handle("/", http.FileServer(http.Dir(fmt.Sprintf("/tmp/map_output/%d", work.WorkerID))))
			}()
		} else if resp.Type == TYPE_REDUCE {
			//type ReduceFunc func(key string, values <-chan string, output chan<- Pair) error
			// Load each input file one at a time (copied from each map task)
			for i:=0; i<work.M; i++ {
				res, err := http.Get(fmt.Sprintf("localhost:%d/tmp/map_output/%d/map_out_%d.sql", 4000+i, i, work.WorkerID))
				if err != nil {
					failure("http.Get")
					log.Fatal(err)
				}
				var file []byte
				_, err = io.ReadFull(res.Body, file)
				if err != nil {
					failure("ioutil.ReadAll")
					log.Fatal(err)
				}
				res.Body.Close()

				// Open the output file using the os package and use io.Copy to get the full contents.
				// fmt.Printf("%s", file)
				f, err := os.Create(fmt.Sprintf("map_out_%d_mapper_%d.sql", work.WorkerID, i))
				defer f.Close()
				if err != nil {
					failure("os.Create")
					log.Fatal(err)
				}
				_, err = f.Write(file)
				if err != nil {
					failure("file.WriteString")
					log.Fatal(err)
				}
			}
			// TODO: Combine all the rows into a single input file

			// Walk through the file's rows, performing the reduce func
				// Basically you will call the reduce func for each key you have, sending each key/value for the current key across the input channel
				// then closing the channel and calling the reduce func again for a different key
		} else {
			log.Println("INVALID WORK TYPE")
			var err error
			return err
		}

		/*
		 * Notify the master when I'm done
		 */

		//TODO: send the location of my output data over to Notify
		err = call(master, "Notify", req, &resp)
		if err != nil {
			failure("Notify")
			continue
		}
		if resp.Message == WORK_DONE {
			log.Println("Finished Working")
			break
		}
	}

	return nil
}
