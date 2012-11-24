package main

import (
	"flag"
	"log"
	"./mapreduce"
	"strconv"
	"strings"
	"unicode"
)

func logf(format string, args ...interface{}) {
	if true {
		log.Printf(format, args...)
	}
}

func identityMapper(key, value string, output chan<- mapreduce.Pair) error {
	p := mapreduce.Pair{Key: key, Value: value}
	output <- p
	close(output)

	return nil
}

func identityReducer(key string, values <-chan string, output chan<- mapreduce.Pair) error {
	for v, ok := <-values; ok; v, ok = <-values {
		p := mapreduce.Pair{Key: key, Value: v}
		output <- p
	}
	close(output)

	return nil
}

func wordCountMapper(key, value string, output chan<- mapreduce.Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func (r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- mapreduce.Pair{Key: word, Value: "1"}
		}
	}

	return nil
}

func wordCountReducer(key string, values <-chan string, output chan<- mapreduce.Pair) error {
	defer close(output)
	count := 0
	for v, ok := <-values; ok; v, ok = <-values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := mapreduce.Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p

	return nil
}

func main() {
	var input, master, output string
	var m, r int
	var ismaster bool
	flag.BoolVar(&ismaster, "ismaster", false, "True for master, false for worker.")
	flag.StringVar(&master, "master", "localhost:3410", "The location of the master.")
	flag.StringVar(&input, "inputdata", "./db.sql", "The data set to load from file.")
	flag.StringVar(&output, "output", "output", "Where to save the output.")
	flag.IntVar(&m, "m", 1, "The number of map tasks to run.")
	flag.IntVar(&r, "r", 1, "The number of reduce tasks to run.")
	flag.Parse()
	if ismaster {
		logf("Master - File: %s, Maps: %d, Reduces: %d", input, m, r)
		var config mapreduce.Config
		config.Master = master
		config.InputData = input
		config.Output = output
		config.M = m
		config.R = r
		err := mapreduce.StartMaster(&config)
		if err != nil {
			log.Println(err)
		}
		err = mapreduce.Merge(r, wordCountReducer)
		if err != nil {
			log.Println(err)
		}
	} else {
		logf("Worker - Master Location: %s", master)
		//err := mapreduce.StartWorker(mapreduce.MapFunc(identityMapper), mapreduce.ReduceFunc(identityReducer), master)
		//err := mapreduce.StartWorker(identityMapper, identityReducer, master)
		err := mapreduce.StartWorker(wordCountMapper, wordCountReducer, master)
		if err != nil {
			log.Println(err)
		}
	}
}
