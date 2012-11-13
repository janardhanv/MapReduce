package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"fmt"
	"log"
	"os"
)

func main() {
	os.Remove("./db.sql")

	db, err := sql.Open("sqlite3", "./db.sql")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("FILE DOES NOT EXIST")
		} else {
			log.Println(err)
			return
		}
	}
	defer db.Close()

	/*
	 * // SINGLE INPUT FILE
	 * create table if not exists data (
	 *   key text not null,
	 *   value text not null
	 * )
	 *
	 * create index if not exists data_key on data (key asc, value asc);
	 *
	 * insert into data values ('green', 'lizard');
	 * insert into data values ('red', 'snake');
	 * insert into data values ('blue', 'chicken');
	 * insert into data values ('blue', 'falcon');
	 *
	 * // MASTER - counts the data
	 * select count(*) from data;
	 // Divide the count up among the number of mapper
	 *
	 * // MAPPER - reads its range (limit, offset)
	 * select * from data order by key limit 2 offset 1;
     *
	 * select distinct key .....
	 */
	 sqls := []string{
		"create table if not exists data (key text not null, value text not null)",
		"create index if not exists data_key on data (key asc, value asc);",
		"insert into data values ('green', 'lizard');",
		"insert into data values ('red', 'snake');",
		"insert into data values ('blue', 'chicken');",
		"insert into data values ('blue', 'falcon');",
		"insert into data values ('1green', 'lizard');",
		"insert into data values ('1red', 'snake');",
		"insert into data values ('1blue', 'chicken');",
		"insert into data values ('1blue', 'falcon');",
		"insert into data values ('2green', 'lizard');",
		"insert into data values ('2red', 'snake');",
		"insert into data values ('2blue', 'chicken');",
		"insert into data values ('2blue', 'falcon');",
	 }

	 for _, sql := range sqls {
		 _, err = db.Exec(sql)
		 if err != nil {
			 fmt.Printf("%q: %s\n", err, sql)
			 return
		 }
	 }
}
