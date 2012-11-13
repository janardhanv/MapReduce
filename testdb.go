package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"fmt"
	"log"
)

func main() {
	db, err := sql.Open("sqlite3", "./db.sql")
	if err != nil {
		log.Println(err)
		return
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
	rows, err := db.Query("select key, value from data;",)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var value string
		rows.Scan(&key, &value)
		fmt.Println(key, value)
	}

}

