package main

import (
	"log"

	"github.com/sanrentai/gokv"
)

func main() {
	db, err := gokv.NewKVDB("d:/gokv")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Put("key1", "value1")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Put("key2", "value2")
	if err != nil {
		log.Fatal(err)
	}

	value, err := db.Get("key1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Value:", value)

	value, err = db.Get("key2")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Value:", value)
}
