package main

import (
	"log"

	"github.com/sanrentai/gokv"
)

func main() {
	db, err := gokv.NewKVDB("db.json")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// err = db.Put("key1", "value1")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// err = db.Put("key2", "value2")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	db.Delete("key1")

	value, ok := db.Get("key1")
	if !ok {
		log.Println("不存在key1")
	} else {
		log.Println("Value:", value)
	}

	value, ok = db.Get("key2")
	if !ok {
		log.Println("不存在key2")
	}
	log.Println("Value:", value)
}
