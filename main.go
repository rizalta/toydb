package main

import (
	"fmt"
	"log"

	"github.com/rizalta/toydb/index"
	"github.com/rizalta/toydb/pager"
	"github.com/rizalta/toydb/storage"
)

func main() {
	pager, err := pager.NewPager("data.db")
	if err != nil {
		log.Fatal(err)
	}
	index := index.NewBTree()
	storage := storage.NewStore(pager, index)
	defer storage.Close()

	val, _, _ := storage.Get("hello")
	fmt.Printf("%s", val)
	val, _, _ = storage.Get("hello1")
	fmt.Printf("%s", val)
	val, _, _ = storage.Get("hello2")
	fmt.Printf("%s", val)
	val, _, _ = storage.Get("hello3")
	fmt.Printf("%s", val)
	val, _, _ = storage.Get("hello4")
	fmt.Printf("%s", val)
	val, _, _ = storage.Get("hello5")
	fmt.Printf("%s", val)
}
