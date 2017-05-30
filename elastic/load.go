package elastic

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"gopkg.in/olivere/elastic.v3"
)

// Load a file into elastic
func Load(host, filename string, index *string) {
	log.Printf("Loading %s to %s\n", filename, host)

	var err error
	counter := 0
	workerSize := 5

	queue := make(chan []byte)
	results := make(chan int)

	for w := 0; w < workerSize; w++ {
		go worker(host, index, queue, results)
	}

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	// Combine the defers?
	defer file.Close()

	defer func() {
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)

			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}

			panic(r)
		}

	}()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		bytes := scanner.Bytes()

		/*
			bufio.Scanner uses one []byte slice to store tokens (lines)
			which won't work when we're sending the slice (which
			are pointers) to the workers and they attempt to UnMarshal
			while scanner is overwriting the same slice.
		*/
		newToken := make([]byte, len(bytes))
		copy(newToken, bytes)

		queue <- newToken

		counter++
	}

	log.Printf("Closing queue - sent %d items", counter)

	close(queue)

	for w := 0; w < workerSize; w++ {
		<-results
	}

}

func worker(host string, forceIndex *string, queue <-chan []byte, results chan<- int) {
	// Create a client
	client := newClient(host)

	var err error
	var body map[string]interface{}
	var documentId string
	var docType string
	var msg interface{}
	var ok bool
	var index string
	var m map[string]interface{}

	if forceIndex != nil && *forceIndex == "" {
		forceIndex = nil
	} else {
		index = *forceIndex
	}

	defer func() {
		// recover panic and print out details of the last document
		if r := recover(); r != nil {
			var ok bool
			err, ok = r.(error)

			if !ok {
				err = fmt.Errorf("pkg: %v", r)
			}
			panic(r)
		}
	}()

	count := 0
	bulk := client.Bulk()

	for rawJson := range queue {
		err = json.Unmarshal(rawJson, &msg)
		if err != nil {
			panic(err)
		}

		m = msg.(map[string]interface{})

		if forceIndex == nil {
			index, ok = m["_index"].(string)

			if ok == false {
				panic("Could not assert _index to string")
			}
		}
		docType, ok = m["_type"].(string)

		if ok == false {
			panic("Could not assert _type to string")
		}

		documentId, ok = m["_id"].(string)
		if ok == false {
			panic("Could not assert _id to string")
		}

		body, ok = m["_source"].(map[string]interface{})
		if ok == false {
			panic("Could not assert _source")
		}

		req := elastic.NewBulkIndexRequest().
			Index(index).
			Type(docType).
			Id(documentId).
			Doc(body)

		bulk.Add(req)

		count++

		if count >= 1000 {
			_, err = bulk.Do()
			if err != nil {
				panic(err)
			}

			count = 0
		}

	}

	if count > 0 {
		_, err = bulk.Do()
		if err != nil {
			panic(err)
		}
	}

	results <- 1
}
