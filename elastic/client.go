package elastic

import (
	"gopkg.in/olivere/elastic.v3"
	"os"
	"log"
)

func newClient(host string) *elastic.Client {
	params := []elastic.ClientOptionFunc{}

	params = append(params, elastic.SetURL(host))
	params = append(params, elastic.SetMaxRetries(2))
	params = append(params, elastic.SetSniff(false))
	params = append(params, elastic.SetHealthcheck(false))
	params = append(params, elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)))

	//params = append(params, elastic.SetTraceLog(log.New(os.Stdout, "TRACE", log.LstdFlags)))
	//params = append(params, elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)))

	client, err := elastic.NewClient(params...)

	if err != nil {
		panic(err)
	}

	return client
}
