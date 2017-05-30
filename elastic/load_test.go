package elastic

import "testing"

// TestLoad tests loading data into elastic server
func TestLoad(t *testing.T)  {
	filename := "../test.json"
	host := "http://localhost:9200"

	Load(host, filename, nil)

}
