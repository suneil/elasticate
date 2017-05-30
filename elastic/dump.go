package elastic

import (
	"encoding/json"
	"io"

	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gopkg.in/cheggaaa/pb.v1"
	"gopkg.in/olivere/elastic.v3"
	"os"
	"sync"
)

// Dump an index
func Dump(host, index, file string) {
	client := newClient(host)

	outputFile, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		panic(err)
	}

	// Count total and setup progress
	total, err := client.Count(index).Do()
	if err != nil {
		panic(err)
	}
	bar := pb.StartNew(int(total))

	hits := make(chan *elastic.SearchHit)
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(hits)
		// Initialize scroller. Just don't call Do yet.
		scroll := client.Scroll(index).Size(2000)
		for {
			results, err := scroll.Do()
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				hits <- hit
			}

			// Check if we need to terminate early
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	var mutex = &sync.Mutex{}

	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for hit := range hits {
				marshaled, err := json.Marshal(*hit)
				if err != nil {
					fmt.Println(err)
				} else {
					mutex.Lock()
					_, err = outputFile.Write(marshaled)
					if err != nil {
						panic(err)
					}

					outputFile.Write([]byte{10})
					mutex.Unlock()
				}

				bar.Increment()

				// Terminate early?
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		panic(err)
	}

	bar.Finish()
}
