package cmd

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/suneil/elasticate/elastic"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "load file",
	Long:  `Loads a file into elastic`,
	Run: func(cmd *cobra.Command, args []string) {
		flags := cmd.LocalFlags()

		host, err := flags.GetString("host")
		if err != nil {
			log.Fatalln("Error getting host parameter")
		}

		index, err := flags.GetString("index")
		if err != nil {
			log.Fatalln("Error getting index parameter")
		}

		file, err := flags.GetString("file")
		if err != nil {
			log.Fatalln("Error getting file parameter")
		}

		elastic.Load(host, file, &index)
	},
}

func init() {
	RootCmd.AddCommand(loadCmd)

	loadCmd.Flags().StringP("host", "s", "http://localhost:9200", "index")
	loadCmd.Flags().StringP("index", "i", "", "index (optional)")
	loadCmd.Flags().StringP("file", "f", "", "filename to dump to")

}
