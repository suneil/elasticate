package cmd

import (
	"github.com/spf13/cobra"
	"github.com/suneil/elasticate/elastic"
	"log"
)

// dumpCmd represents the dump command
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump index (or indices) to file",
	Long: `Dump index (or indices) to file`,
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

		elastic.Dump(host, index, file)
	},
}

func init() {
	RootCmd.AddCommand(dumpCmd)

	dumpCmd.Flags().StringP("host", "s", "http://localhost:9200", "index")
	dumpCmd.Flags().StringP("index", "i", "", "index")
	dumpCmd.Flags().StringP("file", "f", "", "filename to dump to")

}
