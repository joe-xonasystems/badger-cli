package cmd

import (
	"fmt"
	"github.com/joe-xonasystems/badger-cli/pkg/badger"
	"github.com/spf13/cobra"
	"log"
)

func getCmd() *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "get [key]",
		Short: "Get content of a specific key",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			storageFormat, err := cmd.Flags().GetString("fmt")
			if err != nil {
				log.Fatalln(err)
				return
			}

			db, err := badger.Open(cmd.Flag("dir").Value.String())
			if err != nil {
				log.Fatalln(err)
			}
			defer db.Close()

			value, err := db.Get(args[0], storageFormat)
			if err != nil {
				log.Fatalln(err)
			}

			fmt.Println(value)
		},
	}
	cmd.PersistentFlags().StringVar(&format, "fmt", "string", "Storage format of value ('string' or 'int64AsBytes'")

	return cmd
}

func init() {
	rootCmd.AddCommand(getCmd())
}
