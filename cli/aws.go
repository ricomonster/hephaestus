/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/spf13/cobra"

	"github.com/ricomonster/hephaestus/aws"
	"github.com/ricomonster/hephaestus/config"
)

// awsCmd represents the aws command
var awsCmd = &cobra.Command{
	Use: "aws",
	Run: func(cmd *cobra.Command, args []string) {
		// Load the config
		fmt.Println("Loading...")
		c, err := config.Load(".env")
		if err != nil {
			log.Fatal(err)
		}

		// Load DynamoDB
		ddb := aws.NewDynamoDB(*c.AWS)

		fmt.Println("Querying...")
		items, err := ddb.Query(context.TODO(), aws.QueryOptions{
			Table: "table",
			Index: "Status",
			Partition: &aws.QueryKeyValue{
				Key:   "Status",
				Value: "active",
			},
			Where: &aws.Where{
				Conditions: []aws.WhereCondition{{
					Field:    "Keywords",
					Operator: aws.Contains,
					Value:    "pika",
				}},
			},
			// Sort: &aws.QueryKeyValue{
			// 	Key:      "Status",
			// 	Value:    "active",
			// 	Operator: aws.Equal,
			// },
		})

		// Marshal with indentation for readability
		out, err := json.MarshalIndent(items, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(out))
	},
}

func init() {
	rootCmd.AddCommand(awsCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mtgCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mtgCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
