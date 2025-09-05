package aws

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type (
	Config struct {
		Profile string
		Region  string
	}

	DynamoDB interface {
		Query(ctx context.Context, opts QueryOptions) ([]map[string]types.AttributeValue, error)
	}
)

// Loads the config either via AWS_PROFILE or environment variables
func load(config *Config) aws.Config {
	if config.Profile != "" {
		os.Setenv("AWS_PROFILE", config.Profile)
	}

	cfg, err := awsconfig.LoadDefaultConfig(
		context.TODO(),
		awsconfig.WithRegion(config.Region),
	)
	if err != nil {
		log.Fatal(err)
	}

	return cfg
}
