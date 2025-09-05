package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type (
	Config struct {
		Profile string
		Region  string
	}

	DynamoDB interface {
		Query(ctx context.Context) ([]map[string]types.AttributeValue, error)
	}
)
