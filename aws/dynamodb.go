package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var defaultLimit = 100

const (
	AND LogicalOperator = "AND"
	OR  LogicalOperator = "OR"
)

const (
	Equal              WhereOperator = "="
	NotEqual           WhereOperator = "!="
	LessThan           WhereOperator = "<"
	LessThanEqual      WhereOperator = "<="
	GreaterThan        WhereOperator = ">"
	GreaterThanEqual   WhereOperator = ">="
	Between            WhereOperator = "BETWEEN"
	In                 WhereOperator = "IN"
	Contains           WhereOperator = "CONTAINS"
	BeginsWith         WhereOperator = "BEGINS_WITH"
	AttributeExists    WhereOperator = "EXISTS"
	AttributeNotExists WhereOperator = "NOT_EXISTS"
)

type (
	LogicalOperator string

	QueryKeyValue struct {
		Key      string
		Value    any
		Operator WhereOperator
	}

	QueryOptions struct {
		Table     string
		Index     string // GSI name, e.g., "YearGenreIndex"
		Limit     int32  // Desired number of items per page
		Cursor    string // Base64-encoded LastEvaluatedKey for pagination
		Partition *QueryKeyValue
		Sort      *QueryKeyValue
		Where     *Where // Additional non-key filters
		// PartitionKey   string        // Partition key attribute, e.g., "year"
		// PartitionValue any           // Value for partition key, e.g., 2020
		// SortKey      string        // Optional: Sort key attribute, e.g., "genre"
		// SortOperator WhereOperator // Optional: Operator for sort key (e.g., Equal, BeginsWith)
		// SortValue    any           // Optional: Value for sort key, e.g., "Comedy"
	}

	WhereOperator string

	Where struct {
		Conditions []WhereCondition
		Groups     []Where
		Operator   LogicalOperator
	}

	WhereCondition struct {
		Field    string
		Operator WhereOperator
		Value    any
		// For BETWEEN operator, this holds the upper bound
		Value2 any
		// For IN operator, this should be a slice
		Values []any
	}
)

var (
	DynamoDBErrBuildFilterExpression = errors.New("failed to build filter expression")
	DynamoDBErrBuildUpdateExpression = errors.New("failed to build the update expression")
	DynamoDBErrIndexNotSet           = errors.New("index not set")
	DynamoDBErrQuery                 = errors.New("failed to perform query")
	DynamoDBErrTableNotSet           = errors.New("table not set")
	DynamoDBErrUnmarshal             = errors.New("failed to unmarshall items")
	DynamoDBErrUpdateItem            = errors.New("failed to update item")
	DynamoDBErrValueNotSet           = errors.New("key not set")
	DynamoDBErrPartitionNotSet       = errors.New("partition not set")
)

type dynamodbService struct {
	client *dynamodb.Client
}

func NewDynamoDB(config Config) DynamoDB {
	awsConfig := load(&config)
	client := dynamodb.NewFromConfig(awsConfig)
	return &dynamodbService{client}
}

func (d *dynamodbService) Query(ctx context.Context, opts QueryOptions) ([]map[string]types.AttributeValue, error) {
	// Validate
	if opts.Table == "" {
		return nil, DynamoDBErrTableNotSet
	}
	if opts.Index == "" {
		return nil, DynamoDBErrIndexNotSet
	}

	if opts.Partition == nil || opts.Partition.Key == "" || opts.Partition.Value == nil {
		return nil, DynamoDBErrPartitionNotSet
	}

	// Build key condition expression for GSI
	keyEx := expression.Key(opts.Partition.Key).Equal(expression.Value(opts.Partition.Value))

	if opts.Sort != nil && opts.Sort.Key != "" && opts.Sort.Value != nil {
		// TODO: Support other operators and create its own function
		switch opts.Sort.Operator {
		case Equal:
			keyEx = keyEx.And(expression.Key(opts.Sort.Key).Equal(expression.Value(opts.Sort.Value)))
		default:
			return nil, fmt.Errorf("unsupported sort key operator: %s", opts.Sort.Operator)
		}
	}

	builder := expression.NewBuilder().WithKeyCondition(keyEx)

	// Build filter expression for non-key attributes if provided
	if opts.Where != nil {
		filterExpr, err := d.buildFilterExpression(*opts.Where)
		if err != nil {
			return nil, DynamoDBErrBuildFilterExpression
		}
		builder = builder.WithFilter(filterExpr)
	}

	expr, err := builder.Build()
	if err != nil {
		return nil, err
	}

	// Set up query input
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(opts.Table),
		IndexName:                 aws.String(opts.Index),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		KeyConditionExpression:    expr.KeyCondition(),
		ProjectionExpression:      expr.Projection(),
		Limit:                     aws.Int32(opts.Limit),
	}

	if expr.Filter() != nil {
		input.FilterExpression = expr.Filter()
	}

	// Marshal with indentation for readability
	out, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))

	queryPaginator := dynamodb.NewQueryPaginator(d.client, input)

	var items []map[string]types.AttributeValue
	for queryPaginator.HasMorePages() {
		response, err := queryPaginator.NextPage(ctx)
		if err != nil {
			return nil, DynamoDBErrQuery
		}

		items = append(items, response.Items...)
	}

	return items, nil
}

func (d *dynamodbService) buildFilterExpression(where Where) (expression.ConditionBuilder, error) {
	var conditions []expression.ConditionBuilder

	// Process individual conditions
	for _, condition := range where.Conditions {
		cond, err := d.buildSingleCondition(condition)
		if err != nil {
			return expression.ConditionBuilder{}, err
		}

		conditions = append(conditions, cond)
	}

	// Process the nested groups
	if where.Groups != nil {
		for _, nestedGroup := range where.Groups {
			nestedCond, err := d.buildFilterExpression(nestedGroup)
			if err != nil {
				return expression.ConditionBuilder{}, err
			}

			conditions = append(conditions, nestedCond)
		}
	}

	if len(conditions) == 0 {
		return expression.ConditionBuilder{}, errors.New("no conditions provided")
	}

	// Combine all conditions based on the logical operator
	result := conditions[0]
	for i := 1; i < len(conditions); i++ {
		if where.Operator == "" || where.Operator == AND {
			result = result.And(conditions[i])
		} else {
			result = result.Or(conditions[i])
		}
	}

	return result, nil
}

func (d *dynamodbService) buildSingleCondition(cond WhereCondition) (expression.ConditionBuilder, error) {
	name := expression.Name(cond.Field)

	switch cond.Operator {
	case Equal:
		return name.Equal(expression.Value(cond.Value)), nil
	case NotEqual:
		return name.NotEqual(expression.Value(cond.Value)), nil
	case LessThan:
		return name.LessThan(expression.Value(cond.Value)), nil
	case LessThanEqual:
		return name.LessThanEqual(expression.Value(cond.Value)), nil
	case GreaterThan:
		return name.GreaterThan(expression.Value(cond.Value)), nil
	case GreaterThanEqual:
		return name.GreaterThanEqual(expression.Value(cond.Value)), nil
	case Between:
		return name.Between(expression.Value(cond.Value), expression.Value(cond.Value2)), nil
	case In:
		if len(cond.Values) == 0 {
			return expression.ConditionBuilder{}, errors.New("IN operator requires non-empty Values slice")
		}

		// Convert first value separately, then spread the rest
		firstValue := expression.Value(cond.Values[0])
		additionalValues := make([]expression.OperandBuilder, len(cond.Values)-1)
		for i, v := range cond.Values[1:] {
			additionalValues[i] = expression.Value(v)
		}

		return name.In(firstValue, additionalValues...), nil
	case Contains:
		return name.Contains(fmt.Sprint(cond.Value)), nil
	case BeginsWith:
		return name.BeginsWith(fmt.Sprint(cond.Value)), nil
	case AttributeExists:
		return name.AttributeExists(), nil
	case AttributeNotExists:
		return name.AttributeNotExists(), nil
	default:
		return expression.ConditionBuilder{}, fmt.Errorf("unsupported operator: %s", cond.Operator)
	}
}
