package dynamodb_helper

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoTable struct {
	client    *dynamodb.Client
	tableName string
	table     *dynamodb.Table
}

func NewDynamoTable(tableName string, regionName string) (*DynamoTable, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(regionName))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	table := &dynamodb.Table{
		Name:   aws.String(tableName),
		Client: client,
	}

	return &DynamoTable{
		client:    client,
		tableName: tableName,
		table:     table,
	}, nil
}

func (dt *DynamoTable) Get(ctx context.Context, pkName string, pk string, skName *string, sk *string) (bool, map[string]types.AttributeValue, error) {
	key := map[string]types.AttributeValue{
		pkName: &types.AttributeValueMemberS{Value: pk},
	}
	if skName != nil && sk != nil {
		key[*skName] = &types.AttributeValueMemberS{Value: *sk}
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(dt.tableName),
		Key:       key,
	}

	result, err := dt.client.GetItem(ctx, input)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get item: %w", err)
	}

	if result.Item != nil {
		return true, result.Item, nil
	}
	return false, nil, nil
}

func (dt *DynamoTable) Push(ctx context.Context, data map[string]types.AttributeValue) (bool, error) {
	input := &dynamodb.PutItemInput{
		TableName: aws.String(dt.tableName),
		Item:      data,
	}

	_, err := dt.client.PutItem(ctx, input)
	if err != nil {
		return false, fmt.Errorf("failed to put item: %w", err)
	}
	return true, nil
}

func (dt *DynamoTable) Update(ctx context.Context, pkName string, pk string, skName *string, sk *string, updateExpression string, expressionValues map[string]types.AttributeValue, expressionNames map[string]string, conditionExpression *string) (bool, error) {
	key := map[string]types.AttributeValue{
		pkName: &types.AttributeValueMemberS{Value: pk},
	}
	if skName != nil && sk != nil {
		key[*skName] = &types.AttributeValueMemberS{Value: *sk}
	}

	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(dt.tableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionValues,
	}

	if expressionNames != nil {
		input.ExpressionAttributeNames = expressionNames
	}
	if conditionExpression != nil {
		input.ConditionExpression = aws.String(*conditionExpression)
	}

	_, err := dt.client.UpdateItem(ctx, input)
	if err != nil {
		return false, fmt.Errorf("failed to update item: %w", err)
	}
	return true, nil
}

func (dt *DynamoTable) Delete(ctx context.Context, pkName string, pk string, skName *string, sk *string) (bool, error) {
	key := map[string]types.AttributeValue{
		pkName: &types.AttributeValueMemberS{Value: pk},
	}
	if skName != nil && sk != nil {
		key[*skName] = &types.AttributeValueMemberS{Value: *sk}
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(dt.tableName),
		Key:       key,
	}

	_, err := dt.client.DeleteItem(ctx, input)
	if err != nil {
		return false, fmt.Errorf("failed to delete item: %w", err)
	}
	return true, nil
}

func (dt *DynamoTable) BuildConditionExpression(attribute string, operator string, value interface{}, conditionType string) (string, map[string]types.AttributeValue, error) {
	var expression string
	expressionValues := make(map[string]types.AttributeValue)
	placeholder := fmt.Sprintf(":%s_val", strings.ReplaceAll(attribute, "#", "")) // Avoid '#' in placeholder

	switch operator {
	case "eq":
		expression = fmt.Sprintf("#%s = %s", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	case "lt":
		expression = fmt.Sprintf("#%s < %s", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	case "lte":
		expression = fmt.Sprintf("#%s <= %s", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	case "gt":
		expression = fmt.Sprintf("#%s > %s", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	case "gte":
		expression = fmt.Sprintf("#%s >= %s", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	case "contains":
		expression = fmt.Sprintf("contains(#%s, %s)", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	case "begins_with":
		expression = fmt.Sprintf("begins_with(#%s, %s)", attribute, placeholder)
		expressionValues[placeholder] = attributeValueFromInterface(value)
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", operator)
	}

	return expression, expressionValues, nil
}

func (dt *DynamoTable) Filter(ctx context.Context, pkName string, pk string, skName *string, sk *string, skRange *[]string, lsiName *string, lsiValue *string, projectedKeys []string) (bool, []map[string]types.AttributeValue, error) {
	queryInput := &dynamodb.QueryInput{
		TableName:              aws.String(dt.tableName),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :pk_val", pkName)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk_val": &types.AttributeValueMemberS{Value: pk},
		},
		ExpressionAttributeNames: map[string]string{
			"#pk": pkName,
		},
	}

	if skName != nil && sk != nil {
		queryInput.KeyConditionExpression = aws.String(fmt.Sprintf("%s = :pk_val AND %s = :sk_val", pkName, *skName))
		queryInput.ExpressionAttributeValues[":sk_val"] = &types.AttributeValueMemberS{Value: *sk}
		queryInput.ExpressionAttributeNames["#sk"] = *skName
	} else if skName != nil && skRange != nil && len(*skRange) == 2 {
		queryInput.KeyConditionExpression = aws.String(fmt.Sprintf("%s = :pk_val AND %s BETWEEN :sk_start AND :sk_end", pkName, *skName))
		queryInput.ExpressionAttributeValues[":sk_start"] = &types.AttributeValueMemberS{Value: (*skRange)[0]}
		queryInput.ExpressionAttributeValues[":sk_end"] = &types.AttributeValueMemberS{Value: (*skRange)[1]}
		queryInput.ExpressionAttributeNames["#sk"] = *skName
	}

	if lsiName != nil && lsiValue != nil {
		queryInput.IndexName = aws.String(*lsiName)
		queryInput.KeyConditionExpression = aws.String(fmt.Sprintf("%s = :lsi_val", *lsiName))
		queryInput.ExpressionAttributeValues[":lsi_val"] = &types.AttributeValueMemberS{Value: *lsiValue}
		queryInput.ExpressionAttributeNames["#lsi"] = *lsiName // Assuming LSI name is also an attribute name
		if queryInput.KeyConditionExpression == nil {
			queryInput.KeyConditionExpression = aws.String(fmt.Sprintf("%s = :pk_val", pkName))
			queryInput.ExpressionAttributeValues = map[string]types.AttributeValue{
				":pk_val": &types.AttributeValueMemberS{Value: pk},
			}
			queryInput.ExpressionAttributeNames = map[string]string{
				"#pk": pkName,
			}
		}
		queryInput.KeyConditionExpression = aws.String(fmt.Sprintf("%s = :pk_val AND %s = :lsi_val", pkName, *lsiName))
		queryInput.ExpressionAttributeValues[":lsi_val"] = &types.AttributeValueMemberS{Value: *lsiValue}
		queryInput.ExpressionAttributeNames["#lsi"] = *lsiName
	}

	if len(projectedKeys) > 0 {
		queryInput.ProjectionExpression = aws.String(strings.Join(projectedKeys, ", "))
		for _, key := range projectedKeys {
			queryInput.ExpressionAttributeNames["#"+key] = key
		}
	}

	result, err := dt.client.Query(ctx, queryInput)
	if err != nil {
		return false, nil, fmt.Errorf("failed to query: %w", err)
	}

	return true, result.Items, nil
}

func (dt *DynamoTable) Scan(ctx context.Context, filterExpression string, expressionValues map[string]interface{}) (bool, []map[string]types.AttributeValue, error) {
	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(dt.tableName),
	}

	if filterExpression != "" {
		scanInput.FilterExpression = aws.String(filterExpression)
		expressionAttributeValues := make(map[string]types.AttributeValue)
		for key, val := range expressionValues {
			expressionAttributeValues[key] = attributeValueFromInterface(val)
		}
		scanInput.ExpressionAttributeValues = expressionAttributeValues
	}

	var items []map[string]types.AttributeValue
	var lastEvaluatedKey map[string]types.AttributeValue

	for {
		if lastEvaluatedKey != nil {
			scanInput.ExclusiveStartKey = lastEvaluatedKey
		}

		output, err := dt.client.Scan(ctx, scanInput)
		if err != nil {
			return false, nil, fmt.Errorf("failed to scan: %w", err)
		}

		items = append(items, output.Items...)

		if output.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = output.LastEvaluatedKey
	}

	return true, items, nil
}
