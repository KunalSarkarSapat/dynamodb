package dynamodb_helper // v.1

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
}

func NewDynamoTable(tableName string, regionName string) (*DynamoTable, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(regionName))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	return &DynamoTable{
		client:    client,
		tableName: tableName,
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

func (dt *DynamoTable) BuildConditionExpression(attribute string, operator string, value interface{}) (string, map[string]types.AttributeValue, error) {
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
		TableName: aws.String(dt.tableName),
	}
	expressionAttributeValues := make(map[string]types.AttributeValue)
	expressionAttributeNames := make(map[string]string)
	keyConditionExpressionParts := []string{}

	expressionAttributeValues[":pk_val"] = attributeValueFromInterface(pk)
	expressionAttributeNames["#pk"] = pkName
	keyConditionExpressionParts = append(keyConditionExpressionParts, "#pk = :pk_val")

	if skName != nil && sk != nil {
		expressionAttributeValues[":sk_val"] = attributeValueFromInterface(*sk)
		expressionAttributeNames["#sk"] = *skName
		keyConditionExpressionParts = append(keyConditionExpressionParts, "#sk = :sk_val")
	} else if skName != nil && skRange != nil && len(*skRange) == 2 {
		expressionAttributeValues[":sk_start"] = attributeValueFromInterface((*skRange)[0])
		expressionAttributeValues[":sk_end"] = attributeValueFromInterface((*skRange)[1])
		expressionAttributeNames["#sk"] = *skName
		keyConditionExpressionParts = append(keyConditionExpressionParts, "#sk BETWEEN :sk_start AND :sk_end")
	}

	if lsiName != nil && lsiValue != nil {
		expressionAttributeValues[":lsi_val"] = attributeValueFromInterface(*lsiValue)
		expressionAttributeNames["#lsi"] = *lsiName
		keyConditionExpressionParts = append(keyConditionExpressionParts, "#lsi = :lsi_val")
		queryInput.IndexName = aws.String(*lsiName)
	}

	if len(keyConditionExpressionParts) > 0 {
		queryInput.KeyConditionExpression = aws.String(strings.Join(keyConditionExpressionParts, " AND "))
		queryInput.ExpressionAttributeValues = expressionAttributeValues
		queryInput.ExpressionAttributeNames = expressionAttributeNames
	} else {
		return false, nil, fmt.Errorf("no key condition provided")
	}

	if len(projectedKeys) > 0 {
		queryInput.ProjectionExpression = aws.String(strings.Join(projectedKeys, ", "))
		if queryInput.ExpressionAttributeNames == nil {
			queryInput.ExpressionAttributeNames = make(map[string]string)
		}
		for _, key := range projectedKeys {
			queryInput.ExpressionAttributeNames["#"+key] = key
		}
	}

	fmt.Println(ctx, queryInput)

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
		expressionAttributeNames := make(map[string]string)
		for key, val := range expressionValues {
			placeholder := fmt.Sprintf(":%s_val", strings.ReplaceAll(key, "#", ""))
			expressionAttributeValues[placeholder] = attributeValueFromInterface(val)
			expressionAttributeNames["#"+key] = key
		}
		scanInput.ExpressionAttributeValues = expressionAttributeValues
		scanInput.ExpressionAttributeNames = expressionAttributeNames
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
		scanInput.ExclusiveStartKey = nil // Reset for the next iteration
	}

	return true, items, nil
}

// Helper function to create AttributeValue from interface
func attributeValueFromInterface(v interface{}) types.AttributeValue {
	switch val := v.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: val}
	case int64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", val)}
	case int:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", val)}
	case float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", val)}
	case bool:
		return &types.AttributeValueMemberBOOL{Value: val}
	default:
		return &types.AttributeValueMemberS{Value: fmt.Sprintf("%v", v)}
	}
}
