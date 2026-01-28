package graphqlapi

import (
	"fmt"
	"strconv"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

// NewSchema builds the GraphQL schema for querying WeightChanged events.
func NewSchema(eventStore *store.Store) (graphql.Schema, error) {
	if eventStore == nil {
		return graphql.Schema{}, fmt.Errorf("store is required")
	}
	bigIntScalar := graphql.NewScalar(graphql.ScalarConfig{
		Name: "BigInt",
		Serialize: func(value interface{}) interface{} {
			switch v := value.(type) {
			case string:
				return v
			case fmt.Stringer:
				return v.String()
			case uint64:
				return strconv.FormatUint(v, 10)
			case int:
				return strconv.Itoa(v)
			default:
				return nil
			}
		},
		ParseValue: func(value interface{}) interface{} {
			switch v := value.(type) {
			case string:
				return v
			case int:
				return strconv.Itoa(v)
			case float64:
				return strconv.FormatInt(int64(v), 10)
			default:
				return nil
			}
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch v := valueAST.(type) {
			case *ast.StringValue:
				return v.Value
			case *ast.IntValue:
				return v.Value
			default:
				return nil
			}
		},
	})

	accountType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Account",
		Fields: graphql.Fields{
			"id": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
	})

	weightChangeEventType := graphql.NewObject(graphql.ObjectConfig{
		Name: "WeightChangeEvent",
		Fields: graphql.Fields{
			"account": {
				Type: graphql.NewNonNull(accountType),
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					event, ok := p.Source.(store.Event)
					if !ok {
						return nil, fmt.Errorf("unexpected source type")
					}
					return map[string]interface{}{"id": event.Account}, nil
				},
			},
			"previousWeight": {Type: graphql.NewNonNull(bigIntScalar)},
			"newWeight":      {Type: graphql.NewNonNull(bigIntScalar)},
			"blockNumber":    {Type: graphql.NewNonNull(bigIntScalar)},
		},
	})

	orderByEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "WeightChangeEventOrderBy",
		Values: graphql.EnumValueConfigMap{
			"blockNumber": &graphql.EnumValueConfig{Value: "blockNumber"},
		},
	})
	orderDirectionEnum := graphql.NewEnum(graphql.EnumConfig{
		Name: "OrderDirection",
		Values: graphql.EnumValueConfigMap{
			"asc":  &graphql.EnumValueConfig{Value: "asc"},
			"desc": &graphql.EnumValueConfig{Value: "desc"},
		},
	})

	query := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"weightChangeEvents": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(weightChangeEventType))),
				Args: graphql.FieldConfigArgument{
					"first":          &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"skip":           &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.Int)},
					"orderBy":        &graphql.ArgumentConfig{Type: orderByEnum},
					"orderDirection": &graphql.ArgumentConfig{Type: orderDirectionEnum},
				},
				Resolve: func(p graphql.ResolveParams) (interface{}, error) {
					first, _ := p.Args["first"].(int)
					skip, _ := p.Args["skip"].(int)
					orderBy, _ := p.Args["orderBy"].(string)
					orderDirection, _ := p.Args["orderDirection"].(string)
					return eventStore.ListEvents(p.Context, store.ListOptions{
						First:          first,
						Skip:           skip,
						OrderBy:        orderBy,
						OrderDirection: orderDirection,
					})
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{Query: query})
}
