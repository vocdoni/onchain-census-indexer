package graphqlapi

import (
	"context"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/vocdoni/davinci-node/db"
	"github.com/vocdoni/davinci-node/db/metadb"

	"github.com/vocdoni/onchain-census-indexer/internal/store"
)

const sampleQuery = `query GetWeightChangeEvents($first: Int!, $skip: Int!) {
    weightChangeEvents(
        first: $first
        skip: $skip
        orderBy: blockNumber
        orderDirection: asc
    ) {
        account {
            id
        }
        previousWeight
        newWeight
    }
}`

func TestSchemaQuery(t *testing.T) {
	ctx := context.Background()
	database, err := metadb.New(db.TypeInMem, "")
	if err != nil {
		t.Fatalf("create in-memory db: %v", err)
	}
	defer func() {
		if cerr := database.Close(); cerr != nil {
			t.Fatalf("close db: %v", cerr)
		}
	}()
	eventStore := store.New(database)

	events := []store.Event{
		{Account: "0xabc", PreviousWeight: "1", NewWeight: "2", BlockNumber: 1, LogIndex: 0},
		{Account: "0xdef", PreviousWeight: "2", NewWeight: "3", BlockNumber: 2, LogIndex: 0},
	}
	if err := eventStore.SaveEvents(ctx, events, 2); err != nil {
		t.Fatalf("save events: %v", err)
	}

	schema, err := NewSchema(eventStore)
	if err != nil {
		t.Fatalf("build schema: %v", err)
	}

	result := graphql.Do(graphql.Params{
		Schema:         schema,
		RequestString:  sampleQuery,
		VariableValues: map[string]interface{}{"first": 2, "skip": 0},
		Context:        ctx,
	})
	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors: %v", result.Errors)
	}

	data, ok := result.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected data type")
	}
	items, ok := data["weightChangeEvents"].([]interface{})
	if !ok {
		t.Fatalf("unexpected weightChangeEvents type")
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 events, got %d", len(items))
	}
	firstEvent, ok := items[0].(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected event type")
	}
	account, ok := firstEvent["account"].(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected account type")
	}
	if account["id"] != "0xabc" {
		t.Fatalf("expected account 0xabc, got %v", account["id"])
	}
	if firstEvent["previousWeight"] != "1" {
		t.Fatalf("expected previousWeight 1, got %v", firstEvent["previousWeight"])
	}
	if firstEvent["newWeight"] != "2" {
		t.Fatalf("expected newWeight 2, got %v", firstEvent["newWeight"])
	}
}
