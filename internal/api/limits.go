package api

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type operationLimits struct{}

func (operationLimits) ExtensionName() string                   { return "OperationLimits" }
func (operationLimits) Validate(graphql.ExecutableSchema) error { return nil }

func (operationLimits) MutateOperationParameters(_ context.Context, params *graphql.RawParams) *gqlerror.Error {
	if len(params.Query) > maxBodyBytes {
		return &gqlerror.Error{Message: "query is too large", Extensions: map[string]any{"code": "BAD_USER_INPUT"}}
	}
	return nil
}

func (operationLimits) MutateOperationContext(_ context.Context, op *graphql.OperationContext) *gqlerror.Error {
	// Expand fragment references iteratively. The visit budget also bounds
	// repeated fragment expansion before gqlgen calculates field complexity.
	type selection struct {
		set   ast.SelectionSet
		depth int
	}
	stack := []selection{{op.Operation.SelectionSet, 1}}
	visited := 0
	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, node := range item.set {
			visited++
			if item.depth > maxQueryDepth || visited > maxComplexity {
				return &gqlerror.Error{Message: "query exceeds maximum depth or field count", Extensions: map[string]any{"code": "BAD_USER_INPUT"}}
			}
			switch node := node.(type) {
			case *ast.Field:
				if len(node.SelectionSet) > 0 {
					stack = append(stack, selection{node.SelectionSet, item.depth + 1})
				}
			case *ast.InlineFragment:
				stack = append(stack, selection{node.SelectionSet, item.depth})
			case *ast.FragmentSpread:
				stack = append(stack, selection{node.Definition.SelectionSet, item.depth})
			}
		}
	}
	return nil
}
