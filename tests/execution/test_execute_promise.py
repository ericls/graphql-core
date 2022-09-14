from unittest.mock import MagicMock
from graphql.execution.promise import PromiseExecutionContext, execute_promise
from graphql.language import parse
from graphql.type import (
    GraphQLArgument,
    GraphQLField,
    GraphQLID,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLString,
)
from promise import Promise
from promise.dataloader import DataLoader


def describe_execute_promises_handles_promises_as_return_value_from_resolvers():
    def executes_query():

        stub = MagicMock()

        class AuthorNode:
            pass

        class AuthorLoader(DataLoader):
            def batch_load_fn(self, keys):
                stub(keys)
                return Promise.resolve(
                    [type(key, (AuthorNode,), {"id": key, "name": key}) for key in keys]
                )

        author_loader = AuthorLoader()

        Author = GraphQLObjectType(
            "Author",
            lambda: {
                "id": GraphQLField(GraphQLString, resolve=lambda obj, _info: obj.id),
                "name": GraphQLField(
                    GraphQLString, resolve=lambda obj, _info: obj.name
                ),
            },
        )

        # noinspection PyShadowingBuiltins
        BlogQuery = GraphQLObjectType(
            "Query",
            {
                "author": GraphQLField(
                    Author,
                    args={"id": GraphQLArgument(GraphQLID)},
                    resolve=lambda _obj, _info, id: author_loader.load(id),
                ),
            },
        )

        BlogSchema = GraphQLSchema(BlogQuery)

        document = parse(
            """
            query Foo {
              a: author(id: "1") {
                id,
                name,
              }
              b: author(id: "2") {
                id,
                name,
              }
              c: author(id: "2") {
                id,
                name,
              }
            }
            """
        )

        assert execute_promise(
            schema=BlogSchema,
            document=document,
            execution_context_class=PromiseExecutionContext,
        ) == (
            {
                "a": {"id": "1", "name": "1"},
                "b": {"id": "2", "name": "2"},
                "c": {"id": "2", "name": "2"},
            },
            None,
        )

        stub.assert_called_once_with(["1", "2"])
