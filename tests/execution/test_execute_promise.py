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
        stub2 = MagicMock()

        class AuthorNode:
            pass

        class AuthorLoader(DataLoader):
            def batch_load_fn(self, keys):
                stub(keys)
                return Promise.resolve(
                    [type(key, (AuthorNode,), {"id": key}) for key in keys]
                )

        class NameLoader(DataLoader):
            def batch_load_fn(self, keys):
                stub2(keys)
                return Promise.resolve([key + " name" for key in keys])

        author_loader = AuthorLoader()
        name_loader = NameLoader()

        Author = GraphQLObjectType(
            "Author",
            lambda: {
                "id": GraphQLField(GraphQLString, resolve=lambda obj, _info: obj.id),
                "name": GraphQLField(
                    GraphQLString, resolve=lambda obj, _info: name_loader.load(obj.id)
                ),
            },
        )

        # noinspection PyShadowingBuiltins
        TestQuery = GraphQLObjectType(
            "Query",
            {
                "author": GraphQLField(
                    Author,
                    args={"id": GraphQLArgument(GraphQLID)},
                    resolve=lambda _obj, _info, id: author_loader.load(id),
                ),
            },
        )
        TestMutation = GraphQLObjectType(
            "Mutation",
            {
                "newAuthor": GraphQLField(
                    Author,
                    args={"id": GraphQLArgument(GraphQLString)},
                    resolve=lambda obj, info, id: author_loader.load(id),
                )
            },
        )

        TestSchema = GraphQLSchema(TestQuery, TestMutation)

        foo_query = parse(
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
            schema=TestSchema,
            document=foo_query,
            execution_context_class=PromiseExecutionContext,
        ) == (
            {
                "a": {"id": "1", "name": "1 name"},
                "b": {"id": "2", "name": "2 name"},
                "c": {"id": "2", "name": "2 name"},
            },
            None,
        )

        stub.assert_called_once_with(["1", "2"])
        stub2.assert_called_once_with(["1", "2"])
        stub.reset_mock()
        stub2.reset_mock()

        foo_mutation = parse(
            """
            mutation Foo {
                a: newAuthor(id: "3") {
                    id
                    name
                }
                b: newAuthor(id: "4") {
                    id
                    name
                }
            }
        """
        )

        assert execute_promise(
            schema=TestSchema,
            document=foo_mutation,
            execution_context_class=PromiseExecutionContext,
        ) == (
            {
                "a": {"id": "3", "name": "3 name"},
                "b": {"id": "4", "name": "4 name"}
            },
            None,
        )

        stub.assert_called_once_with(["3", "4"])
        stub2.assert_called_once_with(["3", "4"])
