from functools import partial
from typing import Any, Dict, List, Optional, Type, TypeVar, Union, cast

import promise
from promise import Promise

from graphql.error import located_error
from graphql.error.graphql_error import GraphQLError
from graphql.execution.middleware import GraphQLFieldResolver
from graphql.execution.values import get_argument_values
from graphql.language.ast import DocumentNode, FieldNode
from graphql.pyutils.path import Path
from graphql.pyutils.undefined import Undefined
from graphql.type.definition import GraphQLObjectType, GraphQLTypeResolver
from graphql.type.schema import GraphQLSchema

from .execute import (
    ExecutionContext,
    ExecutionResult,
    Middleware,
    assert_valid_execution_arguments,
    get_field_def,
)

T = TypeVar("T")
PromiseOrValue = Union[Promise[T], T]


class PromiseExecutionContext(ExecutionContext):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_awaitable = self.is_promise = promise.is_thenable

    def execute_fields_serially(
        self,
        parent_type: GraphQLObjectType,
        source_value: Any,
        path: Optional[Path],
        fields: Dict[str, List[FieldNode]],
    ) -> PromiseOrValue[Dict[str, Any]]:
        results: PromiseOrValue[Dict[str, Any]] = {}
        is_promise = self.is_promise
        for response_name, field_nodes in fields.items():
            field_path = Path(path, response_name, parent_type.name)
            result = self.execute_field(
                parent_type, source_value, field_nodes, field_path
            )
            if result is Undefined:
                continue
            if is_promise(results):
                # noinspection PyShadowingNames
                def await_and_set_result(
                    results: Promise[Dict[str, Any]],
                    response_name: str,
                    result: PromiseOrValue[Any],
                ) -> Promise[Dict[str, Any]]:
                    def handle_results(resolved_results):
                        if is_promise(result):
                            def on_resolve(v):
                                resolved_results[response_name] = v
                                return resolved_results
                            return result.then(on_resolve)
                        resolved_results[response_name] = result
                        return resolved_results

                    results.then(handle_results)
                    return results

                results = await_and_set_result(
                    cast(Promise, results), response_name, result
                )
            elif is_promise(result):
                # noinspection PyShadowingNames
                def set_result(
                    results: Dict[str, Any],
                    response_name: str,
                    result: Promise,
                ) -> Promise[Dict[str, Any]]:
                    def on_resolve(v):
                        results[response_name] = v
                        return results

                    return result.then(on_resolve)

                results = set_result(
                    cast(Dict[str, Any], results), response_name, result
                )
            else:
                cast(Dict[str, Any], results)[response_name] = result
        return results

    def execute_field(
        self,
        parent_type: GraphQLObjectType,
        source: Any,
        field_nodes: List[FieldNode],
        path: Path,
    ) -> PromiseOrValue[Any]:
        """Resolve the field on the given source object.

        Implements the "Executing fields" section of the spec.

        In particular, this method figures out the value that the field returns by
        calling its resolve function, then calls complete_value to await coroutine
        objects, serialize scalars, or execute the sub-selection-set for objects.
        """
        field_def = get_field_def(self.schema, parent_type, field_nodes[0])
        if not field_def:
            return Undefined

        return_type = field_def.type
        resolve_fn = field_def.resolve or self.field_resolver

        if self.middleware_manager:
            resolve_fn = self.middleware_manager.get_field_resolver(resolve_fn)

        info = self.build_resolve_info(field_def, field_nodes, parent_type, path)

        # Get the resolve function, regardless of if its result is normal or abrupt
        # (error).
        try:
            # Build a dictionary of arguments from the field.arguments AST, using the
            # variables scope to fulfill any variable references.
            args = get_argument_values(field_def, field_nodes[0], self.variable_values)

            # Note that contrary to the JavaScript implementation, we pass the context
            # value as part of the resolve info.
            result = resolve_fn(source, info, **args)

            if self.is_promise(result):
                result: Promise = result
                # noinspection PyShadowingNames
                def await_result() -> Any:
                    def handle_error(raw_error):
                        error = located_error(raw_error, field_nodes, path.as_list())
                        self.handle_field_error(error, return_type)

                    p = result.then(
                        partial(
                            self.complete_value, return_type, field_nodes, info, path
                        ),
                        handle_error,
                    )
                    return p

                return await_result()

            completed = self.complete_value(
                return_type, field_nodes, info, path, result
            )
            if self.is_promise(completed):
                # noinspection PyShadowingNames
                def await_completed() -> Any:
                    def handle_error(raw_error):
                        error = located_error(raw_error, field_nodes, path.as_list())
                        self.handle_field_error(error, return_type)

                    p = completed.then(lambda v: v, handle_error)
                    return p

                return await_completed()

            return completed
        except Exception as raw_error:
            error = located_error(raw_error, field_nodes, path.as_list())
            self.handle_field_error(error, return_type)
            return None

    def execute_fields(
        self,
        parent_type: GraphQLObjectType,
        source_value: Any,
        path: Optional[Path],
        fields: Dict[str, List[FieldNode]],
    ) -> PromiseOrValue[Dict[str, Any]]:
        """Execute the given fields concurrently.

        Implements the "Executing selection sets" section of the spec
        for fields that may be executed in parallel.
        """
        results = {}
        is_promise = self.is_promise
        awaitable_fields: List[str] = []
        append_awaitable = awaitable_fields.append
        for response_name, field_nodes in fields.items():
            field_path = Path(path, response_name, parent_type.name)
            result = self.execute_field(
                parent_type, source_value, field_nodes, field_path
            )
            if result is not Undefined:
                results[response_name] = result
                if is_promise(result):
                    append_awaitable(response_name)

        if not awaitable_fields:
            return results

        def get_results() -> Dict[str, Any]:
            def on_all_resolve(resolved_results: List[Any]):
                for field, result in zip(awaitable_fields, resolved_results):
                    results[field] = result
                return results

            p = Promise.all([results[field] for field in awaitable_fields]).then(
                on_all_resolve
            )
            return p

        return get_results()


def _execute_promise(
    schema: GraphQLSchema,
    document: DocumentNode,
    root_value: Any = None,
    context_value: Any = None,
    variable_values: Optional[Dict[str, Any]] = None,
    operation_name: Optional[str] = None,
    field_resolver: Optional[GraphQLFieldResolver] = None,
    type_resolver: Optional[GraphQLTypeResolver] = None,
    subscribe_field_resolver: Optional[GraphQLFieldResolver] = None,
    middleware: Optional[Middleware] = None,
    execution_context_class: Optional[Type["PromiseExecutionContext"]] = None,
) -> PromiseOrValue[ExecutionResult]:
    """Execute a GraphQL operation.

    Implements the "Executing requests" section of the GraphQL specification.

    Returns an ExecutionResult (if all encountered resolvers are synchronous),
    or a coroutine object eventually yielding an ExecutionResult.

    If the arguments to this function do not result in a legal execution context,
    a GraphQLError will be thrown immediately explaining the invalid input.
    """
    # If arguments are missing or incorrect, throw an error.
    assert_valid_execution_arguments(schema, document, variable_values)

    if execution_context_class is None:
        execution_context_class = PromiseExecutionContext

    # If a valid execution context cannot be created due to incorrect arguments,
    # a "Response" with only errors is returned.
    exe_context: PromiseExecutionContext = execution_context_class.build(
        schema,
        document,
        root_value,
        context_value,
        variable_values,
        operation_name,
        field_resolver,
        type_resolver,
        subscribe_field_resolver,
        middleware,
        promise.is_thenable,
    )

    # Return early errors if execution context failed.
    if isinstance(exe_context, list):
        return ExecutionResult(data=None, errors=exe_context)

    errors = exe_context.errors
    build_response = exe_context.build_response
    operation = exe_context.operation
    try:
        operation = exe_context.operation
        result = exe_context.execute_operation(operation, root_value)

        if exe_context.is_awaitable(result):
            return result.then(
                lambda r: build_response(r, errors),
                lambda e: (not errors.append(e)) and build_response(None, errors),
            )
    except GraphQLError as error:
        errors.append(error)
        return build_response(None, errors)
    else:
        return build_response(result, errors)  # type: ignore


def execute_promise(*args, **kwargs):
    # Gotta make sure there's a queue before calling any other promises
    p = Promise.resolve(None).then(lambda _: _execute_promise(*args, **kwargs))
    return p.get()
