from functools import partial
from typing import (
    Any,
    AsyncIterable,
    Dict,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

import promise
from promise import Promise

from graphql.error import located_error
from graphql.error.graphql_error import GraphQLError
from graphql.execution.middleware import GraphQLFieldResolver
from graphql.execution.values import get_argument_values
from graphql.language.ast import DocumentNode, FieldNode
from graphql.language.parser import parse
from graphql.pyutils import inspect, is_iterable
from graphql.pyutils.path import Path
from graphql.pyutils.undefined import Undefined
from graphql.type.definition import (
    GraphQLAbstractType,
    GraphQLLeafType,
    GraphQLList,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLOutputType,
    GraphQLResolveInfo,
    GraphQLTypeResolver,
    is_abstract_type,
    is_leaf_type,
    is_list_type,
    is_non_null_type,
    is_object_type,
)
from graphql.type.schema import GraphQLSchema
from graphql.type.validate import validate_schema

from .execute import (
    ExecutionContext,
    ExecutionResult,
    Middleware,
    assert_valid_execution_arguments,
    get_field_def,
    invalid_return_type_error,
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

    def complete_object_value(
        self,
        return_type: GraphQLObjectType,
        field_nodes: List[FieldNode],
        info: GraphQLResolveInfo,
        path: Path,
        result: Any,
    ) -> PromiseOrValue[Dict[str, Any]]:
        """Complete an Object value by executing all sub-selections."""
        # Collect sub-fields to execute to complete this value.
        sub_field_nodes = self.collect_subfields(return_type, field_nodes)

        # If there is an `is_type_of()` predicate function, call it with the current
        # result. If `is_type_of()` returns False, then raise an error rather than
        #  continuing execution.
        if return_type.is_type_of:
            is_type_of = return_type.is_type_of(result, info)

            if self.is_promise(is_type_of):

                def execute_subfields_async() -> Dict[str, Any]:
                    is_type_of = cast(Promise, is_type_of)

                    def on_is_type_of_resolve(v):
                        if not v:
                            raise (
                                invalid_return_type_error(
                                    return_type, result, field_nodes
                                )
                            )
                        else:
                            execute_result = self.execute_fields(
                                return_type, result, path, sub_field_nodes
                            )
                            execute_result.then(lambda v: promise.resolve(v))

                    return is_type_of.then(on_is_type_of_resolve)

                return execute_subfields_async()

            if not is_type_of:
                raise invalid_return_type_error(return_type, result, field_nodes)

        return self.execute_fields(return_type, result, path, sub_field_nodes)

    def complete_value(
        self,
        return_type: GraphQLOutputType,
        field_nodes: List[FieldNode],
        info: GraphQLResolveInfo,
        path: Path,
        result: Any,
    ) -> PromiseOrValue[Any]:
        """Complete a value.

        Implements the instructions for completeValue as defined in the
        "Value completion" section of the spec.

        If the field type is Non-Null, then this recursively completes the value
        for the inner type. It throws a field error if that completion returns null,
        as per the "Nullability" section of the spec.

        If the field type is a List, then this recursively completes the value
        for the inner type on each item in the list.

        If the field type is a Scalar or Enum, ensures the completed value is a legal
        value of the type by calling the ``serialize`` method of GraphQL type
        definition.

        If the field is an abstract type, determine the runtime type of the value and
        then complete based on that type.

        Otherwise, the field type expects a sub-selection set, and will complete the
        value by evaluating all sub-selections.
        """
        # If result is an Exception, throw a located error.
        if isinstance(result, Exception):
            raise result

        # If field type is NonNull, complete for inner type, and throw field error if
        # result is null.
        if is_non_null_type(return_type):
            completed = self.complete_value(
                cast(GraphQLNonNull, return_type).of_type,
                field_nodes,
                info,
                path,
                result,
            )
            if completed is None:
                raise TypeError(
                    "Cannot return null for non-nullable field"
                    f" {info.parent_type.name}.{info.field_name}."
                )
            return completed

        # If result value is null or undefined then return null.
        if result is None or result is Undefined:
            return None

        # If field type is List, complete each item in the list with inner type
        if is_list_type(return_type):
            return self.complete_list_value(
                cast(GraphQLList, return_type), field_nodes, info, path, result
            )

        # If field type is a leaf type, Scalar or Enum, serialize to a valid value,
        # returning null if serialization is not possible.
        if is_leaf_type(return_type):
            return self.complete_leaf_value(cast(GraphQLLeafType, return_type), result)

        # If field type is an abstract type, Interface or Union, determine the runtime
        # Object type and complete for that type.
        if is_abstract_type(return_type):
            return self.complete_abstract_value(
                cast(GraphQLAbstractType, return_type), field_nodes, info, path, result
            )

        # If field type is Object, execute and complete all sub-selections.
        if is_object_type(return_type):
            return self.complete_object_value(
                cast(GraphQLObjectType, return_type), field_nodes, info, path, result
            )

        # Not reachable. All possible output types have been considered.
        raise TypeError(  # pragma: no cover
            "Cannot complete value of unexpected output type:"
            f" '{inspect(return_type)}'."
        )

    def complete_list_value(
        self,
        return_type: GraphQLList[GraphQLOutputType],
        field_nodes: List[FieldNode],
        info: GraphQLResolveInfo,
        path: Path,
        result: Union[Iterable[Any], Iterable[Promise[Any]]],
    ) -> PromiseOrValue[List[Any]]:
        """Complete a list value.

        Complete a list value by completing each item in the list with the inner type.
        """
        if not is_iterable(result):
            # experimental: allow async iterables
            if isinstance(result, AsyncIterable):
                # This should never happen in a promise context
                raise Exception("what's going on?")
                # noinspection PyShadowingNames
                async def async_iterable_to_list(
                    async_result: AsyncIterable[Any],
                ) -> Any:
                    sync_result = [item async for item in async_result]
                    return self.complete_list_value(
                        return_type, field_nodes, info, path, sync_result
                    )

                return async_iterable_to_list(result)

            raise GraphQLError(
                "Expected Iterable, but did not find one for field"
                f" '{info.parent_type.name}.{info.field_name}'."
            )
        result = cast(Iterable[Any], result)

        # This is specified as a simple map, however we're optimizing the path where
        # the list contains no coroutine objects by avoiding creating another coroutine
        # object.
        item_type = return_type.of_type
        is_promise = self.is_promise
        awaitable_indices: List[int] = []
        append_awaitable = awaitable_indices.append
        completed_results: List[Any] = []
        append_result = completed_results.append
        for index, item in enumerate(result):
            # No need to modify the info object containing the path, since from here on
            # it is not ever accessed by resolver functions.
            item_path = path.add_key(index, None)
            completed_item: PromiseOrValue[Any]
            if is_promise(item):
                # noinspection PyShadowingNames
                def await_completed(item: Promise[Any], item_path: Path) -> Any:
                    try:

                        def on_item_resolve(item_value):
                            completed = self.complete_value(
                                item_type, field_nodes, info, item_path, item_value
                            )
                            return completed

                        return item.then(on_item_resolve)
                    except Exception as raw_error:
                        error = located_error(
                            raw_error, field_nodes, item_path.as_list()
                        )
                        self.handle_field_error(error, item_type)
                        return None

                completed_item = await_completed(item, item_path)
            else:
                try:
                    completed_item = self.complete_value(
                        item_type, field_nodes, info, item_path, item
                    )
                    if is_promise(completed_item):
                        # noinspection PyShadowingNames
                        def await_completed(item: Promise[Any], item_path: Path) -> Any:
                            def on_error(raw_error):
                                error = located_error(
                                    raw_error, field_nodes, item_path.as_list()
                                )
                                self.handle_field_error(error, item_type)

                            return item.catch(on_error)

                        completed_item = await_completed(completed_item, item_path)
                except Exception as raw_error:
                    error = located_error(raw_error, field_nodes, item_path.as_list())
                    self.handle_field_error(error, item_type)
                    completed_item = None

            if is_promise(completed_item):
                append_awaitable(index)
            append_result(completed_item)

        if not awaitable_indices:
            return completed_results

        # noinspection PyShadowingNames
        def get_completed_results() -> List[Any]:
            def on_single_complte(index, result):
                completed_results[index] = result

            promises = [
                completed_results[index].then(partial(on_single_complte, index))
                for index in awaitable_indices
            ]
            return Promise.all(promises).then(lambda _: completed_results)

        res = get_completed_results()
        return res


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


def graphql_impl_promise(
    schema,
    source,
    root_value,
    context_value,
    variable_values,
    operation_name,
    field_resolver,
    type_resolver,
    middleware,
    execution_context_class,
    is_awaitable=None,
):
    """Execute a query, return asynchronously only if necessary."""
    # Validate Schema
    schema_validation_errors = validate_schema(schema)
    if schema_validation_errors:
        return ExecutionResult(data=None, errors=schema_validation_errors)

    # Parse
    try:
        document = parse(source)
    except GraphQLError as error:
        return ExecutionResult(data=None, errors=[error])

    # Validate
    from graphql.validation import validate

    validation_errors = validate(schema, document)
    if validation_errors:
        return ExecutionResult(data=None, errors=validation_errors)

    # Execute
    return execute_promise(
        schema,
        document,
        root_value,
        context_value,
        variable_values,
        operation_name,
        field_resolver,
        type_resolver,
        None,
        middleware,
        execution_context_class,
    )


def graphql_promise(
    schema,
    source,
    root_value,
    context_value,
    variable_values=None,
    operation_name=None,
    field_resolver=None,
    type_resolver=None,
    middleware=None,
    execution_context_class=None,
    check_sync=False,
) -> ExecutionResult:
    """Execute a GraphQL operation synchronously.

    The graphql_sync function also fulfills GraphQL operations by parsing, validating,
    and executing a GraphQL document along side a GraphQL schema. However, it guarantees
    to complete synchronously (or throw an error) assuming that all field resolvers
    are also synchronous.

    Set check_sync to True to still run checks that no awaitable values are returned.
    """
    result = graphql_impl_promise(
        schema,
        source,
        root_value,
        context_value,
        variable_values,
        operation_name,
        field_resolver,
        type_resolver,
        middleware,
        execution_context_class,
    )

    return cast(ExecutionResult, result)
