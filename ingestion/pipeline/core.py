from abc import abstractmethod, ABC
from typing import List, Any, Union


class OperationException(Exception):
    pass


class Operation(ABC):
    ERR = True
    OK = False

    def invoke(self, *, payload=None, context=None) -> Union[bool, Any]:
        try:
            return Operation.OK, self._invoke(payload, context)
        except Exception as e:
            return Operation.ERR, e

    @abstractmethod
    def _invoke(self, payload, context) -> Any:
        pass


class AbstractPipeline(Operation, ABC):
    context = {}

    @abstractmethod
    def register(self, operation: Operation):
        pass

    def register_lambda(self, lambda_expr: callable):
        class AnonymousOperation(Operation):

            def _invoke(self, payload, context):
                return lambda_expr(payload, context)

        self.register(
            AnonymousOperation()
        )


class Pipeline(AbstractPipeline):

    def __init__(self):
        self.operations: List[Operation] = []

    def register(self, operation: Operation):
        self.operations.append(operation)
        return self

    def _invoke(self, payload, context=None):
        try:
            *tail, head = reversed(self.operations)
        except ValueError:
            raise
        status, result = self._invoke_op(head, payload, context)

        while tail and status is Operation.OK:
            operation = tail.pop()

            status, result = self._invoke_op(operation, result, context)

        if status is Operation.ERR:
            exc = result
            raise exc

        return result

    def _invoke_op(self, operation, payload, context):
        from collections.abc import Iterable

        status, result = operation.invoke(payload=payload, context=context)

        # the result might be iterable because pipeline chaining could be applied
        # the pipeline result generally is tuple of boolean status and the result
        # which is passed to the current pipeline result variable and should be unpacked
        if isinstance(result, Iterable) and not isinstance(result, str):  # ignore strings
            status, result = result

        return status, result
