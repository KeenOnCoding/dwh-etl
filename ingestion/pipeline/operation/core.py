from typing import List

from ingestion.pipeline.core import Operation


class WrapInListOperation(Operation):

    def _invoke(self, payload, context):
        return [payload]


class AppendList(Operation):
    def __init__(self, content):
        self.content = content

    def _invoke(self, payload: List, context):
        payload.append(self.content)
        return payload


class SetObjectOperation(Operation):
    def __init__(self, obj):
        self.obj = obj

    def _invoke(self, payload, context):
        return self.obj


class StashToContextOperation(Operation):
    def __init__(self, key, *, force=False):
        self.key = key
        self.force = force

    def _invoke(self, payload, context):
        if self.key in context and not self.force:
            raise RuntimeError(f'The context already contains key: {self.key}.'
                               f' Use force flag to overwrite existing value')

        context[self.key] = payload
        return payload


class PopFromContextOperation(Operation):
    def __init__(self, key):
        self.key = key

    def _invoke(self, payload, context):
        return context.pop(self.key)
