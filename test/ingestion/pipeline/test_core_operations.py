import pytest

from ingestion.pipeline.operation import core as op


@pytest.mark.parametrize('payload', [1, 'a', [], object()])
def test_wrap_in_list_operation(payload):
    operation = op.WrapInListOperation()
    is_err, res = operation.invoke(payload=payload)

    assert not is_err
    assert isinstance(res, list)


def test_append_list_operation():
    content = 'a'
    operation = op.AppendList(content)
    is_err, res = operation.invoke(payload=[])

    assert not is_err
    assert len(res) == 1
    assert res[0] == content


def test_set_object_operation_with_payload():
    expected_object = list()
    operation = op.SetObjectOperation(expected_object)
    is_err, actual_object = operation.invoke(payload=[])

    assert not is_err
    assert actual_object == expected_object


def test_set_object_operation_without_payload():
    expected_object = set()
    operation = op.SetObjectOperation(expected_object)
    is_err, actual_object = operation.invoke()

    assert not is_err
    assert actual_object == expected_object


def test_stash_to_context_operation():
    expected_passing_object = set()
    ctx = {}
    context_key = 'key'
    operation = op.StashToContextOperation(key=context_key)
    is_err, actual_passing_object = operation.invoke(payload=expected_passing_object, context=ctx)

    assert not is_err
    assert actual_passing_object == expected_passing_object
    assert ctx[context_key] == expected_passing_object


def test_stash_to_context_when_key_exists_returns_exception():
    ctx = {}
    ctx_key = 'key'
    operation_1 = op.StashToContextOperation(ctx_key)
    operation_2 = op.StashToContextOperation(ctx_key)

    operation_1.invoke(context=ctx)
    is_err, exception = operation_2.invoke(context=ctx)
    assert is_err
    assert isinstance(exception, RuntimeError)


def test_stash_to_context_when_key_exists_with_force():
    ctx = {}
    ctx_key = 'key'
    expected_in_context = object()
    operation_1 = op.StashToContextOperation(ctx_key)
    operation_2 = op.StashToContextOperation(ctx_key, force=True)

    operation_1.invoke(context=ctx)
    is_err, actual_result = operation_2.invoke(payload=expected_in_context, context=ctx)

    assert not is_err
    assert actual_result == expected_in_context
    assert ctx[ctx_key] == expected_in_context


def test_pop_from_context():
    ctx_key = 'key'
    expected_result = 'some-value'
    ctx = {
        ctx_key: expected_result
    }
    pop_op = op.PopFromContextOperation(ctx_key)
    is_err, actual_result = pop_op.invoke(payload=object(), context=ctx)

    assert not is_err
    assert actual_result == expected_result
