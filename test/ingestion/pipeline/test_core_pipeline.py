from ingestion.pipeline.core import Pipeline


def test_when_empty():
    pipeline = Pipeline()
    is_err, res = pipeline.invoke()

    assert is_err


def test_register_lambda():
    pipeline = Pipeline()
    ctx = {}
    key = 'some-key'
    value = 'some-value'

    pipeline.register_lambda(lambda payload, context: context.update({
        key: payload
    }))

    is_err, res = pipeline.invoke(payload=value, context=ctx)

    assert not is_err
    assert value == ctx[key]


def test_nested_pipelines():
    first_pipeline = Pipeline()
    expected_output = 10
    addition_lambda = (lambda x, y: x + 5)

    # populate with 0 value
    first_pipeline.register_lambda(lambda x, y: 0)

    second_pipeline = Pipeline()
    # register second pipeline as next operation of the first pipeline
    first_pipeline.register(second_pipeline)

    # register two addition lambdas in second pipeline
    second_pipeline.register_lambda(addition_lambda)
    second_pipeline.register_lambda(addition_lambda)

    is_err, actual_output = first_pipeline.invoke()

    assert not is_err
    assert actual_output == expected_output
