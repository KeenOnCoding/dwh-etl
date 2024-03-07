from commons.utils.bases import WithConfig


class ConfigQueryRunner(WithConfig):
    def __init__(self, config, query_runner):
        WithConfig.__init__(self, config)
        self.query_runner = query_runner

