from ingestion.query_params_builders.query_params_builder import QueryParamsBuilder


class EmptyQueryParamsBuilder(QueryParamsBuilder):
    def build(self, batch_context):
        return ''
