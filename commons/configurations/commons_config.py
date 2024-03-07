from configurations.utility import is_exist


class CommonsConfiguration:
    def __init__(self, shared=None, ingestion=None):
        self.shared = EnvironmentConfiguration(**is_exist(shared))
        self.ingestion = IngestionCommonsConfiguration(**is_exist(ingestion))


class EnvironmentConfiguration:
    def __init__(self, dwh_user=None, dwh_db=None, db_pass=None, dwh_host=None, env=None,
                 suppress_prometheus=None, build_number=None, build_url=None):
        self.dwh_user = dwh_user
        self.dwh_db = dwh_db
        self.db_pass = db_pass
        self.dwh_host = dwh_host
        self.env = env
        self.build_number = build_number
        self.build_url = build_url
        self.suppress_prometheus = suppress_prometheus


class IngestionCommonsConfiguration:
    def __init__(self, api_key=None, failed_job_metric=None,
                 in_total_metric=None, out_total_metric=None):
        self.api_key = api_key
        self.failed_job_metric = failed_job_metric
        self.in_total_metric = in_total_metric
        self.out_total_metric = out_total_metric
