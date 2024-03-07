from typing import Mapping


class GenericStatManager:

    def __init__(self, prometheus_manager, config: Mapping):
        self.prometheus_manager = prometheus_manager
        self.job_name = config.job.ingestion.general.job_name
        self.failed_job_metric = config.environment.ingestion.failed_job_metric
        self.in_total_metric = config.environment.ingestion.in_total_metric
        self.out_total_metric = config.environment.ingestion.out_total_metric

    def handle_numerical_statistics(self, rec_in, rec_out, has_err):
        self.prometheus_manager.send_counter(self.job_name,
                                             self.failed_job_metric,
                                             'Shows if job failed on ingestion',
                                             1 if has_err else 0)
        if not has_err:
            self.prometheus_manager.send_counter(self.job_name,
                                                 self.in_total_metric,
                                                 'Number of input rows on ingestion',
                                                 rec_in)
            self.prometheus_manager.send_counter(self.job_name,
                                                 self.out_total_metric,
                                                 'Number of output rows on ingestion. Is not consistent',
                                                 rec_out)
