from prometheus_client import CollectorRegistry, Gauge, Counter
from prometheus_client import pushadd_to_gateway


class PrometheusManager:

    def __init__(self, port, labels, host='localhost', suppress=False):
        self.host = host
        self.port = str(port)

        self.labels_names = []
        self.labels_values = []
        self.suppress = suppress

        self.metrics = {
            'gauge': Gauge,
            'counter': Counter
        }

        self.labels_names, self.labels_values = zip(*labels) if labels else ((), ())

    def send_counter(self, job_name, name, description,
                     value, *, metric='gauge', opt_label=None):
        if self.suppress:
            return

        opt_labels_name, opt_labels_values = zip(*opt_label) if opt_label else ((), ())

        registry = CollectorRegistry()
        try:
            metric_cls = self.metrics[metric]
        except KeyError:
            raise ValueError(f'No such metric type found: {metric}.\n'
                             f'Allowed values: {tuple(self.metrics.keys())}')

        metric = metric_cls(name, description, self.labels_names + opt_labels_name, registry=registry)
        metric.labels(*(self.labels_values + opt_labels_values)).inc(value)

        pushadd_to_gateway(self.host + ':' + self.port,
                           job=job_name,
                           registry=registry,
                           grouping_key=dict(zip(self.labels_names
                                                 + opt_labels_name,
                                                 self.labels_values
                                                 + opt_labels_values)))
