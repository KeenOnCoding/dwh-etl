class DataSource:
    def __init__(self, query_runner):
        self.query_runner = query_runner

    def extract_data(self, data_source, end_date):
        return self.query_runner.query(f"""
            SELECT employee_id, status, date FROM {data_source}
            WHERE date <= '{end_date.date()}'
        """)
