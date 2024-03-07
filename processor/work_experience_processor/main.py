from dateutil import relativedelta
from operator import itemgetter

from commons.logging.simple_logger import logger
from processor.work_experience_processor.const import WORK_STATUSES, INTERNSHIP_STATUSES, DISMISSAL_STATUS, \
    EXTENDED_VACATION_STATUS
from processor.work_experience_processor.time_interval import TimeInterval
from processor.work_experience_processor.working_period import WorkingPeriod


class WorkExperienceRunner:
    def __init__(self, config, engine, query_runner, datamanager, datasource):
        self.query_runner = query_runner
        self.config = config
        self.engine = engine
        self.datamanager = datamanager
        self.datasource = datasource
        self.source_table = config.job.processing.work_experience_processor.work_experience_source_table
        self.target_table = config.job.processing.work_experience_processor.work_experience_target_table

    def run(self):
        logger.info(f"Using source table {self.source_table}, target table {self.target_table}")

        time = TimeInterval()

        interval = time.get_interval_to_fetch(self.query_runner, self.source_table, self.target_table)
        dates = time.get_ranges(*interval)

        data = self.datasource.extract_data(self.source_table, interval[1])
        logger.info(f"Extracted {len(data)} record")

        mapped_data = self.obj_mapper(data)
        experience = self.count_work_experience(mapped_data, dates)

        temporary_target_table = self.datamanager.create_temporary_table(self.target_table)

        for exp in experience:
            self.datamanager.insert_into_table(temporary_target_table, exp)

        self.datamanager.copy_data_to_table(self.target_table, temporary_target_table, *interval)
        self.datamanager.remove_temporary_table()

    def count_work_experience(self, data, dates):
        result = list()
        for employee_id, employee_history in data.items():
            logger.info(f"Counting work experience for {employee_id}")
            start_count_date = self.get_start_date(employee_history)
            periods = self.define_periods(employee_history)
            merged_periods = self.merge_periods(periods, start_count_date)
            logger.info(f"Status history from start to fetch year: {merged_periods}")
            for date in dates:
                work_experience = int(self.count_day_by_period(merged_periods, date, start_count_date))
                if work_experience >= 0:
                    result.append((employee_id, date.strftime('%Y-%m-%d %H:%M:%S'),
                                   work_experience, date.strftime('%Y-%m-%d')))
        return result

    def count_day_by_period(self, periods, date, start_count_date):
        invest = list()
        is_dismissal = False

        if (periods and date < periods[0].get_start_date()) or date < start_count_date:
            return -1

        for period in periods:
            experience = period.count_duration()

            if period.get_start_date() < start_count_date or date < start_count_date:
                continue

            if period.is_dismissal and date.replace(day=1) > period.get_start_date().replace(day=1):
                invest = list()

            if period.check_belonging(date):
                experience = period.count_duration(date)
                invest.append(experience)
                if period.is_dismissal and date.replace(day=1) > period.get_start_date().replace(day=1):
                    is_dismissal = True
                break
            invest.append(experience)
        work_experience_days = sum(invest)

        if is_dismissal:
            return -1.0
        return work_experience_days

    def define_periods(self, employee_history):
        employee_history = sorted(employee_history, key=itemgetter('date'))
        periods = list()
        for i in range(0, len(employee_history)):
            status = employee_history[i]["status"]
            start_date = employee_history[i]['date']
            if i == len(employee_history) - 1:
                end_date = TimeInterval.get_current_date()
            else:
                end_date = employee_history[i + 1]['date']
                if employee_history[i + 1]['status'] != DISMISSAL_STATUS:
                    end_date -= relativedelta.relativedelta(days=1)

            periods.append((status, start_date, end_date))
        return periods

    def merge_periods(self, periods, start_count_date):
        merged = list()
        status, start, end = periods[0]
        counts_to_exp = bool(status in WORK_STATUSES and start >= start_count_date)

        if len(periods) == 1:
            merged.append(WorkingPeriod(start, end, counts_to_exp, status == DISMISSAL_STATUS))

        for period in range(1, len(periods)):
            next_status, next_start, next_end = periods[period]
            next_counts_to_exp = bool(next_status in WORK_STATUSES and next_start >= start_count_date)
            if next_counts_to_exp != counts_to_exp:
                merged.append(WorkingPeriod(start, end, counts_to_exp, status == DISMISSAL_STATUS))
                status, start, end = periods[period]
                counts_to_exp = next_counts_to_exp
            else:
                end = next_end
            if period == len(periods) - 1:
                merged.append(WorkingPeriod(start, end, counts_to_exp, status == DISMISSAL_STATUS))
        return merged

    def get_start_date(self, employee: dict):
        try:
            min_date = min([
                e['date']
                for e in employee
                if e['status'] not in [*INTERNSHIP_STATUSES, DISMISSAL_STATUS, EXTENDED_VACATION_STATUS]
            ])
            return min_date
        except ValueError:
            logger.error(f"Minimum countable date not found")
            return TimeInterval.get_current_date()

    def obj_mapper(self, data):
        grouped_dict = dict()
        for item in data:
            grouped_dict.setdefault(item['employee_id'], []).append(item)
        return grouped_dict
