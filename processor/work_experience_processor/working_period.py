from datetime import timedelta


class WorkingPeriod:
    def __init__(self, start_date, end_date, counts_towards_exp, is_dismissal):
        self.start_date = start_date
        self.end_date = end_date
        self.counts_towards_exp = counts_towards_exp
        self.is_dismissal = is_dismissal

    def check_belonging(self, date):
        return self.start_date <= date < self.end_date

    def count_duration(self, override_end_date=None):
        if not self.counts_towards_exp:
            return 0.0

        end_date = (override_end_date or self.end_date) + timedelta(days=1)
        month_difference = (end_date.month - self.start_date.month) + (end_date.year - self.start_date.year) * 12
        return month_difference + (end_date.day - self.start_date.day) / 30.5

    def get_start_date(self):
        return self.start_date

    def get_end_date(self):
        return self.end_date

    def __repr__(self):
        return f"Counts to exp: {self.counts_towards_exp}, " \
               f"Is dismissal: {self.is_dismissal}, " \
               f"Start date: {self.start_date}, " \
               f"End date: {self.end_date}"
