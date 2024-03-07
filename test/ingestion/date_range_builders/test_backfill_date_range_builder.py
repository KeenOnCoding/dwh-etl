from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
from dateutil.relativedelta import relativedelta

from commons.configurations.configurator import Configurator
from ingestion.constants import DEFAULT_DATETIME_PATTERN
from ingestion.date_range_builders.backfill_daterange_builder import BackfillIncrementalPayloadBuilder
from ingestion import constants


@pytest.fixture
def empty_runner():
    runner = MagicMock(name="empty-query-runner")
    runner.query_single_result.return_value = None
    return runner


@pytest.fixture
def base_config():
    cfg = Configurator().get_configurator()

    cfg.job.ingestion.fetching_strategy.date_column = 'date'
    cfg.job.ingestion.general.target_table = 'target'
    cfg.job.ingestion.fetching_strategy.batch_window = 0
    return cfg


def datetime_to_str(datetime_: datetime):
    return datetime_.strftime(DEFAULT_DATETIME_PATTERN)


def start_of_the_current_month_minus_timedelta(tmdelta=timedelta()):
    return datetime_to_str(datetime.now().replace(day=1) - tmdelta)


def now_minus_timedelta(tmdelta=timedelta()):
    return datetime_to_str(datetime.now() - tmdelta)


@pytest.mark.parametrize(
    'batch_window_unit_to_expected_ranges',
    [  # format:
        # batch_unit, start_date, expected_amount_of_generated_ranges, deviation
        (constants.MONTHS, start_of_the_current_month_minus_timedelta(timedelta(days=1)), 2, 0),
        (constants.DAYS, now_minus_timedelta(timedelta(days=1)), 2, 0),
        (constants.HOURS, now_minus_timedelta(timedelta(hours=3)), 4, 0),
        (constants.MINUTES, now_minus_timedelta(timedelta(minutes=10)), 11, 1),
        (constants.SECONDS, now_minus_timedelta(timedelta(seconds=60)), 61, 30)
    ])
def test_different_granularity_with_fixed_batch_window(batch_window_unit_to_expected_ranges,
                                                       empty_runner, base_config):
    batch_unit, default_partition_value, expected_ranges_amount, deviation = (
        batch_window_unit_to_expected_ranges
    )
    base_config.job.ingestion.fetching_strategy.granularity = batch_unit
    base_config.job.ingestion.fetching_strategy.default_partition_value = default_partition_value

    backfiller = BackfillIncrementalPayloadBuilder(base_config, query_runner=empty_runner)
    date_ranges = backfiller.build()
    actual_ranges_amount = len(date_ranges)
    assert abs(actual_ranges_amount - expected_ranges_amount) <= deviation


@pytest.mark.parametrize('batch_window_unit', [constants.MONTHS, constants.DAYS, constants.HOURS])
def test_no_range_to_backfill(batch_window_unit, base_config, empty_runner):
    default_date = datetime_to_str(datetime.now())
    base_config.job.ingestion.fetching_strategy.default_partition_value = default_date
    base_config.job.ingestion.fetching_strategy.batch_window_unit = batch_window_unit

    backfiller = BackfillIncrementalPayloadBuilder(base_config, query_runner=empty_runner)
    date_ranges = backfiller.build()
    actual_ranges_amount = len(date_ranges)
    expected_ranges_amount = 0
    assert expected_ranges_amount == actual_ranges_amount


@pytest.mark.parametrize('batch_window_unit',
                         [constants.MONTHS, constants.DAYS, constants.HOURS, constants.MINUTES, constants.SECONDS])
def test_when_negative_should_raise_error(
        batch_window_unit, base_config, empty_runner):

    base_config.job.ingestion.fetching_strategy.default_partition_value = now_minus_timedelta(timedelta(days=1))
    base_config.job.ingestion.fetching_strategy.batch_window = "-1"
    base_config.job.ingestion.fetching_strategy.batch_window_unit = batch_window_unit

    backfiller = BackfillIncrementalPayloadBuilder(base_config, query_runner=empty_runner)

    with pytest.raises(ValueError):
        backfiller.build()


def test_should_raise_when_no_default(base_config, empty_runner):
    with pytest.raises(ValueError):
        base_config.job.ingestion.fetching_strategy.default_partition_value = None
        BackfillIncrementalPayloadBuilder(base_config, empty_runner).build()


# only applicable when calculating amount of ranges when BATCH_WINDOW is set to 0
def amount_of_granularity_units(start: datetime, end: datetime, grano):
    if grano == constants.DAYS:
        return (end - start).days + 1  # add one because 24.02.2022 - 23.02.2022 = 1 day,
        # but range contains 24.02 and 23.02 ranges
    elif grano == constants.MONTHS:
        relative_delta = relativedelta(end, start)
        return relative_delta.years * 12 + relative_delta.months + 1
    elif grano == constants.HOURS:
        return int((end - start).total_seconds() / (60 * 60)) + 1


def test_backfilling_by_months_ranges(base_config, empty_runner):
    start_date = datetime.now() - relativedelta(years=1, months=5)
    backfill_start_date = start_date
    base_config.job.ingestion.fetching_strategy.default_partition_value = datetime_to_str(backfill_start_date)
    base_config.job.ingestion.fetching_strategy.granularity = constants.MONTHS

    now = datetime.now()

    expected_amount_of_ranges = amount_of_granularity_units(backfill_start_date, now, constants.MONTHS)
    ranges = BackfillIncrementalPayloadBuilder(base_config, empty_runner).build()
    actual_amount_of_ranges = len(ranges)

    assert actual_amount_of_ranges == expected_amount_of_ranges


def preset_relativedelta_with_granularity_unit(granno):
    if granno == constants.DAYS:
        return lambda x: relativedelta(days=x)
    elif granno == constants.MONTHS:
        return lambda x: relativedelta(months=x)
    elif granno == constants.HOURS:
        return lambda x: relativedelta(hours=x)
    else:
        raise ValueError('No such granularity')


@pytest.mark.parametrize('upper_offset', [0, -1, -0])
@pytest.mark.parametrize('batch_window_unit', [constants.MONTHS, constants.DAYS, constants.HOURS])
def test_backfill_future_ranges(batch_window_unit, upper_offset, base_config, empty_runner):
    start_date = datetime.now() - relativedelta(years=1, months=5)
    backfill_start_date = start_date

    base_config.job.ingestion.fetching_strategy.default_partition_value = datetime_to_str(backfill_start_date)
    base_config.job.ingestion.fetching_strategy.batch_window_unit = batch_window_unit
    base_config.job.ingestion.fetching_strategy.backfill_upper_offset = upper_offset

    # calculate the upper-bound range
    upper_bound = datetime.now() + preset_relativedelta_with_granularity_unit(batch_window_unit)(upper_offset)

    expected_amount_of_ranges = amount_of_granularity_units(backfill_start_date, upper_bound, batch_window_unit)
    ranges = BackfillIncrementalPayloadBuilder(base_config, empty_runner).build()
    actual_amount_of_ranges = len(ranges)

    assert actual_amount_of_ranges == expected_amount_of_ranges


def test_backfill_crop_to_now(base_config, empty_runner):
    start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta(days=3)
    backfill_start_date = start_date

    base_config.job.ingestion.fetching_strategy.default_partition_value = datetime_to_str(backfill_start_date)
    base_config.job.ingestion.fetching_strategy.granularity = 'minutes'
    base_config.job.ingestion.fetching_strategy.batch_window = 48
    base_config.job.ingestion.fetching_strategy.batch_window_unit = 'hours'
    base_config.job.ingestion.fetching_strategy.backfill_crop_now = True

    ranges = BackfillIncrementalPayloadBuilder(base_config, empty_runner).build()
    actual_amount_of_ranges = len(ranges)

    assert actual_amount_of_ranges == 2
