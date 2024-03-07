Feature: python daily processor

  Scenario: run a non-overtime two days report processing
    Given table "dbo.daily_split"
    And table "dbo.no_split"
    And table "dbo.working_hours"
    And table "dbo.no_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted |
      | 1  | 1        | 10.0   | '2021-09-16 00:00:00' | '2021-09-17 00:00:00' | 0        | 'Accepted' | 0       |
    And table "dbo.daily_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 0        | 8.0    | '2021-08-16 00:00:00' | '2021-08-16 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-16' | '2021-08-16'  |
    And table "dbo.working_hours" contains following records
      | calendar_id                            | date                  | working_hours |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-16 00:00:00' | 8             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-17 00:00:00' | 8             |
    When I run the job
    Then table "dbo.no_split" contains "1" row
    And table "dbo.daily_split" contains "3" row
    And table "dbo.daily_split" contains
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 0        | 8.0    | '2021-08-16 00:00:00' | '2021-08-16 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-16' | '2021-08-16'  |
      | 1  | 1        | 5.0    | '2021-09-16 00:00:00' | '2021-09-16 00:00:00' | 0        | 'Accepted' | 0       | '2021-09-16' | '2021-09-16'  |
      | 1  | 1        | 5.0    | '2021-09-17 00:00:00' | '2021-09-17 00:00:00' | 0        | 'Accepted' | 0       | '2021-09-17' | '2021-09-17'  |

  Scenario: run overtime two days report processing
    Given table "dbo.daily_split"
    And table "dbo.no_split"
    And table "dbo.working_hours"
    And table "dbo.no_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted |
      | 1  | 1        | 10.0   | '2021-09-14 00:00:00' | '2021-09-15 00:00:00' | 1        | 'Accepted' | 0       |
    And table "dbo.daily_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 0        | 10.0   | '2021-08-14 00:00:00' | '2021-08-15 00:00:00' | 1        | 'Accepted' | 0       | '2021-08-14' | '2021-08-15'  |
    And table "dbo.working_hours" contains following records
      | calendar_id                            | date                  | working_hours |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-14 00:00:00' | 8             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-15 00:00:00' | 8             |
    When I run the job
    Then table "dbo.no_split" contains "1" row
    And table "dbo.daily_split" contains "2" row
    And table "dbo.daily_split" contains
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 0        | 10.0   | '2021-08-14 00:00:00' | '2021-08-15 00:00:00' | 1        | 'Accepted' | 0       | '2021-08-14' | '2021-08-15'  |
      | 1  | 1        | 10.0   | '2021-09-14 00:00:00' | '2021-09-15 00:00:00' | 1        | 'Accepted' | 0       | '2021-09-14' | '2021-09-15'  |

  Scenario: should not process not accepted records
    Given table "dbo.daily_split"
    And table "dbo.no_split"
    And table "dbo.working_hours"
    And table "dbo.no_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted |
      | 2  | 2        | 10.0   | '2021-09-14 00:00:00' | '2021-09-15 00:00:00' | 0        | 'Accepted' | 0       |
      | 3  | 3        | 8.0    | '2021-09-14 00:00:00' | '2021-09-14 00:00:00' | 0        | 'None'     | 0       |
    And table "dbo.daily_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 1  | 1        | 5.0    | '2021-08-14 00:00:00' | '2021-08-14 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-14' | '2021-08-14'  |
      | 1  | 1        | 5.0    | '2021-08-15 00:00:00' | '2021-08-15 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-15' | '2021-08-15'  |
    And table "dbo.working_hours" contains following records
      | calendar_id                            | date                  | working_hours |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-14 00:00:00' | 8             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-15 00:00:00' | 8             |
    When I run the job
    Then table "dbo.daily_split" contains "4" row
    And table "dbo.daily_split" contains
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 1  | 1        | 5.0    | '2021-08-14 00:00:00' | '2021-08-14 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-14' | '2021-08-14'  |
      | 1  | 1        | 5.0    | '2021-08-15 00:00:00' | '2021-08-15 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-15' | '2021-08-15'  |
      | 2  | 2        | 5.0    | '2021-09-14 00:00:00' | '2021-09-14 00:00:00' | 0        | 'Accepted' | 0       | '2021-09-14' | '2021-09-14'  |
      | 2  | 2        | 5.0    | '2021-09-15 00:00:00' | '2021-09-15 00:00:00' | 0        | 'Accepted' | 0       | '2021-09-15' | '2021-09-15'  |

  Scenario: should not split records if started on weekend
    Given table "dbo.daily_split"
    And table "dbo.no_split"
    And table "dbo.working_hours"
    And table "dbo.no_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted |
      | 1  | 1        | 10.0   | '2021-08-28 00:00:00' | '2021-08-29 00:00:00' | 0        | 'Accepted' | 0       |
    And table "dbo.daily_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 2        | 8.0    | '2021-07-28 00:00:00' | '2021-07-29 00:00:00' | 0        | 'Accepted' | 0       | '2021-07-28' | '2021-07-29'  |
    And table "dbo.working_hours" contains following records
      | calendar_id                            | date                  | working_hours |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-08-28 00:00:00' | 0             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-08-29 00:00:00' | 0             |
    When I run the job
    Then table "dbo.daily_split" contains "2" row
    And table "dbo.daily_split" contains
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 2        | 8.0    | '2021-07-28 00:00:00' | '2021-07-29 00:00:00' | 0        | 'Accepted' | 0       | '2021-07-28' | '2021-07-29'  |
      | 1  | 1        | 10.0   | '2021-08-28 00:00:00' | '2021-08-29 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-28' | '2021-08-29'  |

  Scenario: should not count weekday
    Given table "dbo.daily_split"
    And table "dbo.no_split"
    And table "dbo.working_hours"
    And table "dbo.no_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted |
      | 1  | 1        | 10.0   | '2021-09-07 00:00:00' | '2021-09-10 00:00:00' | 0        | 'Accepted' | 0       |
    And table "dbo.daily_split" contains following records
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 2        | 8.0    | '2021-08-07 00:00:00' | '2021-08-07 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-07' | '2021-08-07'  |
    And table "dbo.working_hours" contains following records
      | calendar_id                            | date                  | working_hours |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-07 00:00:00' | 8             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-09 00:00:00' | 0             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-08 00:00:00' | 0             |
      | 'ed54d950-8064-11df-b42b-002215202537' | '2021-09-10 00:00:00' | 8             |
    When I run the job
    Then table "dbo.daily_split" contains "3" row
    And table "dbo.daily_split" contains
      | id | batch_id | effort | started               | finished              | overtime | status     | deleted | started_date | finished_date |
      | 2  | 2        | 8.0    | '2021-08-07 00:00:00' | '2021-08-07 00:00:00' | 0        | 'Accepted' | 0       | '2021-08-07' | '2021-08-07'  |
      | 1  | 1        | 5.0    | '2021-09-07 00:00:00' | '2021-09-07 00:00:00' | 0        | 'Accepted' | 0       | '2021-09-07' | '2021-09-07'  |
      | 1  | 1        | 5.0    | '2021-09-10 00:00:00' | '2021-09-10 00:00:00' | 0        | 'Accepted' | 0       | '2021-09-10' | '2021-09-10'  |