Feature: historical processing

  Scenario: casual historical processing
    Given table "dbo.historical_table"
      | id  | version | hashsum |
      | 'a' | 1       | 1       |
      | 'a' | 2       | 2       |
      | 'b' | 2       | 1       |
      | 'c' | 2       | 1       |
    And  table "dbo.staging_table"
      | id  | version | hashsum |
      | 'a' | 3       | 1       |
      | 'a' | 4       | 1       |
      | 'a' | 5       | 2       |
      | 'b' | 3       | 1       |
      | 'c' | 4       | 2       |
    And I change params "{"history_table":"dbo.historical_table", "staging_table": "dbo.staging_table"}"
    When I run the job
    Then table "dbo.historical_table" contains
      | id  | version | hashsum |
      | 'a' | 1       | 1       |
      | 'a' | 2       | 2       |
      | 'b' | 2       | 1       |
      | 'c' | 2       | 1       |
      | 'a' | 3       | 1       |
      | 'a' | 5       | 2       |
      | 'c' | 4       | 2       |

  Scenario: no records in historical table
    Given table "dbo.historical_table"
    And table "dbo.staging_table"
      | id | version | hashsum |
      | 1  | 1       | 1       |
      | 1  | 2       | 1       |
      | 1  | 3       | 2       |
      | 2  | 1       | 1       |
    And I change params "{"history_table":"dbo.historical_table", "staging_table": "dbo.staging_table"}"
    When I run the job
    Then table "dbo.historical_table" contains
      | id | version | hashsum |
      | 1  | 1       | 1       |
      | 1  | 3       | 2       |
      | 2  | 1       | 1       |

  Scenario: no new records in staging table
    Given table "dbo.historical_table"
      | id | version | hashsum |
      | 1  | 1       | 1       |
    And table "dbo.staging_table"
    And I change params "{"history_table":"dbo.historical_table", "staging_table": "dbo.staging_table"}"
    When I run the job
    Then table "dbo.historical_table" contains
      | id | version | hashsum |
      | 1  | 1       | 1       |


  Scenario: historical table doesn't contain required fields
    Given table "dbo.historical_table_no_req"
      | item_id | ver | hash_sum |
      | 1       | 1   | 1        |
    And I change params "{"history_table": "dbo.historical_table_no_req", "staging_table": "dbo.staging_table"}"
    And table "dbo.staging_table"
    When I run the job
    Then error occur

  Scenario: staging table doesn't contain required fields
    Given table "dbo.historical_table"
      | id | version | hashsum |
      | 1  | 1       | 1       |
    And table "dbo.staging_table_no_req"
      | item_id | ver | hash_sum |
      | 1       | 2   | 2        |
    And I change params "{"history_table": "dbo.historical_table", "staging_table": "dbo.staging_table_no_req"}"
    When I run the job
    Then error occur

  Scenario: historical processing with required fields substitution
    Given table "dbo.historical_table_no_req"
      | item_id | ver | hash_sum |
      | 'a'     | 1   | 1        |
      | 'a'     | 2   | 2        |
      | 'b'     | 2   | 1        |
      | 'c'     | 2   | 1        |
    And  table "dbo.staging_table_no_req"
      | item_id | ver | hash_sum |
      | 'a'     | 3   | 1        |
      | 'a'     | 4   | 1        |
      | 'a'     | 5   | 2        |
      | 'b'     | 3   | 1        |
      | 'c'     | 4   | 2        |
    And I change params "{"history_table": "dbo.historical_table_no_req", "staging_table": "dbo.staging_table_no_req", "history_req_field_mappings":[{"from" : "id", "to": "item_id"},{"from" : "version","to": "ver"},{"from" : "hashsum", "to": "hash_sum"}], "staging_req_field_mappings":[{"from" : "id", "to": "item_id"},{"from" : "version","to": "ver"},{"from" : "hashsum", "to": "hash_sum"}]}"
    When I run the job
    Then table "dbo.historical_table_no_req" contains
      | item_id | ver | hash_sum |
      | 'a'     | 1   | 1        |
      | 'a'     | 2   | 2        |
      | 'b'     | 2   | 1        |
      | 'c'     | 2   | 1        |
      | 'a'     | 3   | 1        |
      | 'a'     | 5   | 2        |
      | 'c'     | 4   | 2        |