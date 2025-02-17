#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Wait for number of parts in table $1 to become $2.
# Print the changed value. If no changes for $3 seconds, prints initial value.
wait_for_number_of_parts() {
    for _ in `seq $3`
    do
        sleep 1
        res=`$CLICKHOUSE_CLIENT -q "SELECT count(*) FROM system.parts WHERE database = currentDatabase() AND table='$1' AND active"`
        if [ "$res" -eq "$2" ]
        then
            echo "$res"
            return
        fi
    done
    echo "$res"
}

$CLICKHOUSE_CLIENT -nmq "
DROP TABLE IF EXISTS test_without_merge;
DROP TABLE IF EXISTS test_with_merge;
DROP TABLE IF EXISTS test_replicated;

SELECT 'Without merge';

CREATE TABLE test_without_merge (i Int64) ENGINE = MergeTree ORDER BY i SETTINGS merge_selecting_sleep_ms=1000;
INSERT INTO test_without_merge SELECT 1;
INSERT INTO test_without_merge SELECT 2;
INSERT INTO test_without_merge SELECT 3;"

wait_for_number_of_parts 'test_without_merge' 1 10

$CLICKHOUSE_CLIENT -nmq "
DROP TABLE test_without_merge;

SELECT 'With merge any part range';

CREATE TABLE test_with_merge (i Int64) ENGINE = MergeTree ORDER BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=false;
INSERT INTO test_with_merge SELECT 1;
INSERT INTO test_with_merge SELECT 2;
INSERT INTO test_with_merge SELECT 3;"

wait_for_number_of_parts 'test_with_merge' 1 100

$CLICKHOUSE_CLIENT -nmq "
DROP TABLE test_with_merge;

SELECT 'With merge partition only';

CREATE TABLE test_with_merge (i Int64) ENGINE = MergeTree ORDER BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=true;
INSERT INTO test_with_merge SELECT 1;
INSERT INTO test_with_merge SELECT 2;
INSERT INTO test_with_merge SELECT 3;"

wait_for_number_of_parts 'test_with_merge' 1 100

$CLICKHOUSE_CLIENT -nmq "
DROP TABLE test_with_merge;

SELECT 'With merge replicated any part range';

CREATE TABLE test_replicated (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test02473', 'node')  ORDER BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=false;
INSERT INTO test_replicated SELECT 1;
INSERT INTO test_replicated SELECT 2;
INSERT INTO test_replicated SELECT 3;"

wait_for_number_of_parts 'test_replicated' 1 100

$CLICKHOUSE_CLIENT -nmq "
DROP TABLE test_replicated;

SELECT 'With merge replicated partition only';

CREATE TABLE test_replicated (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test02473_partition_only', 'node')  ORDER BY i
SETTINGS min_age_to_force_merge_seconds=1, merge_selecting_sleep_ms=1000, min_age_to_force_merge_on_partition_only=true;
INSERT INTO test_replicated SELECT 1;
INSERT INTO test_replicated SELECT 2;
INSERT INTO test_replicated SELECT 3;"

wait_for_number_of_parts 'test_replicated' 1 100

$CLICKHOUSE_CLIENT -nmq "
DROP TABLE test_replicated;"
