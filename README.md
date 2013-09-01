monkey-spanner
==============
Monkey-spanner is collection of user defined functions (UDF) for ELT/analytical work with Apache Hive.  


## Get started ##
```sql
add jar hdfs:///path/to/monkey-spanner.jar;

create temporary function to_sorted_array as 'spanner.monkey.hive.GenericUDAFToSortedArray';
create temporary function map_count as 'spanner.monkey.hive.GenericUDAFMapCounter';
create temporary function map_array_by_key as 'spanner.monkey.hive.GenericUDFMapToArrayByKey';
create temporary function sum_row_vectors as 'spanner.monkey.hive.GenericUDAFSumRowVectors';
create temporary function call_jruby as 'spanner.monkey.hive.GenericUDFCallJRuby';
```

## UDF document ##


## Use Case ##
### Calculate user retention rate ###
```sql

-- Step 1
-- Aggregate the active days for each (genre, user)

create table t1
as
select genre, user_id, map_count(distinct dt) as counter
from access_log
where dt >= '20130801'
group by genre, user_id

-- sample line in t1
-- 'checkin', 'userA', {'20130801':1, '20130802':1, '20130804':1, '20130823':1}


-- Step 2
-- transform the counter in t1 to Array<Int>
-- which consists of the same elements as the target day range
-- while each element stands for active status on that day

create table t2
as
select
  genre, user_id,
  map_array_by_key(
    counter,
    0, -- default value
    '20130801', '20130802', '20130803', '20130804', '20130805', '20130806', '20130807', '20130808',
    '20130809', '20130810', '20130811', '20130812', '20130813', '20130814', '20130815', '20130816',
    '20130817', '20130818', '20130819', '20130820', '20130821', '20130822', '20130823', '20130824',
    '20130825', '20130826', '20130827', '20130828', '20130829', '20130830', '20130831'
  ) as active_bits
from t1;

-- sample line in t2
-- 'checkin', 'userA', [1,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0]


-- Step 3
-- to calculate retention rates for each genre, based on 20130801's user
-- we can group the records in t2 by genre, and sum up the active_bits using sum_row_vectors

select
  genre, sum_row_vectors(active_bits)
from t2
where active_bits[0] = 1  -- active user in '20130801'
group by genre;

```
