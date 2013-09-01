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
### User retention rate ###
#### Prepare ####
```sql

add jar hdfs:///path/to/monkey-spanner.jar;
add jar hdfs:///path/to/jruby.jar;

create temporary function map_count as 'spanner.monkey.hive.GenericUDAFMapCounter';
create temporary function map_array_by_key as 'spanner.monkey.hive.GenericUDFMapToArrayByKey';
create temporary function sum_row_vectors as 'spanner.monkey.hive.GenericUDAFSumRowVectors';
create temporary function call_jruby as 'spanner.monkey.hive.GenericUDFCallJRuby';
```

#### Step 1 : Aggregate the active days for each (genre, user) ####
```sql
create table t1
as
select genre, user_id, map_count(distinct dt) as counter
from access_log
where dt >= '20130801'
group by genre, user_id

-- sample line in t1
-- 'checkin', 'userA', {'20130801':1, '20130802':1, '20130804':1, '20130823':1}
```

#### Step 2 : Transform Map&lt;dt, flag&gt; to Array&lt;flag&gt; ####
```sql
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
```

#### Step 3A : Calculate retention rate ####
```sql
-- to calculate retention rates for each genre, based on 20130801's user
-- we can group the records in t2 by genre, and sum up the active_bits using sum_row_vectors

create table t3
as
select
  genre, sum_row_vectors(active_bits) as ret_cnts
from t2
where active_bits[0] = 1  -- active user in '20130801'
group by genre;

-- sample lines in t3
'checkin', [327,201,174,177,188,180,189,183,173,152,164,157,145,151,155,160,152,159,162,157,152,153,156,154,146,153,151,163,156,144,152]
'top', [958,834,830,825,827,821,812,799,803,802,807,798,806,799,779,793,795,799,796,799,790,778,788,779,791,793,783,795,787,779,872]

-- Now you can get retention rate as:
select
  genre, ret_cnts,
  call_jruby(array(1.1), 'ret = arg1.map{|x| (x/(arg2.to_f)*100).round(2).to_f}', ret_cnts, ret_cnts[0]) as ret_rate
from t3;

-- call_jruby executes a ruby scriptlet
-- which return a new array by deviding every count in ret_cnts to count of '20130801' (ret_cnts[0])
-- the first argument 'array(1.1)' is used to tell 'call_jruby' what type the output should be,
-- so here it means array of float number.
```

####  Step 3B : Calculate continuous retention rate ####
```sql
-- you may also want to calculate continuous retention rate,
-- which stands for the percentage of users that keep returning everyday
-- then we can:

create table t3b
as
select
  genre,
  sum_row_vectors(
    call_jruby(Array(1), 'conti=true; ret = arg1.map {|x| conti = false if not (x>0); (conti)?x:0 }', active_bits)
  ) as ret_cnts
from t2
where active_bits[0] = 1  -- active user in '20130801'
group by genre;

-- the call_jruby function here transform a [1,1,0,1] array to [1,1,0,0] for continuous retention calculation

-- then we can query t3b to get continuous retention rates as:

select
  genre, ret_cnts,
  call_jruby(array(1.1), 'ret = arg1.map{|x| (x/(arg2.to_f)*100).round(2).to_f}', ret_cnts, ret_cnts[0]) as ret_rate
from t3b;

-- the result may looks like:
-- 'checkin', ...,[100.0,78.36,69.06,63.37,59.44,56.73,54.09,52.18,50.45,48.69,47.22,45.96,44.97,44.22,43.45,41.95,41.25,40.8,40.46,40.16,39.87,39.31,38.87,38.44,38.02,37.58,37.12,36.66,35.23,34.68,34.12]
```

### Collaborative Filtering Recommendation###
Suppose we have a **user_history** table which contains user action history (may by purchase, rate, play, etc.):
```sql
user_id bigint
item_id bigint
scrore  double
```
We can calculate recommendations for certain item in Hive by using item-based collaborative filtering algorithm.  
Also note that we use cosine similarity here.

#### prepare cosine similarity A ####
```sql
create table item_score
as
select
  item_id,
  sum(pow(score, 2)) as score
from user_log
group by item_id
```

#### prepare cosine similarity B ####
```sql
create table item_cross_score
as
select
  a.item_id as item_a,
  b.item_id as item_b,
  sum(a.score*b.score) as score,
  count(distinct a.user_id) as uu
from user_log a join user_log b
on (a.user_id = b.user_id)
where a.item_id != b.item_id
group by a.item_id, b.item_id
```

#### figure cosine similarity and rank ####
```sql
create temporary function to_sorted_array as 'spanner.monkey.hive.GenericUDAFToSortedArray';
create temporary function p_rank as 'spanner.monkey.hive.PsuedoRank';


create table item_similarity
as
select
  item_a,
  item_b,
  cross_score/sqrt(a.score*b_score) as similarity,
  a.score as a_score,
  b_score,
  cross_score,
  uu
from item_score a join
(
select c.item_a, c.item_b, c.score as cross_score, b.score as b_score, uu
from item_score b join item_cross_score c on (b.item_id = c.item_b)
) d on (d.item_a = a.item_id)
;

create table item_reco
as
select
item_a as item,
to_sorted_array(item_b, rank) as reco
from
(
select item_a, p_rank(item_a) as rank, item_b, similarity as score
from
(
select item_a, item_b, similarity
from item_similarity
where uu >= 3
distribute by item_a
sort by item_a asc , similarity desc
) t1
) t2
where rank <= 30
group by item_a
;
```


### Sessionize ###
