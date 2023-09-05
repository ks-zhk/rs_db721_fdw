DROP EXTENSION pg_hello_world CASCADE;
CREATE EXTENSION pg_hello_world;
create foreign data wrapper test_wrapper handler db721_fdw_handler;
create server test_server foreign data wrapper test_wrapper;
CREATE FOREIGN TABLE IF NOT EXISTS db721_chicken (
    identifier      integer,
    farm_name       varchar,
    weight_model    varchar,
    sex             varchar,
    age_weeks       real,
    weight_g        real,
    notes           varchar
) SERVER test_server OPTIONS
(
filename '/home/alyjay/dev/rs_db721_fdw/src/data-chickens.db721',
tablename 'Chicken'
);
select * from db721_chicken;