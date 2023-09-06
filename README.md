# rs_db721_fdw

使用[pgrx](https://github.com/pgcentralfoundation/pgrx)框架，编写pg插件，对[db721](https://15721.courses.cs.cmu.edu/spring2023/project1.html)文件格式进行适配，从而使得pg能够使用sql操作该文件。

**暂时仅支持读操作**。

## 环境安装
1. Rust环境（https://rustup.rs/)
2. git
3. clang
4. tar
5. bzip2
6. gcc 7 or newer
7. pg依赖
   ```
   sudo apt install build-essential libreadline-dev zlib1g-dev \ 
   flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils \ 
   xsltproc ccache liblz4-dev
   ```
## pgrx安装及初始化
```shell
cargo install --locked cargo-pgrx
# 编译pg时使用lz4
cargo pgrx init --configure-flag=--with-lz4
```
## 运行
```shell
# 进入项目根目录
cd $PROJECT_HOME
cargo pgrx run
```

## 加载插件及创建表
```sql
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
```

## 测试
```sql
pg_hello_world=# select * from db721_chicken where identifier >= 10000 and identifier <= 10010;
 identifier |  farm_name  | weight_model |  sex   | age_weeks | weight_g  | notes 
------------+-------------+--------------+--------+-----------+-----------+-------
      10000 | Cheep Birds | GOMPERTZ     | FEMALE |      3.22 | 1351.4467 | WOODY
      10001 | Cheep Birds | WEIBULL      | FEMALE |      0.77 | 532.59985 | WOODY
      10002 | Cheep Birds | MMF          | FEMALE |      4.72 | 1898.8148 | WOODY
      10003 | Cheep Birds | GOMPERTZ     | FEMALE |      5.13 | 2199.7117 | WOODY
      10004 | Cheep Birds | WEIBULL      | MALE   |      4.55 | 1856.7339 | WOODY
      10005 | Cheep Birds | MMF          | FEMALE |      2.91 | 1254.1752 | WOODY
      10006 | Cheep Birds | WEIBULL      | FEMALE |      4.75 |  2152.301 | WOODY
      10007 | Cheep Birds | GOMPERTZ     | MALE   |      4.23 | 1920.6493 | WOODY
      10008 | Cheep Birds | WEIBULL      | MALE   |      3.31 | 1246.4785 | WOODY
      10009 | Cheep Birds | WEIBULL      | FEMALE |      5.42 |  2259.148 | WOODY
      10010 | Cheep Birds | MMF          | MALE   |      1.38 |  896.7105 | WOODY
            

pg_hello_world=# select identifier, farm_name, sex from db721_chicken limit 10;
identifier |  farm_name  |  sex   
------------+-------------+--------
          1 | Cheep Birds | FEMALE
          2 | Cheep Birds | FEMALE
          3 | Cheep Birds | FEMALE
          4 | Cheep Birds | MALE
          5 | Cheep Birds | MALE
          6 | Cheep Birds | FEMALE
          7 | Cheep Birds | FEMALE
          8 | Cheep Birds | FEMALE
          9 | Cheep Birds | FEMALE
         10 | Cheep Birds | MALE
(10 rows)
           
           
pg_hello_world=# select avg(age_weeks) from db721_chicken;
avg         
--------------------
 31.958300915127264
(1 row)
```

## 参考

https://github.com/citusdata/cstore_fdw

https://github.com/EnterpriseDB/mysql_fdw

https://github.com/pgcentralfoundation/pgrx