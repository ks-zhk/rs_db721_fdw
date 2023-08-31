mod db721;
mod storage;
mod db721rs_fdw_scan;

use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use crate::db721rs_fdw_scan::{db721_begin_foreign_scan, db721_end_foreign_scan, db721_get_foreign_paths, db721_get_foreign_plan, db721_get_foreign_rel_size, db721_iterate_foreign_scan};

pgrx::pg_module_magic!();

#[derive(Serialize, Deserialize, PostgresType)]
pub struct MyType {
    values: Vec<String>,
    thing: Option<Box<MyType>>
}

#[pg_extern]
fn push_value(mut input: MyType, value: String) -> MyType {
    input.values.push(value);
    input
}

#[derive(Serialize, Deserialize, PostgresType)]
pub struct MyPoint{
    x:i32,
    y:i32,
}

#[pg_extern]
fn test_my_point(pt: MyPoint) -> MyPoint{
    pt
}


#[pg_extern]
fn hello_pg_hello_world() -> &'static str {
    "Hello, pg_hello_world"
}

#[pg_extern]
fn test_params(i: i32, strs: &str) -> String{
    format!("i is {i} and strs is {strs}")
}

#[pg_extern]
fn test_vec(arr: Vec<i32>) -> String {
    let mut res = String::from("[");
    for i in arr{
        res.push_str(format!("{i},").as_str());
    }
    res.pop();
    if !res.is_empty() {
        res.push_str("]");
    }
    res
}

#[pg_extern]
fn hello_world() -> &'static str{"hello, world!"}

#[pg_extern]
unsafe fn db721_fdw_handler() -> PgBox<pg_sys::FdwRoutine>{
    let mut fdw_routine = PgBox::<pg_sys::FdwRoutine>::alloc_node(pg_sys::NodeTag_T_FdwRoutine);
    fdw_routine.GetForeignRelSize = Some(db721_get_foreign_rel_size);
    fdw_routine.GetForeignPaths = Some(db721_get_foreign_paths);
    fdw_routine.GetForeignPlan = Some(db721_get_foreign_plan);
    fdw_routine.BeginForeignScan = Some(db721_begin_foreign_scan);
    fdw_routine.IterateForeignScan = Some(db721_iterate_foreign_scan);
    fdw_routine.EndForeignScan = Some(db721_end_foreign_scan);
    fdw_routine.into_pg_boxed()
}
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_hello_world() {
        assert_eq!("Hello, pg_hello_world", crate::hello_pg_hello_world());
    }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
