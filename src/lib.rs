mod db721;
mod storage;

use pgrx::prelude::*;
use serde::{Deserialize, Serialize};

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
