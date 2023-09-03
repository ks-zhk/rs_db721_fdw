use std::str::Bytes;

trait Storage {
    fn get_value_by_column_name(name: String, index: i32) -> Option<Vec<u8>>;
    // fn get_ref_value_by_column_name(name:String, index: i32) -> Option<&'a[u8]>;
}
