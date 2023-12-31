use crate::db721::DB721Type::Str;
use anyhow::{bail, Context};
use bytes::Buf;
use pgrx::pg_sys::{float8, Oid, PlannerInfo, RelOptInfo};
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;
use std::sync::Arc;

pub struct Block {
    meta: BlockMeta,
    data: Vec<u8>,
}
impl<'a> Block {
    pub fn serialize_ref(&'a self) -> &'a [u8] {
        self.data.as_slice()
    }
    pub fn serialize_clone(&self) -> Vec<u8> {
        self.data.clone()
    }
}
#[derive(Debug, Clone, Deserialize, Serialize, PartialOrd, PartialEq)]
#[serde(untagged)]
pub enum DB721Type {
    Float(f32),
    Integer(i32),
    Str(String),
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BlockMeta {
    #[serde(rename = "num")]
    value_num: i32,
    min: DB721Type,
    max: DB721Type,
    #[serde(default)]
    min_len: Option<i32>,
    #[serde(default)]
    max_len: Option<i32>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ColumnMeta {
    #[serde(rename = "type")]
    value_type: String,
    start_offset: i32,
    num_blocks: i32,
    #[serde(rename = "block_stats")]
    block_meta: HashMap<String, BlockMeta>,
}
impl ColumnMeta {
    pub fn get_offset_of_block(&self, block_idx: i32) -> usize {
        let mut offset = 0usize;
        for i in 0..block_idx {
            let blk_meta = self.block_meta.get(&i.to_string()).unwrap();
            offset += match self.value_type.as_str() {
                "int" | "float" => 4usize,
                "str" => 32usize,
                _ => panic!("unsupported value type"),
            } * blk_meta.value_num as usize;
        }
        // dbg!(offset);
        offset
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DB721Meta {
    #[serde(rename = "Table")]
    table_name: String,
    #[serde(rename = "Max Values Per Block")]
    max_value_per_block: i32,
    #[serde(rename = "Columns")]
    pub column_meta: HashMap<String, ColumnMeta>,
}
#[derive(Debug, Clone)]
pub struct DB721 {
    pub path: PathBuf,
    pub meta: DB721Meta,
    meta_size: i32,
}
impl DB721 {
    pub fn open(path: PathBuf) -> anyhow::Result<Self> {
        let mut file = std::fs::OpenOptions::new().read(true).open(path.clone())?;
        let _ = file.seek(SeekFrom::End(-4))?;
        let mut buf = Vec::new();
        let mut r_size = file.read_to_end(&mut buf)?;
        assert_eq!(r_size, 4);
        let meta_size = (&buf[0..]).get_i32_le();
        // dbg!(&buf);
        // dbg!(meta_size);
        assert!(meta_size > 0);
        let _ = file.seek(SeekFrom::Current(-((meta_size + 4) as i64)))?;
        buf.clear();
        r_size = file.read_to_end(&mut buf)?;
        assert_eq!(r_size, meta_size as usize + 4);
        assert_eq!(buf[0], b'{');
        let db721_meta: DB721Meta = serde_json::from_slice(&buf[0..meta_size as usize])?;
        return Ok(Self {
            path,
            meta: db721_meta,
            meta_size,
        });
    }
    pub fn row_count(&self) -> usize {
        let column_metas = &self.meta.column_meta;
        let mut row_cnt: usize = 0;
        for column_meta in column_metas.values() {
            for block_meta in column_meta.block_meta.values() {
                row_cnt += block_meta.value_num as usize;
            }
            break;
        }
        row_cnt
    }
}
pub struct BlockIterator {
    block: Arc<Block>,
    value_type: String,
    offset: usize,
    next_value_idx: i32,
    block_meta: BlockMeta,
}
impl BlockIterator {
    pub fn new(block: Arc<Block>, value_type: String, meta: BlockMeta) -> anyhow::Result<Self> {
        match value_type.as_str() {
            "int" | "float" | "str" => {}
            _ => bail!(format!("no support for value type = {}", value_type)),
        };
        Ok(Self {
            block,
            value_type,
            offset: 0,
            block_meta: meta,
            next_value_idx: 1,
        })
    }
    pub fn next(&mut self) -> Option<DB721Type> {
        let mut res: DB721Type;
        if self.next_value_idx > self.block_meta.value_num {
            return None;
        }
        match self.value_type.as_str() {
            "int" => {
                res = DB721Type::Integer(
                    (&self.block.data[self.offset..self.offset + 4]).get_i32_le(),
                );
                self.offset += 4;
            }
            "float" => {
                res =
                    DB721Type::Float((&self.block.data[self.offset..self.offset + 4]).get_f32_le());
                self.offset += 4;
            }
            "str" => {
                res = DB721Type::Str({
                    let mut str = String::from_utf8(
                        (&self.block.data[self.offset..self.offset + 32]).to_vec(),
                    )
                    .expect("need valid UTF-8 String");
                    // TODO: should remove the suffix '\0'?
                    let idx = str.find('\0');
                    match idx {
                        Some(idx) => {
                            str.truncate(idx);
                        }
                        None => {}
                    };
                    str
                });
                self.offset += 32;
            }
            _ => {
                panic!("iterator on unsupported value type")
            }
        };
        self.next_value_idx += 1;
        Some(res)
    }
}

pub struct ColumnIteratorBuilder {
    column_meta: ColumnMeta,
    column_name: String,
    file_path: PathBuf,
    minv: Option<DB721Type>,
    maxv: Option<DB721Type>,
    min_len: Option<i32>,
    max_len: Option<i32>,
}
impl ColumnIteratorBuilder {
    pub fn new(column_meta: ColumnMeta, column_name: String, file_path: PathBuf) -> Self {
        Self {
            column_name,
            column_meta,
            file_path,
            minv: None,
            maxv: None,
            min_len: None,
            max_len: None,
        }
    }
    pub fn build(&self) -> anyhow::Result<ColumnIterator> {
        ColumnIterator::new(
            self.column_name.clone(),
            self.column_meta.clone(),
            self.file_path.clone(),
            None,
            None,
            None,
            None,
        )
    }
    pub fn set_min_value(&mut self, minv: DB721Type) -> &mut Self {
        self.minv = Some(minv);
        self
    }
    pub fn set_max_value(&mut self, maxv: DB721Type) -> &mut Self {
        self.maxv = Some(maxv);
        self
    }
    pub fn set_min_len(&mut self, min_len: i32) -> &mut Self {
        self.min_len = Some(min_len);
        self
    }
    pub fn set_max_len(&mut self, max_len: i32) -> &mut Self {
        self.max_len = Some(max_len);
        self
    }
}

pub struct ColumnIterator {
    next_block_idx: i32,
    now_block_iterator: BlockIterator,
    column_meta: ColumnMeta,
    column_name: String,
    file_path: PathBuf,
    start: bool,
    minv: Option<DB721Type>,
    maxv: Option<DB721Type>,
    min_len: Option<i32>,
    max_len: Option<i32>,
    is_end: bool,
}
impl ColumnIterator {
    pub fn is_end(&self) -> bool{self.is_end}

    pub fn new(
        column_name: String,
        column_meta: ColumnMeta,
        file_path: PathBuf,
        minv: Option<DB721Type>,
        maxv: Option<DB721Type>,
        min_len: Option<i32>,
        max_len: Option<i32>,
    ) -> anyhow::Result<Self> {
        let block_meta = column_meta
            .block_meta
            .get(&0.to_string()).with_context(|| "need at least one block to read")
            ?.clone();
        let block = Arc::new(
            read_one_block(
                column_meta.value_type.clone(),
                column_meta.start_offset as usize,
                block_meta.clone(),
                file_path.clone(),
            )?,
        );
        let block_iterator =
            BlockIterator::new(block, column_meta.value_type.clone(), block_meta).unwrap();
        Ok(Self {
            column_meta,
            column_name,
            file_path,
            next_block_idx: 1,
            start: false,
            now_block_iterator: block_iterator,
            minv: None,
            maxv: None,
            min_len: None,
            max_len: None,
            is_end: false,
        })
    }
    pub fn next(&mut self) -> Option<DB721Type> {
        if self.is_end {
            return None;
        }
        while true {
            if self.next_block_idx > self.column_meta.num_blocks {
                self.is_end = true;
                return None;
            }
            match self.now_block_iterator.next() {
                None => {
                    // let offset = self.column_meta.get_offset_of_block(self.next_block_idx);
                    if self.next_block_idx == self.column_meta.num_blocks{
                        self.is_end = true;
                        return None;
                    }
                    let blk_meta = self
                        .column_meta
                        .block_meta
                        .get(&self.next_block_idx.to_string())
                        .unwrap()
                        .clone();
                    if let Some(min) = self.minv.clone() {
                        if min > blk_meta.max {
                            self.next_block_idx += 1;
                            continue;
                        }
                    }
                    if let Some(max) = self.maxv.clone() {
                        if max < blk_meta.min {
                            self.next_block_idx += 1;
                            continue;
                        }
                    }
                    let offset = self.column_meta.get_offset_of_block(self.next_block_idx);
                    let block = Arc::new(
                        read_one_block(
                            self.column_meta.value_type.clone(),
                            self.column_meta.start_offset as usize + offset,
                            blk_meta.clone(),
                            self.file_path.clone(),
                        )
                        .unwrap(),
                    );
                    let blk_iter =
                        BlockIterator::new(block, self.column_meta.value_type.clone(), blk_meta)
                            .unwrap();
                    self.now_block_iterator = blk_iter;
                    self.next_block_idx += 1;
                }
                Some(val) => {
                    return Some(val);
                }
            }
        }
        None
    }
}
fn read_one_block(
    value_type: String,
    mut offset: usize,
    block_meta: BlockMeta,
    path: PathBuf,
) -> anyhow::Result<Block> {
    let mut res: Vec<u8> = Vec::new();
    let mut file = std::fs::OpenOptions::new().read(true).open(path)?;
    let _ = file.seek(SeekFrom::Start(offset as u64))?;
    let mut size;
    match value_type.as_str() {
        "int" | "float" => {
            size = block_meta.value_num * 4;
        }
        "str" => {
            size = block_meta.value_num * 32;
        }
        _ => {
            panic!("invalid value type ");
        }
    };
    res.resize(size as usize, 0u8);
    // res.clear();
    file.read_exact_at(&mut res, offset as u64)?;
    Ok(Block {
        meta: block_meta,
        data: res,
    })
}
#[cfg(test)]
mod tests {
    use std::io::Write;
    use crate::db721::DB721Type::Str;
    use crate::db721::{
        read_one_block, BlockIterator, ColumnIterator, ColumnIteratorBuilder, DB721Type, DB721,
    };
    use std::path::PathBuf;
    use std::sync::Arc;
    use serde::de::Unexpected::Option;

    fn get_test_db721() -> DB721 {
        let path = PathBuf::from("/home/alyjay/dev/rs_db721_fdw/src/data-chickens.db721");
        DB721::open(path).unwrap()
    }
    fn get_test_dbcsv() -> csv::Reader<std::fs::File> {
        let path = PathBuf::from("data-chickens.csv");
        csv::Reader::from_path(path).unwrap()
    }
    #[test]
    fn export_json_meta() {
        let db721 = get_test_db721();
        let mut file = std::fs::OpenOptions::new().create(true).write(true).read(true).open("/home/alyjay/dev/rs_db721_fdw/src/db721.json").unwrap();
        file.write_all(serde_json::to_string(&db721.meta).unwrap().as_bytes()).unwrap();
    }
    #[test]
    fn test_blk_iterator_no_error() {
        let db721 = get_test_db721();
        for (column_name, column_meta) in db721.meta.column_meta.iter() {
            let blk_meta = column_meta.block_meta.get(&0.to_string()).unwrap().clone();
            let blk = read_one_block(
                column_meta.value_type.clone(),
                column_meta.start_offset as usize,
                blk_meta.clone(),
                db721.path.clone(),
            )
            .unwrap();
            let mut blk_it = BlockIterator::new(
                Arc::new(blk),
                column_meta.value_type.clone(),
                blk_meta.clone(),
            )
            .unwrap();
            while let Some(val) = blk_it.next() {
                if let DB721Type::Str(str) = val {
                    println!("str = {}", str);
                } else {
                    dbg!(val);
                }
            }
            break;
        }
    }
    #[test]
    fn test_column_iterator_no_error() {
        let db721 = get_test_db721();
        for (column_name, column_meta) in db721.meta.column_meta.iter() {
            let column_iterator_buildr = ColumnIteratorBuilder::new(
                column_meta.clone(),
                column_name.clone(),
                db721.path.clone(),
            );

            let mut column_iter = column_iterator_buildr.build().unwrap();
            while let Some(val) = column_iter.next() {
                if let DB721Type::Str(str) = val {
                    println!("str = {}", str);
                } else {
                    dbg!(val);
                }
            }
            break;
        }
    }
    #[test]
    fn test_db721type_order_compare() {
        let db721int_big = DB721Type::Integer(100);
        let db721int_small = DB721Type::Integer(99);
        assert!(db721int_big > db721int_small);

        let db721str_big = DB721Type::Str(String::from("2345"));
        let db721str_small = DB721Type::Str(String::from("1234"));
        assert!(db721str_small < db721str_big);
    }
    #[test]
    fn test_column_iterator_correct_compared_with_csv_iterator() {
        let db721 = get_test_db721();
        let mut csv_reader = get_test_dbcsv();
        for (column_name, column_meta) in db721.meta.column_meta.iter() {
            let column_iterator_buildr = ColumnIteratorBuilder::new(
                column_meta.clone(),
                column_name.clone(),
                db721.path.clone(),
            );
            let mut column_iter = column_iterator_buildr.build().unwrap();
            let mut csv_iterator = csv_reader.records();
            while true {
                let record = csv_iterator.next();
                let column = column_iter.next();
                break;
                // TODO
            }
            break;
        }
    }
    #[test]
    fn test_str_truncate(){
        let mut strr = String::from("12345");
        strr.truncate(0);
        assert_eq!(strr.len(),0);
    }
    #[test]
    fn test_50001_column_name_and_value() {
        let db721 = get_test_db721();
        let column_name_id = "identifier";
        let column_name_sex = "sex";
        let column_meta_id = db721.meta.column_meta.get(column_name_id).unwrap();
        let column_meta_sex = db721.meta.column_meta.get(column_name_sex).unwrap();
        let column_iterator_builder_id = ColumnIteratorBuilder::new(
            column_meta_id.clone(),
            column_name_id.to_string(),
            db721.path.clone(),
        );
        let column_iterator_builder_sex = ColumnIteratorBuilder::new(
            column_meta_sex.clone(),
            column_name_sex.to_string(),
            db721.path.clone(),
        );
        let mut c_id_it = column_iterator_builder_id.build().unwrap();
        let mut c_sex_it = column_iterator_builder_sex.build().unwrap();
        while true{
            let id = c_id_it.next();
            let sex = c_sex_it.next();
            if let None = id{
                break;
            }
            if let Some(DB721Type::Integer(50001)) = id{
                assert_eq!(sex, Some(DB721Type::Str(String::from("FEMALE"))));
            }
        }

    }
}
