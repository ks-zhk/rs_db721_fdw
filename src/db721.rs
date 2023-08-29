use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::PathBuf;
use std::sync::Arc;
use anyhow::{bail, Context};
use pgrx::pg_sys::{float8, Oid, PlannerInfo, RelOptInfo};
use serde::{Deserialize, Serialize};
use bytes::Buf;
use crate::db721::DB721Type::Str;


struct Block{
    meta: BlockMeta,
    data: Vec<u8>,
}
impl<'a> Block{
    pub fn serialize_ref(&'a self) -> &'a[u8] {
        self.data.as_slice()
    }
    pub fn serialize_clone(&self) -> Vec<u8> {
        self.data.clone()
    }
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum DB721Type{
    Float(f32),
    Integer(i32),
    Str(String),
}
#[derive(Debug, Deserialize,Serialize, Clone)]
struct BlockMeta{
    #[serde(rename="num")]
    value_num: i32,
    min: DB721Type,
    max: DB721Type,
    #[serde(default)]
    min_len: Option<i32>,
    #[serde(default)]
    max_len: Option<i32>,
}
#[derive(Debug, Clone, Deserialize,Serialize)]
struct ColumnMeta{
    #[serde(rename="type")]
    value_type: String,
    start_offset: i32,
    num_blocks: i32,
    #[serde(rename="block_stats")]
    block_meta: HashMap<String, BlockMeta>,
}
impl ColumnMeta{
    pub fn get_offset_of_block(&self,block_idx: i32) -> usize{
        let mut offset = 0usize;
        for i in 0..block_idx{
            let blk_meta = self.block_meta.get(&i.to_string()).unwrap();
            offset += match self.value_type.as_str(){
                "int" | "float" => 4usize,
                "str" => 32usize,
                _ => panic!("unsupported value type")
            } * blk_meta.value_num as usize;
        }
        offset
    }
}
#[derive(Debug, Deserialize,Serialize)]
struct DB721Meta{
    #[serde(rename="Table")]
    table_name: String,
    #[serde(rename="Max Values Per Block")]
    max_value_per_block: i32,
    #[serde(rename="Columns")]
    column_meta: HashMap<String, ColumnMeta>,
}
#[derive(Debug)]
struct DB721{
    path: PathBuf,
    meta: DB721Meta,
    meta_size: i32,
}
impl DB721{
    pub fn open(path: PathBuf) -> anyhow::Result<Self>{
        let mut file = std::fs::OpenOptions::new().read(true).open(path.clone())?;
        let _ = file.seek(SeekFrom::End(-4))?;
        let mut buf = Vec::new();
        let mut r_size = file.read_to_end(&mut buf)?;
        assert_eq!(r_size, 4);
        let meta_size = (&buf[0..]).get_i32_le();
        dbg!(&buf);
        dbg!(meta_size);
        assert!(meta_size > 0);
        let _ = file.seek(SeekFrom::Current(-((meta_size + 4) as i64)))?;
        buf.clear();
        r_size = file.read_to_end(&mut buf)?;
        assert_eq!(r_size, meta_size as usize + 4);
        assert_eq!(buf[0], b'{');
        let db721_meta: DB721Meta = serde_json::from_slice(&buf[0..meta_size as usize])?;
        return Ok(Self{ path, meta: db721_meta, meta_size}
        )
    }

}
struct BlockIterator{
    block: Arc<Block>,
    value_type: String,
    offset: usize,
    next_value_idx: i32,
    block_meta: BlockMeta,
}
impl BlockIterator {
    pub fn new(block: Arc<Block>, value_type: String, meta: BlockMeta) -> anyhow::Result<Self>{
        match value_type.as_str(){
            "int" | "float" | "str" => {},
            _ => bail!(format!("no support for value type = {}", value_type))
        };
        Ok(Self{
            block,value_type, offset: 0, block_meta: meta, next_value_idx: 1,
        })
    }
    pub fn next(&mut self) -> Option<DB721Type> {
        let mut res:DB721Type;
        if self.next_value_idx > self.block_meta.value_num {
            return None;
        }
        match self.value_type.as_str(){
            "int" => {
                res = DB721Type::Integer((&self.block.data[self.offset..self.offset + 4]).get_i32_le());
                self.offset += 4;
            },
            "float" => {
                res = DB721Type::Float((&self.block.data[self.offset..self.offset + 4]).get_f32_le());
                self.offset += 4;
            },
            "str" => {
                res = DB721Type::Str(String::from_utf8
                    ((&self.block.data[self.offset..self.offset+32]).to_vec())
                    .expect("need valid UTF-8 String")
                );
                self.offset += 32;
            },
            _ => {
                panic!("iterator on unsupported value type")
            },
        };
        self.next_value_idx += 1;
        Some(res)
    }
}
struct ColumnIterator{
    next_block_idx: i32,
    now_block_iterator: BlockIterator,
    column_meta: ColumnMeta,
    column_name: String,
    file_path: PathBuf,
    start: bool,
}
impl ColumnIterator{
    pub fn new(column_name: String, column_meta: ColumnMeta, file_path: PathBuf) -> Self{
        let block_meta = column_meta.block_meta.get(&0.to_string()).expect("column must have one block").clone();
        let block = Arc::new(
            read_one_block(
                column_meta.value_type.clone(),
                column_meta.start_offset as usize,
                block_meta.clone(), file_path.clone()
            ).unwrap()
        );
        let block_iterator = BlockIterator::new(block, column_meta.value_type.clone(), block_meta).unwrap();
        Self{column_meta, column_name, file_path, next_block_idx: 1, start: false, now_block_iterator: block_iterator}
    }
    pub fn next(&mut self) -> Option<DB721Type> {
        while true{
            if self.next_block_idx >= self.column_meta.num_blocks {
                return None;
            }
            // 默认不可写
            match self.now_block_iterator.next() {
                None => {
                    let offset = self.column_meta.get_offset_of_block(self.next_block_idx);
                    let blk_meta = self.column_meta.block_meta.get(&self.next_block_idx.to_string()).unwrap().clone();
                    let block = Arc::new(
                        read_one_block(
                            self.column_meta.value_type.clone(),
                            offset,
                            blk_meta.clone(),
                            self.file_path.clone()
                        ).unwrap()
                    );
                    let blk_iter = BlockIterator::new(block, self.column_meta.value_type.clone(), blk_meta).unwrap();
                    self.now_block_iterator = blk_iter;
                    self.next_block_idx += 1;
                }
                Some(val) => return Some(val),
            };
        }
        None
    }
}
fn read_one_block(value_type: String, mut offset: usize, block_meta: BlockMeta, path: PathBuf) -> anyhow::Result<Block>{
    let mut res:Vec<u8> = Vec::new();
    let mut file = std::fs::OpenOptions::new().read(true).open(path)?;
    let _ = file.seek(SeekFrom::Start(offset as u64))?;
    let mut size;
    match value_type.as_str(){
        "int" | "float" => {
            size = block_meta.value_num * 4;

        },
        "str" => {
            size = block_meta.value_num * 32;
            // let start = offset;
            // let mut block_size = 0usize;
            // let mut cnt = 0;
            // let mut tmp = Vec::new();
            // while cnt < block_meta.value_num{
            //     let r_size = file.read_at(&mut tmp, offset as u64)?;
            //     offset += r_size;
            //     for val in tmp.iter(){
            //         block_size += 1;
            //         if val == &u8{
            //             cnt += 1;
            //             if cnt == block_meta.value_num{
            //                 break;
            //             }
            //         }
            //     }
            // }
            // res.resize(block_size,0u8);
            // res.clear();
            // file.read_exact_at(&mut res, start as u64)?;
        },
        _ => {
            panic!("invalid value type ");
        }
    };
    res.resize(size as usize,0u8);
    // res.clear();
    file.read_exact_at(&mut res, offset as u64)?;
    Ok(Block{
        meta: block_meta,
        data: res,
    })
}
#[cfg(test)]
mod tests{
    use std::path::PathBuf;
    use std::sync::Arc;
    use crate::db721::{BlockIterator, ColumnIterator, DB721, read_one_block};

    fn get_test_db721() -> DB721{
        let path = PathBuf::from("/home/alyjay/Documents/dev/postgres/data-chickens.db721");
        DB721::open(path).unwrap()
    }
    #[test]
    fn test_blk_iterator_no_error() {
        let db721 = get_test_db721();
        for (column_name, column_meta) in db721.meta.column_meta.iter(){
            let blk_meta = column_meta.block_meta.get(&0.to_string()).unwrap().clone();
            let blk = read_one_block(
                column_meta.value_type.clone(),
                column_meta.start_offset as usize,
                blk_meta.clone(),
                db721.path.clone(),

            ).unwrap();
            let mut blk_it = BlockIterator::new(
                Arc::new(blk),
                column_meta.value_type.clone(),
                blk_meta.clone())
                .unwrap();
            while let Some(val) = blk_it.next() {
                dbg!(val);
            }
            break
        }
    }
    #[test]
    fn test_column_iterator_no_error() {
        let db721 = get_test_db721();
        for (column_name, column_meta) in db721.meta.column_meta.iter(){
            let mut column_iter = ColumnIterator::new(
                column_name.clone(),
                column_meta.clone(),
                db721.path.clone(),
            );
            while let Some(val) = column_iter.next(){
                dbg!(val);
            }
            break
        }
    }
}