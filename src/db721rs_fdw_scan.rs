/// this mod aims to impl some scan callbacks for db721 file
use crate::db721::{ColumnIterator, ColumnIteratorBuilder, DB721, DB721Type};
use anyhow::Context;
use libc::{c_uchar, memcpy, memset, size_t, strncmp};
use pgrx::pg_sys::{cluster_name, defGetString, extract_actual_clauses, get_attname, lappend, list_concat, list_copy, list_make1_impl, list_union, makeVar, make_foreignscan, palloc0, pull_var_clause, relation_close, relation_open, scalararraysel, AccessShareLock, AttrNumber, BeginForeignScan_function, Cardinality, DefElem, ForEachState, ForeignScan, ForeignScanState, FormData_pg_attribute, GetForeignTable, List, ListCell, Node, NodeTag_T_List, Oid, PLpgSQL_stmt_foreach_a, PlannerInfo, RelOptInfo, Relation, RelationGetReplicaIndex, RestrictInfo, Size, TupleDesc, TupleDescGetAttInMetadata, Var, EXEC_FLAG_EXPLAIN_ONLY, LOCKMODE, NAMEDATALEN, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, TupleTableSlot, Datum, Hash, ExecStoreVirtualTuple, DatumTupleFields, varlena, VarChar, VARHDRSZ, VariableStatData};
use pgrx::prelude::*;
use pgrx::{ereport, pg_guard, void_mut_ptr, PgList, PgLogLevel, NULL};
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::io::Write;
use std::mem::size_of;
use std::ops::{DerefMut, Index};
use std::path::PathBuf;
use std::ptr;
use std::str::Utf8Error;
use core::option::Option;

/// 获取一个列表元素中的指针值, ListCell是一个union
macro_rules! l_first {
    ($lc:ident) => {
        (*($lc as *mut ListCell)).ptr_value
    };
}
/// 获取列表中第n个ListCell
macro_rules! l_nth_cell {
    ($list:ident, $idx: ident) => {
        ((*$list).elements as usize + size_of::<*mut ListCell>() * ($idx as usize)) as *mut c_void
    };
    ($list:ident, $idx: literal) => {
        ((*$list).elements as usize + size_of::<*mut ListCell>() * ($idx as usize)) as *mut c_void
    };
    ($list:expr, $idx: ident) => {
        ((*$list).elements as usize + size_of::<*mut ListCell>() * ($idx as usize)) as *mut c_void
    }
}
/// 将rust字符串转为c字符串，并返回 char *
macro_rules! literal_str_to_cstr {
    ($str:literal) => {
        CString::new($str).expect("CString::new failed").into_raw()
    };
    ($str:ident) => {
        CString::new($str).expect("CString::new failed").into_raw()
    };
    ($str:expr) => {
        CString::new($str).expect("CString::new failed").into_raw()
    }
}
/// 提取option中的元素，并返回其可变引用(&mut T)
/// input: Option; output: mut reference of inner of this option
/// Will PANIC if option is NONE
macro_rules! def_option_ptr_mut {
    ($ptr:ident) => {
        if let Some(o_ref) = &mut (($ptr)) {
            o_ref
        } else {
            panic!("can not deference None Option")
        }
    };
    ($ptr:expr) => {
        if let Some(o_ref) = &mut (($ptr)) {
            o_ref
        } else {
            panic!("can not deference None Option")
        }
    };
}
/// 提取option内的对象，返回不可变引用
/// input: Option; output: mut reference of inner of this option
/// Will PANIC if option is NONE
macro_rules! def_option_ptr {
    ($ptr:ident) => {
        if let Some(o_ref) = &(($ptr)) {
            o_ref
        } else {
            panic!("can not deference None Option")
        }
    };
    ($ptr:expr) => {
        if let Some(o_ref) = &(($ptr)) {
            o_ref
        } else {
            panic!("can not deference None Option")
        }
    };
}
/// 使用ereport进行log
macro_rules! warning_log {
    ($str:literal) => {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            $str
        );
    };
    ($str:expr) => {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            $str
        );
    }
}
pub struct DB721ScanState {
    db721: DB721,
    column_list: *mut List,
    tuple_desc: TupleDesc,
    where_clause_list: *mut List,
    column_iterators: HashMap<String, ColumnIterator>,
    column_index_map_name: HashMap<i16, String>,
}

impl DB721ScanState {
    /// 这里返回 *mut Option<DB721ScanState> 的原因是为了能够
    /// 在foreign_scan_end函数中，对rust自动申请的堆内存进行释
    /// 放（使用std::mem::replace）
    #[pg_guard]
    pub fn new(
        db_721: DB721,
        tuple_desc: TupleDesc,
        column_list: *mut List,
        where_clause_list: *mut List,
    ) -> *mut Option<DB721ScanState> {
        unsafe {
            // warning_log!("in db_721_scan_state new func");
            let mut state = DB721ScanState{
                db721: db_721.clone(),
                column_list,
                tuple_desc,
                where_clause_list,
                column_iterators: HashMap::new(),
                column_index_map_name: HashMap::new(),
            };
            let column_count = (*tuple_desc).natts;
            for index in 0..(*column_list).length {
                let lc = l_nth_cell!(column_list, index) as *mut ListCell;
                let column = l_first!(lc) as *mut Var;
                let attr_form = (((*tuple_desc).attrs.as_mut_ptr()) as *mut FormData_pg_attribute)
                    .add((*column).varattno as usize - 1);
                let column_name_raw = (*attr_form).attname.data.as_mut_ptr();
                let column_name_cstr = CStr::from_ptr(column_name_raw);
                let column_name = column_name_cstr.to_str().unwrap();
                let column_iterator_builder = ColumnIteratorBuilder::new(
                    db_721.meta.column_meta.get(column_name).unwrap().clone(),
                    column_name.to_string(),
                    db_721.path.clone(),
                );
                let column_iterator = column_iterator_builder.build().unwrap();
                state.column_iterators.insert(column_name.to_string(), column_iterator);
                state.column_index_map_name.insert((*column).varattno -1, column_name.to_string());
            }
            let b_state = Box::new(Some(state));
            Box::leak(b_state)
        }
    }
    pub fn is_end(&self) -> bool{
        for column_it in self.column_iterators.values(){
            if !column_it.is_end() {
                return false
            }
        }
        true
    }
}
/// 行数预测
#[pg_guard]
pub extern "C" fn db721_get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
) {
    unsafe {
        // 首先获取表对应的文件名
        let file_name = db721_get_option_value(foreign_table_id, literal_str_to_cstr!("filename"));
        let cstr_file_name = CStr::from_ptr(file_name);
        // 进行文件元信息的读取
        let db721_table = DB721::open(PathBuf::from(
            cstr_file_name
                .to_str()
                .expect("file_name should be valid UTF-8"),
        ))
        .unwrap();
        // 获取行数量，并赋值给pg中的对象
        (*base_rel).rows = db721_table.row_count() as Cardinality;
    }
}
/// 准备开始扫表，主要工作是创建扫描状态。
#[pg_guard]
pub extern "C" fn db721_begin_foreign_scan(node: *mut ForeignScanState, e_flags: c_int) {
    unsafe {
        let relation = (*node).ss.ss_currentRelation;
        let relation_id = (*relation).rd_id;
        let tuple_desc = (*relation).rd_att;
        if (e_flags & EXEC_FLAG_EXPLAIN_ONLY as c_int) != 0 {
            return;
        }
        // 获取文件名（C字符串）
        let mut filename_raw = db721_get_option_value(
            relation_id,
            CString::new("filename")
                .expect("CString::new failed")
                .into_raw(),
        );
        let file_name = CStr::from_ptr(filename_raw)
            .to_str()
            .expect("convert filename to UTF-8 failed");
        let foreign_scan = (*node).ss.ps.plan as *mut ForeignScan;
        let foreign_private_list = (*foreign_scan).fdw_private as *mut List;
        let where_clause_list = (*foreign_scan).scan.plan.qual as *mut List;
        let pl_first_cell = l_nth_cell!(foreign_private_list, 0);
        let column_list = l_first!(pl_first_cell) as *mut List;
        let db_721 = DB721::open(PathBuf::from(file_name))
            .with_context(|| "failed to create db_721 in db721_begin_foreign_scan")
            .unwrap();
        let db721_scan_state =
            DB721ScanState::new(db_721, tuple_desc, column_list, where_clause_list);
        (*node).fdw_state = db721_scan_state as *mut c_void;
    }
}
// 主要用于成本估计，后续编写成本估计函数，现在为了简单起见，暂时不估计。
#[pg_guard]
pub extern "C" fn db721_get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
) {
    unsafe {
        let path = pg_sys::create_foreignscan_path(
            root,
            base_rel,
            ptr::null_mut(), // default pathtarget
            (*base_rel).rows,
            0.0,
            0.0,
            ptr::null_mut(), // no pathkeys
            ptr::null_mut(), // no outer rel either
            ptr::null_mut(), // no extra plan
            ptr::null_mut(), // no fdw_private data
        );
        pg_sys::add_path(base_rel, &mut ((*path).path));
    }
}
/// 生成plan的函数，主要工作是获取需要从文件中读取的列信息。
#[pg_guard]
pub extern "C" fn db721_get_foreign_plan(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: Oid,
    best_path: *mut pg_sys::ForeignPath,
    t_list: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    unsafe {
        let new_scan_clauses = extract_actual_clauses(scan_clauses, false);
        let column_list = db721_column_list(base_rel, foreign_table_id);
        let foreign_private_list = list_make1_impl(
            NodeTag_T_List,
            ListCell {
                ptr_value: column_list as *mut c_void,
            },
        );
        let foreign_scan = make_foreignscan(
            t_list,
            new_scan_clauses,
            (*base_rel).relid,
            ptr::null_mut(),
            foreign_private_list,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        );
        foreign_scan
    }
}
/// 进行迭代，每调用一次获取一行。
#[pg_guard]
pub extern "C" fn db721_iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    unsafe {
        let db721_scan_state = (def_option_ptr_mut!(*((*node).fdw_state as *mut Option<DB721ScanState>))) as *mut DB721ScanState;
        let tuple_table_slot = (*node).ss.ss_ScanTupleSlot;
        let tuple_desc = (*tuple_table_slot).tts_tupleDescriptor;
        // 结果数组
        let column_values = (*tuple_table_slot).tts_values;
        // 结果是否为null数组
        let column_nulls = (*tuple_table_slot).tts_isnull;
        let column_count = (*tuple_desc).natts;
        memset(
            column_values as *mut c_void,
            0,
            column_count as size_t * size_of::<Datum>() as size_t,
        );
        memset(
            column_nulls as *mut c_void,
            c_int::from(true),
            column_count as size_t * size_of::<bool>(),
        );
        (*((*tuple_table_slot).tts_ops)).clear.unwrap()(tuple_table_slot);
        // 读取下一行
        let next_row_found = db721_read_next_row(
            db721_scan_state,
            column_values,
            column_nulls,
        );
        if next_row_found{
            ExecStoreVirtualTuple(tuple_table_slot);
        }
        tuple_table_slot
    }
}
/// 利用replace手动触发drop机制，释放rust自动申请的堆内存。
/// 防止内存泄漏
#[pg_guard]
pub extern "C" fn db721_end_foreign_scan(node: *mut pg_sys::ForeignScanState) {
    unsafe {
        /// 利用replace手动触发drop，释放rust自动申请的堆内存。
        /// 防止内存泄漏
        std::mem::replace(
            &mut *((*node).fdw_state as *mut Option<DB721ScanState>),
            Option::<DB721ScanState>::None,
        );
    }
}
/// from foreign_table's option, get the filename
/// travel the options list, and match the key of option, return the value
#[pg_guard]
pub extern "C" fn db721_get_option_value(
    foreign_table_id: pgrx::pg_sys::Oid,
    option_name: *mut c_char,
) -> *mut c_char {
    return unsafe {
        let foreign_table = GetForeignTable(foreign_table_id);
        let mut option_value = ptr::null_mut();
        let raw_option_list = list_copy((*foreign_table).options);
        for i in 0..(*raw_option_list).length {
            let option_def = (*(((*raw_option_list).elements as usize + (i * 8) as usize)
                as *mut ListCell))
                .ptr_value as *mut DefElem;
            if option_def.is_null() {
                warning_log!("option_def_is_null");
            }
            let option_def_name = (*option_def).defname;
            if strncmp(option_def_name, option_name, NAMEDATALEN as size_t) == 0 {
                option_value = defGetString(option_def);
                break;
            }
        }
        if option_value.is_null() {
            warning_log!("option value get failed, is null");
        }
        option_value
    };
}

/// 获取所有需要读取的列信息
#[pg_guard]
pub extern "C" fn db721_column_list(base_rel: *mut RelOptInfo, foreign_table_id: Oid) -> *mut List {
    unsafe {
        let mut need_column_list: *mut List = ptr::null_mut();
        let mut column_list: *mut List = ptr::null_mut();
        let column_count: AttrNumber = (*base_rel).max_attr;
        let target_column_list = (*((*base_rel).reltarget)).exprs;
        let restrict_info_list = (*base_rel).baserestrictinfo;
        const WHOLE_ROW: AttrNumber = 0;
        // 访问relation之前先加锁
        let relation: Relation = relation_open(foreign_table_id, AccessShareLock as c_int);
        let tuple_desc: TupleDesc = (*relation).rd_att;
        // 首先获取在join以及projection中使用的column
        for i in 0..(*target_column_list).length {
            let list_cell = (*target_column_list).elements.add(i as usize);
            let target_expr = (*list_cell).ptr_value as *mut Node;
            let target_val_list = pull_var_clause(
                target_expr,
                PVC_RECURSE_AGGREGATES as c_int | PVC_RECURSE_PLACEHOLDERS as c_int,
            );
            need_column_list = list_union(need_column_list, target_val_list);
        }
        if !restrict_info_list.is_null(){
            // 然后获取在where子句中所使用的column
            for i in 0..(*restrict_info_list).length {
                let list_cell = l_nth_cell!(restrict_info_list, i) as *mut ListCell;
                let restrict_clause =
                    (*(l_first!(list_cell) as *mut RestrictInfo)).clause as *mut Node;
                if restrict_clause.is_null(){
                    warning_log!("restrict_clause is null");
                }
                let clause_column_list = pull_var_clause(
                    restrict_clause,
                    PVC_RECURSE_AGGREGATES as c_int | PVC_RECURSE_PLACEHOLDERS as c_int,
                );
                need_column_list = list_union(need_column_list, clause_column_list);
            }
        }
        // 清除重复的column
        for column_index in 1..column_count + 1 {
            let attr_form = (((*tuple_desc).attrs.as_mut_ptr()) as *mut FormData_pg_attribute)
                .add(column_index as usize - 1);
            if (*attr_form).attisdropped {
                continue;
            }
            let mut column = ptr::null_mut();
            for i in 0..(*need_column_list).length {
                let list_cell = (*need_column_list).elements.add(i as usize);
                let list_ind_cell = l_nth_cell!(need_column_list, i) as *mut ListCell;
                assert_eq!(list_cell, list_ind_cell);
                let need_column = (*list_cell).ptr_value as *mut Var;
                if (*need_column).varattno == column_index {
                    column = need_column;
                    break;
                } else if (*need_column).varattno == WHOLE_ROW {
                    let table_id = (*need_column).varno;
                    column = makeVar(
                        table_id,
                        column_index,
                        (*attr_form).atttypid,
                        (*attr_form).atttypmod,
                        (*attr_form).attcollation,
                        0,
                    );
                    break;
                }
            }
            if !column.is_null() {
                column_list = lappend(column_list, column as *mut c_void);
            }
        }
        // 访问完毕后解锁
        relation_close(relation, AccessShareLock as LOCKMODE);
        column_list
    }
}
/// 读取下一行
#[pg_guard]
pub extern "C" fn db721_read_next_row(
    scan_state: *mut DB721ScanState,
    column_values: *mut Datum,
    column_nulls: *mut bool,
) -> bool{
    unsafe {
        // set all column_null to true
        memset(
            column_nulls as *mut c_void,
            c_int::from(true),
            (*((*scan_state).column_list)).length as size_t * size_of::<bool>()
        );
        for index in 0..(*((*scan_state).column_list)).length{
            let list_cell = l_nth_cell!((*scan_state).column_list, index) as *mut ListCell;
            let column = l_first!(list_cell) as *mut Var;
            let column_index = (*column).varattno - 1;
            let column_name = (*scan_state).column_index_map_name.get(&column_index)
                .expect("get column name by column index failed");
            let column_iterator = (*scan_state).column_iterators.get_mut(column_name)
                .expect("get column iterator by column name failed");
            let next_val = column_iterator.next();
            match next_val{
                None => {
                    continue;
                },
                Some(DB721Type::Str(str)) => {
                    let text_p =  pgrx::rust_str_to_text_p(str.as_str());
                    *(column_values.add(column_index as usize)) = Datum::from(text_p.into_pg());
                },
                Some(DB721Type::Integer(val))  => {
                    let p_int = palloc0(size_of::<i32>()) as *mut i32;
                    *p_int = val;
                    *(column_values.add(column_index as usize)) = Datum::from(val);
                },
                Some(DB721Type::Float(val)) => {
                    let res = u32::from_ne_bytes(val.to_ne_bytes());
                    *(column_values.add(column_index as usize)) = Datum::from(res);
                }
            };
            *(column_nulls.add(column_index as usize)) = false;
        }
        // 只有全部都获取到的none才算获取完毕。
        if (*scan_state).is_end(){
            return false
        }
    }
    true
}
