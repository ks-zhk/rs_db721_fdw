use std::ffi::{c_char, c_int, CStr, CString};
use std::io::Write;
use std::mem::size_of;
use std::ptr;
use std::str::Utf8Error;
use libc::{c_uchar, size_t, strncmp};
use pgrx::pg_sys::{BeginForeignScan_function, DefElem, defGetString, EXEC_FLAG_EXPLAIN_ONLY, ForEachState, ForeignScan, ForeignScanState, GetForeignTable, list_concat, list_copy, ListCell, NAMEDATALEN, Oid, palloc0, PlannerInfo, PLpgSQL_stmt_foreach_a, RelationGetReplicaIndex, RelOptInfo, scalararraysel, Size};
use pgrx::{ereport, pg_guard, PgList, PgLogLevel, void_mut_ptr};
use pgrx::prelude::*;
/// this mod aims to impl some scan callbacks for db721 file
use crate::db721::{ColumnIterator, ColumnIteratorBuilder, DB721};
struct DB721ScanState{
    db721: DB721,
    need_column: Vec<String>,
    now_column_iterator: ColumnIterator,
    now_column_name: String,
}
struct DB721FdwOptions{
    filename: String,
}
#[pg_guard]
pub extern "C"  fn db721_get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
) {
    unsafe {
        ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "into db721_get_foreign_rel_size");
        (*base_rel).rows = 10000f64;
    }
}
#[pg_guard]
pub extern "C"  fn db721_begin_foreign_scan(
    node: *mut ForeignScanState,
    e_flags: c_int,
) {
    unsafe{
        ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "into db721_begin_foreign_scan");
        let relation = (*node).ss.ss_currentRelation;
        let relation_id = (*relation).rd_id;

        if (e_flags & EXEC_FLAG_EXPLAIN_ONLY as c_int) != 0{
            return;
        }
        // // let fs_state = palloc0(size_of::<DB721ScanState>() as Size)
        // //     as *mut DB721ScanState;
        // (*node).fdw_state = fs_state as void_mut_ptr;
        ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "try to get filename");
        let mut filename = db721_get_option_value(
            relation_id,
            CString::new("filename").expect("CString::new failed").into_raw()
        ).unwrap();
        // while let ok =  filename.read(){
        //     if ok == 0i8{
        //         break
        //     }
        //     ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, (ok as i8).to_string());
        //     filename = (filename as usize + 1usize) as *mut c_char;
        // }
        let str = CStr::from_ptr(filename);

        match str.to_str(){
            Ok(res) => {
                ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, res);
            }
            Err(err) => {
                ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, err.to_string());
            }
        };
    }
    // current_relation.rd_node
    // // let tuple_desc = pgrx::pg_sys::
}
#[pg_guard]
pub extern "C" fn db721_get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
){
    unsafe {
        ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "into db721_get_foreign_paths");
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
        ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "into db721_get_foreign_plan");
        pg_sys::make_foreignscan(
            t_list,
            scan_clauses,
            (*base_rel).relid,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            outer_plan,
        )
    }
}
#[pg_guard]
pub extern "C" fn db721_iterate_foreign_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "into db721_iterate_foreign_scan");
    ptr::null_mut()
}
#[pg_guard]
pub extern "C" fn db721_end_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "into db721_end_foreign_scan");
}
#[pg_guard]
pub extern "C" fn db721_get_option_value(
    foreign_table_id: pgrx::pg_sys::Oid,
    option_name: *mut c_char
) -> Option<*mut c_char>{
    // let mut file = std::fs::OpenOptions::new().read(true).write(true).create(true).open("./test_log.log").unwrap();
    // file.write_all("in the db721_get_option_value".as_ref()).unwrap();
    return unsafe {

        let foreign_table = GetForeignTable(foreign_table_id);
        // 悬垂指针
        let mut option_value:Option<*mut c_char> = None;
        let raw_option_list = list_copy((*foreign_table).options);
        let option_list = pgrx::PgList::<ListCell>::from_pg(raw_option_list);
        let option_list_iter = option_list.iter_ptr();
        for i in 0..(*raw_option_list).length{
            let option_def = (*(((*raw_option_list).elements as usize + (i*8) as usize) as *mut ListCell)).ptr_value as *mut DefElem;
            if option_def.is_null(){
                ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "option_def_is_null");
            }else{
                ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, (option_def as usize).to_string());
            }
            let shit = (*option_def);

            ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "here");
            let option_def_name = (*option_def).defname;
            // ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "here3");
            // ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, CString::from_raw(option_def_name).to_str().unwrap());
            if strncmp(option_def_name, option_name, NAMEDATALEN as size_t) == 0{
                option_value = Some(defGetString(option_def));
                break;
            }

        }
        // ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, CString::from_raw(option_name).to_str().unwrap());
        // for option in option_list_iter{
        //     ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, (option as usize).to_string());
        //     let option_def = ((*option).ptr_value) as *mut DefElem;
        //     if option_def.is_null(){
        //         ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "option_def_is_null");
        //     }else{
        //         ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, (option_def as usize).to_string());
        //     }
        //     let shit = (*option_def);
        //
        //     ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "here");
        //     let option_def_name = (*option_def).defname;
        //     // ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "here3");
        //     // ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, CString::from_raw(option_def_name).to_str().unwrap());
        //     if strncmp(option_def_name, option_name, NAMEDATALEN as size_t) == 0{
        //         option_value = Some(defGetString(option_def));
        //         break;
        //     }
        // }
        if let None = option_value{
            ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "get option value failed, search failed");
        }else{
            ereport!(PgLogLevel::WARNING, PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION, "success get option value");
        }
        option_value
    }
}

// pub extern "C" fn db721_get_options(
//     foreign_table_id: Oid
// ) ->
