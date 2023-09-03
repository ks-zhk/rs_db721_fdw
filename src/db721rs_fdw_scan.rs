/// this mod aims to impl some scan callbacks for db721 file
use crate::db721::{ColumnIterator, ColumnIteratorBuilder, DB721};
use libc::{c_uchar, size_t, strncmp};
use pgrx::pg_sys::{defGetString, list_concat, list_copy, list_union, palloc0, pull_var_clause, relation_open, scalararraysel, AccessShareLock, AttrNumber, BeginForeignScan_function, DefElem, ForEachState, ForeignScan, ForeignScanState, FormData_pg_attribute, GetForeignTable, List, ListCell, Node, Oid, PLpgSQL_stmt_foreach_a, PlannerInfo, RelOptInfo, Relation, RelationGetReplicaIndex, RestrictInfo, Size, TupleDescGetAttInMetadata, EXEC_FLAG_EXPLAIN_ONLY, NAMEDATALEN, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, TupleDesc, Var, makeVar, lappend, relation_close, LOCKMODE};
use pgrx::prelude::*;
use pgrx::{ereport, pg_guard, void_mut_ptr, PgList, PgLogLevel, NULL};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::io::Write;
use std::mem::size_of;
use std::ptr;
use std::str::Utf8Error;
struct DB721ScanState {
    db721: DB721,
    need_column: Vec<String>,
    now_column_iterator: ColumnIterator,
    now_column_name: String,
}
struct DB721FdwOptions {
    filename: String,
}
#[pg_guard]
pub extern "C" fn db721_get_foreign_rel_size(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
) {
    unsafe {
        (*base_rel).rows = 10000f64;
    }
}
#[pg_guard]
pub extern "C" fn db721_begin_foreign_scan(node: *mut ForeignScanState, e_flags: c_int) {
    unsafe {
        let relation = (*node).ss.ss_currentRelation;
        let relation_id = (*relation).rd_id;

        if (e_flags & EXEC_FLAG_EXPLAIN_ONLY as c_int) != 0 {
            return;
        }
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            "try to get filename"
        );
        let mut filename_raw = db721_get_option_value(
            relation_id,
            CString::new("filename")
                .expect("CString::new failed")
                .into_raw(),
        )
        .expect("get db721 foreign table filename failed, get NONE");
        let str = CStr::from_ptr(filename_raw)
            .to_str()
            .expect("convert filename to UTF-8 failed");
        todo!("get select column infos")
        // let scan_desc = node.ss.ss_currentScanDesc;
        // node.ss.ps.plan.targetlist
    }
    // current_relation.rd_node
    // // let tuple_desc = pgrx::pg_sys::
}
#[pg_guard]
pub extern "C" fn db721_get_foreign_paths(
    root: *mut pg_sys::PlannerInfo,
    base_rel: *mut pg_sys::RelOptInfo,
    foreign_table_id: pg_sys::Oid,
) {
    unsafe {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            "into db721_get_foreign_paths"
        );
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
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            "into db721_get_foreign_plan"
        );
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
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        "into db721_iterate_foreign_scan"
    );
    ptr::null_mut()
}
#[pg_guard]
pub extern "C" fn db721_end_foreign_scan(_node: *mut pg_sys::ForeignScanState) {
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        "into db721_end_foreign_scan"
    );
}
#[pg_guard]
pub extern "C" fn db721_get_option_value(
    foreign_table_id: pgrx::pg_sys::Oid,
    option_name: *mut c_char,
) -> Option<*mut c_char> {
    return unsafe {
        let foreign_table = GetForeignTable(foreign_table_id);
        // 悬垂指针
        let mut option_value: Option<*mut c_char> = None;
        let raw_option_list = list_copy((*foreign_table).options);
        for i in 0..(*raw_option_list).length {
            let option_def = (*(((*raw_option_list).elements as usize + (i * 8) as usize)
                as *mut ListCell))
                .ptr_value as *mut DefElem;
            if option_def.is_null() {
                ereport!(
                    PgLogLevel::WARNING,
                    PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
                    "option_def_is_null"
                );
            }
            let option_def_name = (*option_def).defname;
            if strncmp(option_def_name, option_name, NAMEDATALEN as size_t) == 0 {
                option_value = Some(defGetString(option_def));
                break;
            }
        }
        option_value
    };
}
#[pg_guard]
pub extern "C" fn db721_column_list(base_rel: *mut RelOptInfo, foreign_table_id: Oid) -> *mut List {
    unsafe {
        let mut need_column_list: *mut List = ptr::null_mut();
        let mut column_list: *mut List = ptr::null_mut();
        let column_count: AttrNumber = (*base_rel).max_attr;
        let target_column_list = (*((*base_rel).reltarget)).exprs;
        let restrict_info_list = (*base_rel).baserestrictinfo;
        const WHOLE_ROW: AttrNumber = 0;
        let relation: Relation = relation_open(foreign_table_id, AccessShareLock as c_int);
        let tuple_desc:TupleDesc = (*relation).rd_att;
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
        // 然后获取在where子句中所使用的column
        for i in 0..(*restrict_info_list).length {
            let list_cell = (*restrict_info_list).elements.add(i as usize);
            let restrict_clause =
                (*((*list_cell).ptr_value as *mut RestrictInfo)).clause as *mut Node;
            let clause_column_list = pull_var_clause(
                restrict_clause,
                PVC_RECURSE_AGGREGATES as c_int | PVC_RECURSE_PLACEHOLDERS as c_int,
            );
            need_column_list = list_union(need_column_list, clause_column_list);
        }
        for column_index in 1..column_count + 1 {
            let attr_form =
                (((*tuple_desc).attrs.as_mut_ptr()) as *mut FormData_pg_attribute).add(column_index as usize - 1);
            if (*attr_form).attisdropped {
                continue;
            }
            let mut column = ptr::null_mut();
            for i in 0..(*need_column_list).length{
                let list_cell = (*need_column_list).elements.add(i as usize);
                let need_column = (*list_cell).ptr_value as *mut Var;
                if (*need_column).varattno == column_index {
                    column = need_column;
                    break;
                }else if (*need_column).varattno == WHOLE_ROW {
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
            if !column.is_null(){
                column_list = lappend(column_list, column as *mut c_void);
            }
        }
        relation_close(relation, AccessShareLock as LOCKMODE);
        column_list
    }
}
// pub extern "C" fn db721_get_options(
//     foreign_table_id: Oid
// ) ->
