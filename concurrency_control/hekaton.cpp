#include "txn.h"
#include "row.h"
#include "row_hekaton.h"
#include "manager.h"

#if CC_ALG==HEKATON
//
//RC txn_man::validate_hekaton(RC rc, const Func& func)
//{
//	uint64_t starttime = get_sys_clock();
//	INC_STATS(get_thd_id(), debug1, get_sys_clock() - starttime);
//	ts_t commit_ts = glob_manager->get_ts(get_thd_id());
//	// validate the read set.
//#if ISOLATION_LEVEL == SERIALIZABLE
//	if (rc == RCOK) {
//		for (int rid = 0; rid < row_cnt; rid ++) {
//			if (accesses[rid]->type == WR)
//				continue;
//			rc = accesses[rid]->orig_row->manager->prepare_read(this, accesses[rid]->data, commit_ts);
//			if (rc == Abort)
//				break;
//		}
//	}
//#endif
//
//	rc = apply_index_changes(rc);
//
//	// postprocess
//  if (rc == RCOK) {
//    for (UInt32 i = 0; i < insert_cnt; i++) {
//      row_t * row = insert_rows[i];
//      row->manager->set_ts(commit_ts);
//    }
//  }
//	for (int rid = 0; rid < row_cnt; rid ++) {
//		if (accesses[rid]->type == RD)
//			continue;
//		accesses[rid]->orig_row->manager->post_process(this, commit_ts, rc);
//	}
//	return rc;
//}

#endif
