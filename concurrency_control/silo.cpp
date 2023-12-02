#include "txn.h"
#include "row.h"
#include "row_silo.h"

#if CC_ALG == SILO
RC txn_man::validate_silo()
{
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	int write_set[wr_cnt];
	std::vector<Access *> insrt_set;
	insrt_set.clear();
	int cur_wr_idx = 0;
#if ISOLATION_LEVEL != REPEATABLE_READ
	int read_set[row_cnt - wr_cnt];
	int cur_rd_idx = 0;
#endif
	for (int rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR){
            write_set[cur_wr_idx ++] = rid;
		} else if(accesses[rid]->type == INS){
            insrt_set.emplace_back(accesses[rid]);
		}

#if ISOLATION_LEVEL != REPEATABLE_READ
		else
			read_set[cur_rd_idx ++] = rid;
#endif
	}

	// bubble sort the write_set, in primary key order
	for (int i = wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ write_set[j] ]->orig_row->get_primary_key() >
				accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}

	int num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;
	if (_pre_abort) {
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
                printf("abort 6. \n");
				rc = Abort;
				goto final;
			}
		}
#if ISOLATION_LEVEL != REPEATABLE_READ
		for (int i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#endif
	}

	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (int i = 0; i < wr_cnt; i++) {
                assert(accesses[write_set[i]]->type == WR);
				row_t * row = accesses[write_set[i]]->orig_row;
				if (!row->manager->try_lock()){
                    break;
				}
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != accesses[write_set[i]]->tid)
				{
                    printf("abort 3. \n");
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == wr_cnt){
                done = true;
			}else {
				for (int i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->release();
				if (_pre_abort) {
					num_locks = 0;
					for (int i = 0; i < wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
                            printf("abort 4. \n");
							rc = Abort;
							goto final;
						}
					}
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (int i = 0; i < row_cnt - wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
				usleep(1);
			}
		}
	} else {
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			row->manager->lock();
			num_locks++;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
                printf("abort 5. \n");
				rc = Abort;
				goto final;
			}
		}
	}

	// validate rows in the read set
#if ISOLATION_LEVEL != REPEATABLE_READ
	// for repeatable_read, no need to validate the read set.
	for (int i = 0; i < row_cnt - wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, false);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
#endif
	// validate rows in the write set
	for (int i = 0; i < wr_cnt; i++) {
		Access * access = accesses[ write_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, true);
		if (!success) {
            printf("abort 2. \n");
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	if (max_tid > _cur_tid){
        _cur_tid = max_tid + 1;
	} else{
        _cur_tid ++;
	}

final:
#if AGGRESSIVE_INLINING == false
	rc = apply_index_changes(rc);
#endif
	if (rc == Abort) {
	    printf("abort 1. \n");
		for (int i = 0; i < num_locks; i++){
            accesses[ write_set[i] ]->orig_row->manager->release();
		}
		cleanup(rc);
	} else {
#if AGGRESSIVE_INLINING
        for (int i = 0; i < insrt_set.size(); ++i) {
            assert(insrt_set[i]->type == INS);
            row_t *insr = insrt_set[i]->data;
            if (insr == nullptr) continue;
            insr->manager->set_tid(_cur_tid);
        }
#else
        for (UInt32 i = 0; i < insert_cnt; i++) {
			row_t * row = insert_rows[i];
            row->manager->set_tid(_cur_tid);  // unlocking is done as well
		}
#endif

		for (int i = 0; i < wr_cnt; i++) {
			Access * access = accesses[write_set[i]];
#if AGGRESSIVE_INLINING
            read_index_again(write_set[i]);        //read again, to get the real position
            access->orig_row->copy(access->data);  //copy row.data
            #if BUFFERING
                 access->orig_row->is_updated = false;
            #endif
#else
			access->orig_row->manager->write(access->data, _cur_tid );
#endif

			accesses[ write_set[i] ]->orig_row->manager->release();
		}
		cleanup(rc);
	}
	return rc;
}
#endif
