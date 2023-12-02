#include <sched.h>
#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
//#include "tatp_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/
int Query_queue::_next_tid;

void
Query_queue::init(workload * h_wl) {
	all_queries = new Query_thd * [g_thread_cnt];
	_wl = h_wl;
	_next_tid = 0;
	next_oid = 0;

#if WORKLOAD == YCSB
	ycsb_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpcc_buffer != NULL);
    //generate next o_id for district table
//    std::vector<uint64_t> random_insert_oids;
    uint64_t thread_insert_oids = 1000*100;
//    uint64_t init_oid = 3001;
//    uint64_t start_oid = init_oid + thread_insert_oids;
//    uint64_t end_oid = init_oid + (thread_id+1)*thread_insert_oids;
    for (uint64_t i = 0; i < thread_insert_oids; ++i) {
        random_insert_oids[i] = i+3001;
    }
    std::random_shuffle(random_insert_oids.begin(), random_insert_oids.end());
#elif WORKLOAD == TATP
	// TATP shares tpcc_buffer with TPCC
	assert(tpcc_buffer != NULL);
#endif
	int64_t begin = get_server_clock();
	pthread_t p_thds[g_thread_cnt - 1];
	for (UInt32 i = 0; i < g_thread_cnt - 1; i++) {
		pthread_create(&p_thds[i], NULL, threadInitQuery, this);
	}
	threadInitQuery(this);
	for (uint32_t i = 0; i < g_thread_cnt - 1; i++){
        pthread_join(p_thds[i], NULL);
	}
	int64_t end = get_server_clock();
	printf("Query Queue Init Time %f\n", 1.0 * (end - begin) / 1000000000UL);

}

void
Query_queue::init_per_thread(int thread_id) {
	all_queries[thread_id] = (Query_thd *) mem_allocator.alloc(sizeof(Query_thd), thread_id);
	all_queries[thread_id]->init(_wl, thread_id, random_insert_oids, next_oid);
}

base_query *
Query_queue::get_next_query(uint64_t thd_id) {
	base_query * query = all_queries[thd_id]->get_next_query();
	return query;
}

void *Query_queue::threadInitQuery(void * This) {
	Query_queue * query_queue = (Query_queue *)This;
	uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);

	// set cpu affinity
#if CC_ALG == MICA
  ::mica::util::lcore.pin_thread(tid);
#else
	set_affinity(tid);
#endif

    mem_allocator.register_thread(tid);

    query_queue->init_per_thread(tid);
    return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/

void Query_thd::init(workload * h_wl, int thread_id, std::array<uint64_t, 1000*100> &insert_oids,  uint64_t  next_oid) {
	uint64_t request_cnt;
	q_idx = 0;
	// request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
	request_cnt = WARMUP + MAX_TXN_PER_PART + ABORT_BUFFER_SIZE * 2;
#if WORKLOAD == YCSB
	queries = (ycsb_query *) mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);
	srand48_r(thread_id + 1, &buffer);

    //generate insert keys
//    size_t thread_insert_keys = g_synth_table_size;
    size_t thread_insert_keys = MAX_TXN_PER_PART + ABORT_BUFFER_SIZE * 2;
    std::vector<uint64_t> random_insert_keys;
    if (g_insert_perc >0) {
        uint64_t table_size_ = g_synth_table_size / g_virtual_part_cnt;
        uint64_t start_key = table_size_ + thread_id*thread_insert_keys;
        uint64_t end_key = table_size_ + (thread_id+1)*thread_insert_keys;
        for (uint64_t i = start_key; i < end_key; ++i)
        {
            random_insert_keys.push_back(i  +1);
        }
        //random the insert keys
        std::random_shuffle(random_insert_keys.begin(), random_insert_keys.end());
    }
    uint64_t idx_inst = 0;

#elif WORKLOAD == TPCC
	queries = (tpcc_query *) mem_allocator.alloc(sizeof(tpcc_query) * request_cnt, thread_id);


#elif WORKLOAD == TATP
	queries = (tatp_query *) mem_allocator.alloc(sizeof(tatp_query) * request_cnt, thread_id);
#else
	assert(false);
#endif

	for (uint64_t qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB
		new(&queries[qid]) ycsb_query();
        queries[qid].init(thread_id, h_wl, this, random_insert_keys, qid);
//		queries[qid].init(thread_id, h_wl, this, random_insert_keys, idx_inst);
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
        next_oid++;
		uint64_t nexto_id = insert_oids[next_oid];
		queries[qid].init(thread_id, h_wl, nexto_id);
#elif WORKLOAD == TATP
		new(&queries[qid]) tatp_query();
		queries[qid].init(thread_id, h_wl);
#endif
	}
}

base_query *Query_thd::get_next_query() {
	base_query * query = &queries[q_idx++];
	return query;
}
