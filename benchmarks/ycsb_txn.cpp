#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "storage/table.h"
#include "storage/row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

void ycsb_txn_man::init(thread_t* h_thd, workload* h_wl, uint64_t thd_id) {
  txn_man::init(h_thd, h_wl, thd_id);
  _wl = (ycsb_wl*)h_wl;
}

RC ycsb_txn_man::run_txn(base_query* query) {
  RC rc;
  ycsb_query* m_query = (ycsb_query*)query;
  ycsb_wl* wl = (ycsb_wl*)h_wl;
  row_cnt = 0;
  table_t *table_ = _wl->the_table;
  Catalog *schema_ =  table_->schema;

  const uint64_t kColumnSize = MAX_TUPLE_SIZE / FIELD_PER_TUPLE;

  uint64_t v = 0;

#if CC_ALG == MICA
  mica_tx->begin(false);
#endif

  for (uint32_t rid = 0; rid < m_query->request_cnt; rid++) {
    ycsb_request* req = &m_query->requests[rid];
    int part_id = wl->key_to_part(req->key);
    uint64_t column = req->column;
    bool finish_req = false;
    UInt32 iteration = 0;
    while (!finish_req) {
      access_t type = req->rtype;
      row_t* row = nullptr;
      if (type == RD ||  type == WR){
         row = search(_wl->the_index, req->key, part_id, type);
      } else if(type == INS){
          uint64_t out_row_id;
          uint64_t key_inst = req->key;
          auto ret = insert_row(table_, row, part_id, out_row_id, key_inst);
#if AGGRESSIVE_INLINING == false
          ret = insert_idx(_wl->the_index, (uint64_t)req->key, row, part_id);
#endif
          if (!ret){
              rc = Abort;
              goto final;
          }
          for (UInt32 fid = 0; fid < schema_->get_field_cnt(); fid++) {
              char value[6] = "hello";
              row->set_value(fid, value);
          }
      }

      if (row == NULL) {
//        printf("null, abort. \n");
        rc = Abort;
        goto final;
      }

      // Computation //
      // Only do computation when there are more than 1 requests.
      /*if (m_query->request_cnt > 1)*/
      {
        if (req->rtype == RD || req->rtype == SCAN) {
          //                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
#if !TPCC_CF
          const char* data = row->get_data() + column * kColumnSize;
//          string ss(data);
//          printf("ss: %s, \n", ss.c_str());
#else
          const char* data = row->cf_data[0] + column * kColumnSize;
#endif
          for (uint64_t j = 0; j < kColumnSize; j += 64)
            v += static_cast<uint64_t>(data[j]);
          v += static_cast<uint64_t>(data[kColumnSize - 1]);
          //                  }
        } else if(req->rtype == WR) {
          assert(req->rtype == WR);
          //					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
          //int fid = 0;
          // char * data = row->get_data();
#if AGGRESSIVE_INLINING
#else
#if !TPCC_CF
          char* data = row->get_data() + column * kColumnSize;
#else
          char* data = row->cf_data[0] + column * kColumnSize;
#endif
          for (uint64_t j = 0; j < kColumnSize; j += 64) {
            v += static_cast<uint64_t>(data[j]);
            data[j] = static_cast<char>(v);
          }
          v += static_cast<uint64_t>(data[kColumnSize - 1]);
          data[kColumnSize - 1] = static_cast<char>(v);
          //*(uint64_t *)(&data[fid * 10]) = 0;
          // memcpy(data, v, column_size);
          //					}
#endif
        }
      }

      iteration++;
      if (req->rtype == RD || req->rtype == WR || req->rtype == INS || iteration == req->scan_len)
        finish_req = true;
    }
  }
  rc = RCOK;

final:

  rc = finish(rc, [](char* data) {
      uint64_t v = 0;
      for (uint64_t j = 0; j < kColumnSize; j += 64) {
          v += static_cast<uint64_t>(data[j]);
          data[j] = static_cast<char>(v);
      }
      v += static_cast<uint64_t>(data[kColumnSize - 1]);
      data[kColumnSize - 1] = static_cast<char>(v);
      return true;
  });

//    printf("finish : %u. \n", rc);

  return rc;
}
