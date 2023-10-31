#pragma once

#include <global.h>
//#include "index_hash.h"
#include "btree_store.h"

// TODO sequential scan is not supported yet.
// only index access is supported for table.
class Catalog;
class row_t;
class IndexBtree;

class table_t
{
public:
	void init(Catalog * schema, uint64_t part_cnt);
	// row lookup should be done with index. But index does not have
	// records for new rows. get_new_row returns the pointer to a
	// new row.
	RC get_new_row(row_t *& row); // this is equivalent to insert()
	RC get_new_row( row_t *& row, uint64_t part_id, uint64_t &row_id, uint64_t idx_key=0);
    RC get_new_row_wl(row_t*& row, uint64_t part_id, uint64_t& row_id, uint64_t idx_key=0);

	void delete_row(); // TODO delete_row is not supportet yet

	// uint64_t get_table_size() { return cur_tab_size; };
	Catalog * get_schema() { return schema; };
	const char * get_table_name() { return table_name; };

	Catalog * 		schema{};
	void add_table_index(IndexBtree *the_index){
	    this->table_index = the_index;
	}
    IndexBtree* get_table_index(){return table_index;}

#if CC_ALG == MICA
	MICADB* mica_db{};
	std::vector<MICATable*> mica_tbl{};
#endif

private:
	const char * 	table_name;
	// uint64_t  		cur_tab_size;
	uint64_t        part_cnt;
	char 			pad[CL_SIZE - sizeof(void *)*3];
//    IndexHash*      table_index;
    IndexBtree*     table_index;
};
