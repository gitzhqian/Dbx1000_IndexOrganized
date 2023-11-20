#pragma once
#ifndef MICA_TRANSACTION_CBTREE_INDEX_IMPL_H_
#define MICA_TRANSACTION_CBTREE_INDEX_IMPL_H_

#include <system/helper.h>

#if CC_ALG == MICA
namespace mica {
namespace transaction {
template <class StaticConfig>
CBtreeIndex<StaticConfig>::CBtreeIndex(
        DB<StaticConfig>* db, Table<StaticConfig>* main_tbl, Table<StaticConfig>* idx_tbl)
        : db_(db), main_tbl_(main_tbl), idx_tbl_(idx_tbl) {
}

template <class StaticConfig>
bool CBtreeIndex<StaticConfig>::init(Transaction<StaticConfig>* txn, uint32_t key_size ){
    ParameterSet param(SPLIT_THRESHOLD, MERGE_THRESHOLD, DRAM_BLOCK_SIZE, PAYLOAD_SIZE, KEY_SIZE);
    auto ret = Initt(param, txn, key_size);
    return ret;
}

template <class StaticConfig>
bool CBtreeIndex<StaticConfig>::Initt(ParameterSet param, Transaction<StaticConfig>* txn, uint32_t key_size )  {
    this->parameters = param;
    this->key_sz = key_size;

    root = reinterpret_cast<BaseNode<StaticConfig> *>(_mm_malloc(sizeof(parameters.leaf_node_size), CL_SIZE) );
    LeafNode<StaticConfig> **root_node = reinterpret_cast<LeafNode<StaticConfig> **>(&root);
    LeafNode<StaticConfig>::New(root_node, parameters.leaf_node_size );

    return true;
}


template <class StaticConfig>
bool CBtreeIndex<StaticConfig>::TraverseToTarget( Stack<StaticConfig> *stack, uint64_t key,
                                                  uint32_t key_size, AggressiveRowHead<StaticConfig> ** messge_upt,
                                                  LeafNode<StaticConfig> **l_node,
                                                  InternalNode<StaticConfig> **i_node,
                                                  bool le_child) {
    BaseNode<StaticConfig> *node = GetRootNodeSafe();
    __builtin_prefetch((const void *) (root), 0, 1);

    if (stack) {
        stack->SetRoot(node);
    }
    InternalNode<StaticConfig> *parent = nullptr;
    uint32_t meta_index = 0;
    assert(node);
    while (!node->IsLeaf()) {
        parent = reinterpret_cast<InternalNode<StaticConfig> *>(node);
        if (parent == nullptr) return RC::ERROR;
        //if find in the inner node, and txn'ts > version's begin
        //else goto the leaf node (if WR, return in the leaf node)
        //else goto the version store (if RD, may return in the version store)
        auto buffer_ = parent->update_messages;
        if (!buffer_->empty()){
//            printf("inner node buffer size: %lu. \n", buffer_->size());
            auto fin = buffer_->find(key);
            if (fin != buffer_->end()){
                auto mess = fin->second;
                *messge_upt = mess;
                *i_node = parent;
                return RC::RCOK;
            }
        }
        //binary search in inner node
        meta_index = parent->GetChildIndex(reinterpret_cast<char *>(&key), key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);
        if (node == nullptr) return RC::ERROR;
        assert(node);
    }

    *l_node = reinterpret_cast<LeafNode<StaticConfig> *>(node);

    return RC::RCOK;
}

template <class StaticConfig>
LeafNode<StaticConfig> *CBtreeIndex<StaticConfig>::TraverseToLeaf( Stack<StaticConfig> *stack,
                                                                   uint64_t key, uint32_t key_size,
                                                                   bool le_child) {
    static const uint32_t kCacheLineSize = 64;
    BaseNode<StaticConfig> *node = GetRootNodeSafe();
    __builtin_prefetch((const void *) (root), 0, 1);

    if (stack) {
        stack->SetRoot(node);
    }
    InternalNode<StaticConfig> *parent = nullptr;
    uint32_t meta_index = 0;
    assert(node);
    while (!node->IsLeaf()) {
        parent = reinterpret_cast<InternalNode<StaticConfig> *>(node);
        //binary search in inner node
        meta_index = parent->GetChildIndex(reinterpret_cast<const char *>(&key),
                                           key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);
//        for (uint32_t i = 0; i < node->header.size / kCacheLineSize; ++i) {
//            __builtin_prefetch((const void *)((char *)node + i * kCacheLineSize), 0, 1);
//        }
        if (node == nullptr){
            return nullptr;
        }
        assert(node);
        if(stack != nullptr){
            stack->Push(parent, meta_index);
        }
    }

//    for (uint32_t i = 0; i < parameters.leaf_node_size / kCacheLineSize; ++i) {
//        __builtin_prefetch((const void *)((char *)node + i * kCacheLineSize), 0, 3);
//    }
    return reinterpret_cast<LeafNode<StaticConfig> *>(node);
}

template <class StaticConfig>
bool CBtreeIndex<StaticConfig>::ChooseChildToPushdown(BaseNode<StaticConfig> *node_i,
                                                      LeafNode<StaticConfig> **node_l ){
    auto sz = node_i->header.sorted_count;
    if (sz <=0 || node_i->update_messages->size() <= MESSAGE_COUNT){
        node_i->UnFreeze();
        return false;
    }
    assert(sz >= 2);
    auto upt_messages = node_i->update_messages;
    auto upt_sz = upt_messages->size();
    if (upt_sz <= MESSAGE_COUNT) {
        node_i->UnFreeze();
        return false;
    }
    uint32_t max_size = 0;
    uint64_t child = 0;
    auto itrb = upt_messages->begin();
    auto itre = upt_messages->end();
    uint64_t key_1 = 0;
    uint64_t key_2 = 0;
    auto itr1 = upt_messages->begin();
    auto itr2 = upt_messages->end();
    for (int i = 0; i < sz; ++i) {
        auto meta = node_i->row_meta[i];
        char *m_key = nullptr;
        uint64_t m_payload = 0;
        node_i->GetRawRow(meta, nullptr, &m_key, &m_payload);

        if (m_key == nullptr){
            itr1 = upt_messages->begin();
        } else{
            key_1 = *reinterpret_cast<uint64_t *>(m_key);
            itr1 = upt_messages->lower_bound(key_1);
        }

        if ((i+1) >= sz){
            itr2 = upt_messages->end();
        } else{
            auto next_meta = node_i->row_meta[i+1];
            char *m_key2 = nullptr;
            node_i->GetRawRow(next_meta, nullptr, &m_key2, nullptr);
            key_2 = *reinterpret_cast<uint64_t *>(m_key2);
            itr2 = upt_messages->lower_bound(key_2);
        }

        auto dist = distance(itr1, itr2);
        if (dist > max_size){
            itrb = itr1;
            itre = itr2;
            child = m_payload;
            max_size = dist;
        }
    }

    //insert into child's buffer
    auto node_child = reinterpret_cast<BaseNode<StaticConfig> *>(child);
    if (node_child == nullptr) {
        node_i->UnFreeze();
        return false;
    }

    //insert upts to child
    if (node_child->IsFrozen()){
        node_i->UnFreeze();
        return false;
    }
    thread_local bool frozen_by_me1 = node_child->Freeze();
    assert(frozen_by_me1);
    node_child->update_messages->insert(itrb, itre);

    //clear this buffer
    auto curr_nd_sz = node_i->update_messages->size();
    node_i->update_messages->erase(itrb, itre);
    node_i->UnFreeze();

    if (node_child->is_leaf){
        node_child->UnFreeze();
        *node_l = reinterpret_cast<LeafNode<StaticConfig> *>(node_child);
        assert((*node_l)->is_leaf);
    }else{
        if (node_child->update_messages->size() > MESSAGE_COUNT)
        {
            auto ret = ChooseChildToPushdown( node_child, node_l);
            if (!ret){
                node_child->UnFreeze();
                return false;
            }
        }else{
            node_child->UnFreeze();
        }
    }

    return true;
}

template <class StaticConfig>
bool CBtreeIndex<StaticConfig>::ChangeRoot(uint64_t expected_root_addr,
                                           uint64_t new_root_addr) {
    bool ret = ATOM_CAS(*reinterpret_cast<uint64_t *>(&root), expected_root_addr, new_root_addr);

    assert(ret);

    return ret;
}

template <class StaticConfig>
ReturnCode CBtreeIndex<StaticConfig>::Insert(uint64_t key, uint32_t key_size,
                                             AggressiveRowHead<StaticConfig>* &row_head,
                                             uint64_t payload_ptr, uint32_t payload_size,
                                             Conflicts<StaticConfig> *conflicts) {
    thread_local Stack<StaticConfig> stack;
    stack.tree = this;
    uint64_t freeze_retry = 0;
    LeafNode<StaticConfig> *node = nullptr;

    while(true) {
        stack.Clear();

        node = TraverseToLeaf(&stack, key, key_size);
        if(node == nullptr){
            return ReturnCode::NotFound() ;
        }

        // 1.Try to frozen the node
        thread_local bool frozen_by_me = false;
        while(!node->IsFrozen()) {
            frozen_by_me = node->Freeze();
        }
        if(!frozen_by_me) {
            if (++freeze_retry <= MAX_FREEZE_RETRY) {
                continue;
            }
            if (!frozen_by_me){
                return ReturnCode::NodeFrozen();
            }
        }

        // 2.Try to insert to the leaf node
        auto rc = node->Insert_LF(key, key_size, row_head, payload_ptr,
                                 payload_size, parameters.leaf_node_size );
        if(rc.IsOk() || rc.IsKeyExists()) {
            node->UnFreeze();
            return rc;
        }

        assert( rc.IsNotEnoughSpace());
        assert(node->IsFrozen());

        bool backoff = (freeze_retry <= MAX_FREEZE_RETRY);
        // Should split and we have three cases to handle:
        // 1. Root node is a leaf node - install [parent] as the new root
        // 2. We have a parent but no grandparent - install [parent] as the new root
        // 3. We have a grandparent - update the child pointer in the grandparent
        //    to point to the new [parent] (might further cause splits up the tree)
        char *b_r = nullptr;
        char *b_l = nullptr;
        char *b_pt ;
        LeafNode<StaticConfig> **ptr_r = reinterpret_cast<LeafNode<StaticConfig> **>(&b_r);
        LeafNode<StaticConfig> **ptr_l = reinterpret_cast<LeafNode<StaticConfig> **>(&b_l);
        InternalNode<StaticConfig> **ptr_parent = reinterpret_cast<InternalNode<StaticConfig> **>(&b_pt);

        bool should_proceed;
        should_proceed = node->PrepareForSplit(
                stack, parameters.split_threshold, key_size,  payload_size,
                ptr_l, ptr_r, ptr_parent, backoff,  conflicts);

        if (!should_proceed) {
            if (b_r != nullptr){
                memset(b_r, 0 , parameters.leaf_node_size);
                _mm_free(b_r);
            }
            if (b_l != nullptr){
                memset(b_l, 0 , parameters.leaf_node_size);
                _mm_free(b_l );
            }
            continue;
        }

        assert(*ptr_parent);
        auto node_parent = reinterpret_cast<uint64_t>(*ptr_parent);

        auto *top = stack.Pop();
        InternalNode<StaticConfig> *old_parent = nullptr;
        if (top) {
            old_parent = reinterpret_cast<InternalNode<StaticConfig> *>(top->node);
        }

        top = stack.Pop();
        InternalNode<StaticConfig> *grand_parent = nullptr;
        if (top) {
            grand_parent = reinterpret_cast<InternalNode<StaticConfig> *>(top->node);
        }

        if (grand_parent) {
            M_ASSERT(old_parent, "BTree Insert fail grand parent.");
            // There is a grand parent. We need to swap out the pointer to the old
            // parent and install the pointer to the new parent.
            auto result = grand_parent->Update(top->node->GetRowMeta(top->meta_index),
                                               old_parent, *ptr_parent);
            if (!result.IsOk()) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    _mm_free(b_r );
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    _mm_free(b_l );
                }

                return ReturnCode::CASFailure();
            }
        } else {
            // No grand parent or already popped out by during split propagation
            // uint64 pointer, Compare and Swap operation
            bool result = ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), node_parent);
            if (!result) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    _mm_free(b_r );
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    _mm_free(b_l );
                }

                return ReturnCode::CASFailure();
            }
        }

        assert(should_proceed);
        if (should_proceed){
            memset(reinterpret_cast<char *>(node), 0 , parameters.leaf_node_size);
            _mm_free(reinterpret_cast<char *>(node) );
        }
        if (old_parent) {
            _mm_free(reinterpret_cast<char *>(old_parent) );
        }
    }
}

template <class StaticConfig>
ReturnCode CBtreeIndex<StaticConfig>::Insert_Append(uint64_t key, uint32_t key_size,
                                                   AggressiveRowHead<StaticConfig>* &row_head,
                                                   uint32_t payload_size,
                                                   Conflicts<StaticConfig> *conflicts) {
#if AGGRESSIVE_INLINING
    thread_local Stack<StaticConfig> stack;
    stack.tree = this;
    uint64_t freeze_retry = 0;
    LeafNode<StaticConfig> *node = nullptr;

    while(true) {
        stack.Clear();

        node = TraverseToLeaf(&stack, key, key_size);
        if(node == nullptr){
            return ReturnCode::NotFound() ;
        }

        // 1.Try to frozen the node
        thread_local bool frozen_by_me = false;
        while(!node->IsFrozen()) {
            frozen_by_me = node->Freeze();
        }
        if(!frozen_by_me) {
            if (++freeze_retry <= MAX_FREEZE_RETRY) {
                continue;
            }
            if (!frozen_by_me){
                return ReturnCode::NodeFrozen();
            }
        }

        // 2.Try to insert to the leaf node
        auto rc = node->Insert_Append_LF(key, key_size, row_head, payload_size, parameters.leaf_node_size );
        if(rc.IsOk() || rc.IsKeyExists() ) {
            return rc;
        }

        bool backoff = (freeze_retry <= MAX_FREEZE_RETRY);
        assert( rc.IsNotEnoughSpace());
        assert(node->IsFrozen());
        // Should split and we have three cases to handle:
        // 1. Root node is a leaf node - install [parent] as the new root
        // 2. We have a parent but no grandparent - install [parent] as the new root
        // 3. We have a grandparent - update the child pointer in the grandparent
        //    to point to the new [parent] (might further cause splits up the tree)
        char *b_r = nullptr;
        char *b_l = nullptr;
        char *b_pt ;
        LeafNode<StaticConfig> **ptr_r = reinterpret_cast<LeafNode<StaticConfig> **>(&b_r);
        LeafNode<StaticConfig> **ptr_l = reinterpret_cast<LeafNode<StaticConfig> **>(&b_l);
        InternalNode<StaticConfig> **ptr_parent = reinterpret_cast<InternalNode<StaticConfig> **>(&b_pt);

        bool should_proceed;
        should_proceed = node->PrepareForSplit(
                stack, parameters.split_threshold, key_size,  payload_size,
                ptr_l, ptr_r, ptr_parent, backoff, conflicts);

        if (!should_proceed) {
            if (b_r != nullptr){
                memset(b_r, 0 , parameters.leaf_node_size);
                _mm_free(b_r);
            }
            if (b_l != nullptr){
                memset(b_l, 0 , parameters.leaf_node_size);
                _mm_free(b_l);
            }
            continue;
        }

        assert(*ptr_parent);
        auto node_parent = reinterpret_cast<uint64_t>(*ptr_parent);

        auto *top = stack.Pop();
        InternalNode<StaticConfig> *old_parent = nullptr;
        if (top) {
            old_parent = reinterpret_cast<InternalNode<StaticConfig> *>(top->node);
        }

        top = stack.Pop();
        InternalNode<StaticConfig> *grand_parent = nullptr;
        if (top) {
            grand_parent = reinterpret_cast<InternalNode<StaticConfig> *>(top->node);
        }

        if (grand_parent) {
            M_ASSERT(old_parent, "BTree Insert fail grand parent.");
            // There is a grand parent. We need to swap out the pointer to the old
            // parent and install the pointer to the new parent.
            auto result = grand_parent->Update(top->node->GetRowMeta(top->meta_index),
                                               old_parent, *ptr_parent);
            if (!result.IsOk()) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    _mm_free(b_r);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    _mm_free(b_l);
                }
                return ReturnCode::CASFailure();
            }
        } else {
            // No grand parent or already popped out by during split propagation
            // uint64 pointer, Compare and Swap operation
            bool result =  ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), node_parent);
            if (!result) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    _mm_free(b_r);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    _mm_free(b_l);
                }
                return ReturnCode::CASFailure();
            }
        }

        assert(should_proceed);
        if (should_proceed){
            memset(reinterpret_cast<char *>(node), 0 , parameters.leaf_node_size);
            _mm_free(reinterpret_cast<char *>(node));
        }
        if (old_parent) {
            _mm_free(reinterpret_cast<char *>(old_parent));
        }
    }

#endif
}

template <class StaticConfig>
RC CBtreeIndex<StaticConfig>::Insert_Batch( LeafNode<StaticConfig> *node_l,
                                           uint32_t key_size, uint32_t payload_size,
                                           uint32_t split_threshold){
    RC rc = RCOK;
#if AGGRESSIVE_INLINING

    if (node_l->IsFrozen()){
        return Abort;
    }
    thread_local bool frozen_by_me = node_l->Freeze();
    if(!frozen_by_me) return Abort;

    auto leaf_buffer = node_l->update_messages;
    auto buffer_sz = leaf_buffer->size();
    if (buffer_sz <=0) {
        node_l->UnFreeze();
        return RCOK;
    }
    Conflicts<StaticConfig> conflicts;
    conflicts.Clear();

    auto statuss = node_l->header.GetStatus();
    uint32_t used_space = LeafNode<StaticConfig>::GetUsedSpace(statuss);
    uint32_t row_m_sz = sizeof(row_m);
    uint32_t one_row_sz = row_m_sz + key_size + payload_size;
    uint32_t new_size = used_space + one_row_sz*buffer_sz;
//    if (new_size >= split_threshold) {
    //1. index insert append
    thread_local Stack<StaticConfig> stack;
    stack.Clear();

    auto first_item = leaf_buffer->begin();
    auto first_key = first_item->first;
    auto first_payload = first_item->second;
    auto first_payload_sz = sizeof(AggressiveRowHead<StaticConfig>) + sizeof(RowVersion<StaticConfig>)
            + sizeof(RowCommon<StaticConfig>) + first_payload->inlined_rv->data_size;
    assert(payload_size == first_payload_sz);
    auto node = TraverseToLeaf(&stack,  first_key, key_size);
    if(node == nullptr){
        rc = Abort;
    }
    auto itr = first_item;
    uint32_t current_sz = used_space;
    uint32_t distance = split_threshold - used_space;
    //when node need not split
    while (distance > one_row_sz){
        if (itr == leaf_buffer->end()){
            break;
        }
        node->Insert_Append_LF( first_key, sizeof(uint64_t),
                               first_payload, first_payload_sz, split_threshold );
        current_sz = current_sz + used_space + one_row_sz;
        distance = split_threshold - current_sz;
        itr++;
    }
    //when node need split
    for (; itr != leaf_buffer->end(); ++itr) {
        auto itr_key = itr->first;
        auto itr_payload = itr->second;
        auto retc = Insert_Append( itr_key, sizeof(uint64_t),
                                  itr_payload, first_payload_sz, &conflicts);
        if (retc.IsOk()){
            for (int i = 0; i < conflicts.Size(); ++i) {
                auto item = conflicts.Get(i);
                auto key_c = item.key;
                auto mess_c = item.newest;
                auto txn_id_c = mess_c->inlined_rv->rts;
                index_insert_buffer(key_c, mess_c,  first_payload_sz);
                uint64_t tt = txn_id_c.get().t2;
                conflicts_txn.insert(tt);
            }
            rc = RCOK;
        }else{
            if (retc.IsKeyExists()){
                printf("insert key is exist :%lu. \n", itr_key);
                rc =  Abort;
            }
        }
    }
//    }else{
//        //2. node insert by sorting
//
//    }
    for (auto itr1 = leaf_buffer->begin(); itr1 != leaf_buffer->end(); ++itr1) {
        conflicts_k.erase(itr1->first);
        leaf_buffer->erase(itr1);
    }
    leaf_buffer->clear();

    node_l->UnFreeze();

#endif
    return rc;
}

template <class StaticConfig>
RC CBtreeIndex<StaticConfig>::index_insert(Transaction<StaticConfig>* txn,
                                           uint64_t key, void* &row_head,
                                           uint64_t payload_ptr, uint32_t payload_size){
    RC rc = RCOK;

    uint32_t key_size = sizeof(key);
    int retry_count=0;
    //transaction updates conflict with tree splits
    Conflicts<StaticConfig> conflicts;
    conflicts.Clear();
    AggressiveRowHead<StaticConfig> *head_;

    retry:
    auto retc = Insert(key, key_size, head_, payload_ptr, payload_size, &conflicts);

    if (retc.IsOk()){
        row_head = reinterpret_cast<void *>(head_);

#if AGGRESSIVE_INLINING
#if BUFFERING
        //emplace conflicts to root buffer
        for (int i = 0; i < conflicts.Size(); ++i) {
            auto item = conflicts.Get(i);
            auto key_c = item.key;
            auto mess_c = item.newest;
            auto txn_id_c = mess_c->inlined_rv->rts;
            index_insert_buffer( key_c, mess_c,  payload_size);
            uint64_t tt = txn_id_c.get().t2;
            conflicts_txn.insert(tt);
        }
#endif
#endif
        rc = RCOK;
    }else{
        if (retc.IsKeyExists()){
//            printf("insert key is exist :%lu. \n", key);
            return Abort;
        }
        if (retry_count < MAX_INSERT_RETRY){
            retry_count++;
            goto retry;
        }
    }
    return rc;
}

template <class StaticConfig>
RC CBtreeIndex<StaticConfig>::index_insert_buffer( uint64_t key, AggressiveRowHead<StaticConfig>* &row_head,
                                                   uint32_t payload_size  ){
    RC rc = RCOK;
#if AGGRESSIVE_INLINING
    //1.get root, insert root.
    auto root_ = GetRootNodeSafe();
    auto root_i = root_ ;
//    printf("buff insert key:%lu. \n",key);

    if (root_i->IsFrozen()){
//        printf("root frozen, buff insert key:%lu. \n",key);
        return Abort;
    }
    thread_local bool frozen_by_me = root_i->Freeze();
    assert(frozen_by_me);
//    if (row_head->inlined_rv->data_size == 0){
//        printf("row_head->inlined_rv->data_size :%lu. \n",row_head->inlined_rv->data_size);
//    }

    auto root_buff = root_i->update_messages;
    //insert key to root's buffer
    root_buff->insert(std::make_pair(key, row_head));
    //insert key to root's BF
    conflicts_k.insert((uint64_t)(key));

//    printf("buff size:%lu. \n",root_buff->size());
    //2.check this node, if the root fill up, pushdown
    if (root_buff->size() > MESSAGE_COUNT){
        //choose a child having the largest set of pending insertion message
        LeafNode<StaticConfig> *node_l = nullptr;
        auto ret = ChooseChildToPushdown( root_i, &node_l);
        if (ret && (node_l != nullptr)){
            assert(node_l->is_leaf);
            //3.1.if the leaf fill up, split
            //3.2.if split conflits with transaction, insert it to root
            rc = Insert_Batch( node_l,  sizeof(uint64_t), payload_size, parameters.leaf_node_size);
        }
    }else{
        if (frozen_by_me) root_i->UnFreeze();
    }

#endif

    return rc;
}

template <class StaticConfig>
RC CBtreeIndex<StaticConfig>::index_read(uint64_t key, void*& item, access_t type) {
    RC rc = RCOK;

    row_m *row_meta;
    uint32_t key_size = sizeof(key);
    ReturnCode retc;

    LeafNode<StaticConfig> *node = TraverseToLeaf(nullptr, key,  key_size);
    if (node == nullptr) {
        return RC::Abort;
    }

    retc = node->SearchRowMeta(key, key_size, &row_meta, 0, 0, false);

    if (retc.IsOk()){
#if AGGRESSIVE_INLINING
        if (type == WR){
            if (node->IsFrozen()){
                printf("leaf node frozen. ");
                return RC::Abort;
            }
        }
        char *source_addr = (reinterpret_cast<char *>(node) + row_meta->GetOffset());
        char *payload_ = source_addr + row_meta->GetPaddedKeyLength() ;
        item = reinterpret_cast<void *>(payload_);
#else
        char *source_addr = (reinterpret_cast<char *>(node) + row_meta->GetOffset());
        char *payload_ = source_addr + row_meta->GetPaddedKeyLength();
        uint64_t payload__ = *reinterpret_cast<uint64_t *>(payload_);
        item = reinterpret_cast<void *>(&payload__);
#endif
        rc = RCOK;
    } else{
        printf("search is not ok. \n");
        rc = Abort;
    }

    return rc;
}

template <class StaticConfig>
RC CBtreeIndex<StaticConfig>::index_read_buffer( uint64_t key, void *&item,
                                                 access_t type, int part_id) {
    RC rc = RCOK;

#if AGGRESSIVE_INLINING
    char *payload_;
    row_m *row_meta = nullptr;
    uint32_t key_size = sizeof(key);
    uint32_t data_sz = 0;
    RC ret = RCOK;
    AggressiveRowHead<StaticConfig> *messge_upt = nullptr;
    LeafNode<StaticConfig> *node_l = nullptr;
    InternalNode<StaticConfig> *node_i = nullptr;

    bool is_in_buffer = conflicts_k.contains(key);
    if (!is_in_buffer){
        node_l = TraverseToLeaf(nullptr, key,  key_size);
    }else{
        ret = TraverseToTarget( nullptr, key,  key_size , &messge_upt, &node_l, &node_i, true) ? RCOK:Abort;
        if (ret != RCOK) {
            return RC::Abort;
        }
    }

    AggressiveRowHead<StaticConfig> *leaf_row = nullptr;
    AggressiveRowHead<StaticConfig> *orgin = nullptr;
//    row_t *update = nullptr;
    //find in the leaf buffer or inner buffer
    if (messge_upt != nullptr) {
//        payload_sz = messge_upt->payload_sz;
        data_sz = messge_upt->inlined_rv->data_size;
        orgin = messge_upt;
        payload_ = reinterpret_cast<char *>(orgin);
        if (orgin == nullptr){
//          printf("orgin read error. ");
            return RC::Abort;
        }
    } else{
        if(node_l == nullptr){
//            printf("leaf node error. ");
            return RC::Abort;
        }
        //read the node, if it is leaf
        //find in the leaf node
        auto ret1 = node_l->Read(key, key_size, &row_meta);
        if (ret1 == RCOK){
            char *source_addr = (reinterpret_cast<char *>(node_l) + row_meta->GetOffset());
            payload_ = source_addr + row_meta->GetPaddedKeyLength() ;
            leaf_row = reinterpret_cast<AggressiveRowHead<StaticConfig> *>(payload_);
            data_sz = leaf_row->inlined_rv->data_size;
            if (leaf_row == nullptr){
                printf("leaf_row read error. ");
                return RC::Abort;
            }
            if (type == WR){
                if (node_l->IsFrozen()){
                    printf("leaf node frozen. ");
                    return RC::Abort;
                }
                auto rett = ATOM_CAS(leaf_row->inlined_rv->is_updated, false, true);
                if(!rett){
                    printf("set control fail. ");
                    return RC::Abort;
                }
            }
        }else{
            printf("leaf node read error:%lu. \n ", key);
        }
    }

    if (data_sz >0){
        item = reinterpret_cast<void *>(payload_);
    } else{
        rc = RC::Abort;
        printf("abort, type:%u , node_i: %p, node_l: %p. \n", type, node_i, node_l);
    }

#endif
    return rc;
}

template <class StaticConfig>
bool CBtreeIndex<StaticConfig>::index_read_buffer_again(Transaction<StaticConfig>* txn, uint64_t key,
                                                        void *&item, access_t type, int part_id){
#if AGGRESSIVE_INLINING
    auto txn_id = txn->ts();
//    bool is_in_buffer = conflicts_txn.query(reinterpret_cast<char *>(&txn_id));
    uint64_t tt = txn_id.get().t2;
    bool is_in_buffer = conflicts_txn.contains(tt);
    if (!is_in_buffer){
        return is_in_buffer; //false
    }else{
        index_read_buffer( key, item,  type, part_id);
    }

    return !is_in_buffer; //true
#endif
}

template <class StaticConfig>
InternalNode<StaticConfig>::InternalNode(uint32_t node_size, char * key,
                                         const uint32_t key_size, uint64_t left_child_addr,
                                         uint64_t right_child_addr)
        : BaseNode<StaticConfig>(false, node_size) {
    // Initialize a new internal node with one key only
    this->header.sorted_count = 2;  // Includes the null dummy key
    this->header.size = node_size;

    // Fill in left child address, with an empty key, key len =0
    uint64_t offset = node_size - sizeof(left_child_addr);
    //invalid commit id = 0
    uint64_t total_size = 0 + sizeof(uint64_t);
    this->row_meta[0].FinalizeForInsert(offset, 0, total_size);
    char *ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, &left_child_addr, sizeof(left_child_addr));

    // Fill in right child address, with the separator key
    auto padded_key_size = row_m::PadKeyLength(key_size);
    auto total_len = padded_key_size + sizeof(right_child_addr);
    offset -= total_len;
    total_size = padded_key_size + sizeof(uint64_t);
    this->row_meta[1].FinalizeForInsert(offset, key_size, total_size);
    ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, key, key_size);
    memcpy(ptr + padded_key_size, &right_child_addr, sizeof(right_child_addr));

    assert((uint64_t) ptr == (uint64_t) this + sizeof(*this) + 2 * sizeof(row_m));

    ::mica::util::memory_barrier();
}


template <class StaticConfig>
InternalNode<StaticConfig>::InternalNode(
        uint32_t node_size, InternalNode *src_node,
        uint32_t begin_meta_idx, uint32_t nr_records,
        char * key, const uint32_t key_size,
        uint64_t left_child_addr, uint64_t right_child_addr,
        uint64_t left_most_child_addr, uint32_t value_size)
        : BaseNode<StaticConfig>(false, node_size) {
    M_ASSERT(src_node,"InternalNode src_node is null.");
    __builtin_prefetch((const void *) (src_node), 0, 1);
    auto padded_key_size = row_m::PadKeyLength(key_size);
    uint64_t total_size = 0;

    uint64_t offset = node_size;
    bool need_insert_new = key;
    uint32_t insert_idx = 0;

    // See if we need a new left_most_child_addr, i.e., this must be the new node
    // on the right
    if (left_most_child_addr) {
        offset -= sizeof(uint64_t);
        total_size = 0 + sizeof(uint64_t);
        this->row_meta[0].FinalizeForInsert(offset, 0, total_size);
//        LOG_DEBUG("left_most_child_addr = %lu",left_most_child_addr);
        memcpy(reinterpret_cast<char *>(this) + offset, &left_most_child_addr,
               sizeof(uint64_t));
        ++insert_idx;
    }

    assert(nr_records > 0);

    for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
        row_m meta = src_node->row_meta[i];
        assert(meta.IsVisible());
        if (!meta.IsVisible()) continue;
        uint64_t m_payload = 0;
        char *m_key = nullptr;
        char *m_data = nullptr;
        src_node->GetRawRow(meta, &m_data, &m_key, &m_payload);
        auto m_key_size = meta.GetKeyLength();

        if (!need_insert_new) {
            // New key already inserted, so directly insert the key from src node
            assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
            offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
            total_size = m_key_size + sizeof(uint64_t);
            this->row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, total_size);
            memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                   (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
        } else {
            // Compare the two keys to see which one to insert (first)
            auto cmp = this->KeyCompare(m_key, m_key_size, key, key_size);
//            if (m_key != nullptr && key != nullptr){
//                uint64_t mk = *reinterpret_cast<uint64_t *>(m_key);
//                uint64_t kk = *reinterpret_cast<uint64_t *>(key);
//                printf("mk:%lu,kk:%lu. \n", mk, kk);
//            }

            assert(!(cmp == 0 && key_size == m_key_size));

            if (cmp > 0) {
                assert(insert_idx >= 1);
                // Modify the previous key's payload to left_child_addr
                auto prev_meta = this->row_meta[insert_idx-1];

                memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
                       prev_meta.GetPaddedKeyLength(), &left_child_addr, sizeof(left_child_addr));

                // Now the new separtor key itself
                offset -= (padded_key_size + sizeof(right_child_addr));
                total_size = key_size + sizeof(uint64_t);
                this->row_meta[insert_idx].FinalizeForInsert(offset, key_size, total_size);

                ++insert_idx;
                memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
                memcpy(reinterpret_cast<char *>(this) + offset + padded_key_size,
                       &right_child_addr, sizeof(right_child_addr));

                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                total_size = m_key_size + sizeof(uint64_t);
                this->row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, total_size);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                       (meta.GetPaddedKeyLength() + sizeof(uint64_t)));

                need_insert_new = false;
            } else {
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                total_size = m_key_size + sizeof(uint64_t);
                this->row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, total_size);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                       (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
            }
        }
        ++insert_idx;
    }

    if (need_insert_new) {
        // The new key-payload pair will be the right-most (largest key) element
        offset -= total_size;
        total_size = key_size + sizeof(uint64_t);
        this->row_meta[insert_idx].FinalizeForInsert(offset, key_size, total_size);
        memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
        memcpy(reinterpret_cast<char *>(this) + offset + row_m::PadKeyLength(key_size),
               &right_child_addr, sizeof(right_child_addr));

        // Modify the previous key's payload to left_child_addr
        auto prev_meta = this->row_meta[insert_idx - 1];
        memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
               prev_meta.GetPaddedKeyLength(), &left_child_addr, sizeof(left_child_addr));

        ++insert_idx;
    }


    ::mica::util::memory_barrier();
    this->header.size = node_size;
    this->header.sorted_count = insert_idx;

}


template <class StaticConfig>
bool InternalNode<StaticConfig>::PrepareForSplit(
        Stack<StaticConfig> &stack, uint32_t split_threshold, char * key, uint32_t key_size,
        uint64_t left_child_addr,    // [key]'s left child pointer
        uint64_t right_child_addr,   // [key]'s right child pointer
        InternalNode **new_node, bool backoff ) {
    uint32_t data_size = this->header.size ;
    data_size = data_size + key_size + sizeof(right_child_addr);
    data_size = data_size + sizeof(row_m);

    uint32_t new_node_size = sizeof(InternalNode) + data_size;
    if (new_node_size < split_threshold) {
        InternalNode::New(this, key, key_size, left_child_addr, right_child_addr, new_node );
#if AGGRESSIVE_INLINING
#if BUFFERING
        if (this->update_messages->size()>0){
            auto upt_ben = this->update_messages->begin();
            auto upt_end = this->update_messages->end();
            (*new_node)->update_messages->insert(upt_ben, upt_end);
         }
#endif
#endif
        assert(this->IsFrozen());
        this->UnFreeze();
        return true;
    }

    // After adding a key and pointers the new node would be too large. This
    // means we are effectively 'moving up' the tree to do split
    // So now we split the node and generate two new internal nodes
    M_ASSERT(this->header.sorted_count >= 2, "header.sorted_count >= 2.");
    uint32_t n_left = this->header.sorted_count >> 1;

    char *l_pt ;
    char *r_pt ;
    InternalNode **ptr_l = reinterpret_cast<InternalNode **>(&l_pt);
    InternalNode **ptr_r = reinterpret_cast<InternalNode **>(&r_pt);

    // Figure out where the new key will go
    auto separator_meta = this->row_meta[n_left];
    char *separator_key = nullptr;
    uint32_t separator_key_size = separator_meta.GetKeyLength();
    uint64_t separator_payload = 0;
    bool success = this->GetRawRow(separator_meta, nullptr, &separator_key, &separator_payload);
    M_ASSERT(success, "InternalNode::PrepareForSplit GetRawRecord fail.");

    int cmp = this->KeyCompare(key, key_size, separator_key, separator_key_size);

//    printf("separator_key, key, %lu, %lu, \n", *reinterpret_cast<uint64_t *>(separator_key),
//                *reinterpret_cast<uint64_t *>(key));

    if (cmp == 0) {
        cmp = key_size - separator_key_size;
    }
    M_ASSERT(cmp != 0,"InternalNode::PrepareForSplit KeyCompare fail.");
#if BUFFERING
    auto upt_messgs = this->update_messages;
    auto upt_sz = upt_messgs->size();
    typename std::map<uint64_t, AggressiveRowHead<StaticConfig> *>::iterator upt_ben;
    typename std::map<uint64_t,  AggressiveRowHead<StaticConfig> *>::iterator upt_end1;
    typename std::map<uint64_t, AggressiveRowHead<StaticConfig> *>::iterator upt_end2;
    if (upt_sz >0){
        upt_ben = upt_messgs->begin();
        upt_end1 = upt_messgs->find(n_left);
        upt_end2 = upt_messgs->end();
    }
#endif
    if (cmp < 0) {
        // Should go to left
        InternalNode::New(this, 0, n_left, key, key_size, left_child_addr, right_child_addr, ptr_l, 0 );
        InternalNode::New( this, n_left + 1, (this->header.sorted_count - n_left - 1), 0, 0,
                           0, 0, ptr_r, separator_payload );
#if BUFFERING
        if (upt_sz >0) {
        (*ptr_l)->update_messages->insert(upt_ben, upt_end1);
        (*ptr_r)->update_messages->insert(upt_end1, upt_end2);
    }
#endif
    } else {
        InternalNode::New( this, 0, n_left, 0, 0, 0, 0, ptr_l, 0 );
        InternalNode::New(this, n_left + 1, (this->header.sorted_count - n_left - 1), key, key_size,
                          left_child_addr, right_child_addr, ptr_r, separator_payload );
#if BUFFERING
        if (upt_sz >0) {
            (*ptr_l)->update_messages->insert(upt_ben, upt_end1);
            (*ptr_r)->update_messages->insert(upt_end1, upt_end2);
         }
#endif
    }
    assert(*ptr_l);
    assert(*ptr_r);

    uint64_t node_l = reinterpret_cast<uint64_t>(*ptr_l);
    uint64_t node_r = reinterpret_cast<uint64_t>(*ptr_r);

    // Pop here as if this were a leaf node so that when we get back to the
    // original caller, we get stack top as the "parent"
    stack.Pop();

    // Now get this internal node's real parent
    auto stack_node = stack.Top() ? stack.Top()->node : nullptr;
    InternalNode *parent = reinterpret_cast<InternalNode *>(stack_node);
    if (parent == nullptr) {
        // Good!
        InternalNode::New( separator_key , separator_key_size,
                           (uint64_t) node_l, (uint64_t) node_r, new_node );
#if BUFFERING
        if (this->update_messages->size()>0){
        auto upt_ben = this->update_messages->begin();
        auto upt_end = this->update_messages->end();
        (*new_node)->update_messages->insert(upt_ben, upt_end);
    }
#endif
        return true;
    }

    __builtin_prefetch((const void *) (parent), 0, 1);

    // Try to freeze the parent node first
    thread_local bool frozen_by_me = false;
    while (parent->IsFrozen())
        ::mica::util::pause();

    frozen_by_me = parent->Freeze();
    // Someone else froze the parent node and we are told not to compete with
    // others (for now)
    if (!frozen_by_me && backoff) {
        return false;
    }

    auto parent_split_ret = parent->PrepareForSplit(stack, split_threshold,
                                                    separator_key , separator_key_size,
                                                    (uint64_t) node_l, (uint64_t) node_r,
                                                    new_node, backoff );
    return parent_split_ret;
}


template <class StaticConfig>
uint32_t InternalNode<StaticConfig>::GetChildIndex(const char *key, uint32_t key_size, bool get_le) {
    // Keys in internal nodes are always sorted, visible
    int32_t left = 0, right = this->header.sorted_count - 1, mid = 0;
//    char *key_ = reinterpret_cast<char *>(&key);

    //binary search
    while (true) {
        mid = (left + right) / 2;
        auto meta = this->row_meta[mid];
        char *record_key = nullptr;
        this->GetRawRow(meta, nullptr, &record_key, nullptr);

        auto cmp = this->KeyCompare(key, key_size, record_key, meta.GetKeyLength());
        if (cmp == 0) {
            // Key exists
            if (get_le) {
                return static_cast<uint32_t>(mid - 1);
            } else {
                return static_cast<uint32_t>(mid);
            }
        }
        if (left > right) {
            if (cmp <= 0 && get_le) {
                return static_cast<uint32_t>(mid - 1);
            } else {
                return static_cast<uint32_t>(mid);
            }
        } else {
            if (cmp > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
    }
}


template <class StaticConfig>
ReturnCode InternalNode<StaticConfig>::Update(row_m meta, InternalNode *old_child, InternalNode *new_child ){
    auto status = this->header.GetStatus();
//    if (status.IsFrozen()) {
//        return ReturnCode::NodeFrozen();
//    }

    bool ret_header = ATOM_CAS(this->header.status.word,
                               status.word, status.word);

    uint64_t old_ = reinterpret_cast<uint64_t> (old_child);
    uint64_t new_ = reinterpret_cast<uint64_t> (new_child);
    auto payload_ptr_ = GetPayloadPtr(meta);
    bool ret_addr =  ATOM_CAS(*payload_ptr_, old_, new_);

    if (ret_header && ret_addr) {
        return ReturnCode::Ok();
    } else {
        return ReturnCode::CASFailure();
    }
}

template <class StaticConfig>
void LeafNode<StaticConfig>::New(LeafNode **mem, uint32_t node_size ) {
    *mem = reinterpret_cast<LeafNode *>(_mm_malloc(node_size, CL_SIZE) );

    memset(*mem, 0, node_size);
    new(*mem) LeafNode(node_size);
    ::mica::util::memory_barrier();

    (*mem)->update_messages = new std::map<uint64_t, AggressiveRowHead<StaticConfig> *>();
}


template <class StaticConfig>
ReturnCode LeafNode<StaticConfig>::SearchRowMeta(
        uint64_t key, uint32_t key_size, row_m **out_metadata_ptr,
        uint32_t start_pos, uint32_t end_pos, bool check_concurrency) {
    ReturnCode rc = ReturnCode::NotFound();
    char *key_ = reinterpret_cast<char *>(&key);

    for (uint32_t i = 0; i < this->header.sorted_count; i++) {
        row_m current = this->row_meta[i];
        char *current_key = this->GetKey(current);
        assert(current_key);
        auto cmp_result = this->KeyCompare(key_, key_size, current_key, current.GetKeyLength());
        if (cmp_result == 0) {
            if (!current.IsVisible()) {
                break;
            }
            if (out_metadata_ptr) {
                *out_metadata_ptr = this->row_meta + i;
                rc = ReturnCode::Ok();
            }
            return rc;
        }
    }

    for (uint32_t i = this->header.sorted_count; i < this->header.GetStatus().GetRecordCount(); i++) {
        row_m current_row = this->row_meta[i];
        //delete/select
//        if (current_row.IsInserting()) {
//            if (check_concurrency) {
//                // Encountered an in-progress insert, recheck later
//                *out_metadata_ptr = &row_meta[i];
//                rc = ReturnCode::Ok();
//                return rc;
//            } else {
//                continue;
//            }
//        }
        if (current_row.IsVisible() ) {
            auto current_size = current_row.GetKeyLength();
            auto current_key = this->GetKey(current_row);
            if (current_size == key_size &&
                this->KeyCompare(key_, key_size, current_key, current_size) == 0) {
                *out_metadata_ptr = &this->row_meta[i];
                rc = ReturnCode::Ok();
                return rc;
            }
        }
    }

    return rc;
}


template <class StaticConfig>
ReturnCode LeafNode<StaticConfig>::Insert_LF( uint64_t key, uint32_t key_size,
                                             AggressiveRowHead<StaticConfig>* &row_head,
                                             uint64_t payload_ptr, uint32_t payload_size,
                                             uint32_t split_threshold) {
    //1.frozee the location/offset
    //2.copy record to the location
    ReturnCode rc=ReturnCode::Ok();

    NodeHeader::StatusWord expected_status = this->header.GetStatus();
    assert(expected_status.IsFrozen());

//    printf("insert insert key :%lu. \n", key);
    Uniqueness uniqueness = CheckUnique(key, key_size);
    if (uniqueness == Duplicate) {
        printf("insert insert key is exist :%lu. \n", key);
        return ReturnCode::KeyExists();
    }

    // Check space to see if we need to split the node
    uint32_t used_space = LeafNode::GetUsedSpace(expected_status);
    uint32_t row_m_sz = sizeof(row_m);
    uint32_t new_size;
    new_size = used_space + row_m_sz +key_size + payload_size;

//    LOG_DEBUG("LeafNode::GetUsedSpace: %u.",  new_size);
    if (new_size >= split_threshold) {
        return ReturnCode::NotEnoughSpace();
    }

    NodeHeader::StatusWord desired_status = expected_status;
    uint32_t total_size;
    total_size = key_size + payload_size;
    desired_status.PrepareForInsert(total_size);
    auto ret = ATOM_CAS(this->header.status.word,expected_status.word, desired_status.word);
    assert(ret);
    this->header.sorted_count++;

    auto insert_index = GetFirstGreater(reinterpret_cast<char *>(&key));
    row_m *row_meta_ptr = &this->row_meta[insert_index];
    row_m expected_meta = *row_meta_ptr;
    assert(row_meta_ptr);
    assert(expected_meta.IsVacant());

    row_m desired_meta = expected_meta;
    desired_meta.PrepareForInsert();
    ret = ATOM_CAS(row_meta_ptr->meta,expected_meta.meta, desired_meta.meta);
    assert(ret);

    uint64_t offset = this->header.size - desired_status.GetBlockSize();
    char *data_ptr = &(reinterpret_cast<char *>(this))[offset];
    std::memcpy(data_ptr, &key, key_size);

#if AGGRESSIVE_INLINING
    if (row_head == nullptr){
        std::memset(data_ptr + key_size, 0, payload_size);
    } else {
        std::memcpy(data_ptr + key_size, row_head, payload_size);
    }
#else
    uint64_t payload_ = payload_ptr;
    std::memcpy(data_ptr + key_size, &payload_, sizeof(uint64_t));
#endif

    auto new_meta = desired_meta;
    new_meta.FinalizeForInsert(offset, key_size, total_size);
    ret = ATOM_CAS(row_meta_ptr->meta,desired_meta.meta, new_meta.meta);
    assert(ret);


    ::mica::util::memory_barrier();
    auto payld = reinterpret_cast<void *>(data_ptr + key_size);
    row_head = reinterpret_cast<AggressiveRowHead<StaticConfig> *>(payld);
//    printf("insert k1:%lu \n.", *reinterpret_cast<uint64_t *>(data_ptr));
    return rc;
}

template <class StaticConfig>
ReturnCode LeafNode<StaticConfig>::Insert_Append_LF(uint64_t key, uint32_t key_size,
                                                  AggressiveRowHead<StaticConfig>* &row_head,
                                                  uint32_t payload_size, uint32_t split_threshold) {
    //1.frozee the location/offset
    //2.copy record to the location
    ReturnCode rc=ReturnCode::Ok();

#if AGGRESSIVE_INLINING
    NodeHeader::StatusWord expected_status = this->header.GetStatus();

    Uniqueness uniqueness = CheckUnique(key, key_size);
    if (uniqueness == Duplicate) {
        return ReturnCode::KeyExists();
    }

    // Check space to see if we need to split the node
    uint32_t used_space = LeafNode::GetUsedSpace(expected_status);
    uint32_t row_m_sz = sizeof(row_m);
    uint32_t new_size = used_space + row_m_sz +key_size + payload_size;

//    LOG_DEBUG("LeafNode::GetUsedSpace: %u.",  new_size);
    if (new_size >= split_threshold) {
        return ReturnCode::NotEnoughSpace();
    }

    NodeHeader::StatusWord desired_status = expected_status;
    uint32_t total_size;
    total_size = key_size + payload_size;
    desired_status.PrepareForInsert(total_size);
    auto ret = ATOM_CAS(this->header.status.word,expected_status.word, desired_status.word);
    assert(ret);

    auto expected_status_record_count = expected_status.GetRecordCount();
    row_m *row_meta_ptr = &this->row_meta[expected_status_record_count];
    auto expected_meta = *row_meta_ptr;

    row_m desired_meta = expected_meta;
    desired_meta.PrepareForInsert();
    ret = ATOM_CAS(row_meta_ptr->meta,expected_meta.meta, desired_meta.meta);
    assert(ret);

    uint64_t offset = this->header.size - desired_status.GetBlockSize();
    char *data_ptr = &(reinterpret_cast<char *>(this))[offset];
    std::memcpy(data_ptr, &key, key_size);
    if (row_head == nullptr){
        std::memset(data_ptr + key_size, 0, payload_size);
    } else {
        std::memcpy(data_ptr + key_size, row_head, payload_size);
    }

    auto new_meta = desired_meta;
    new_meta.FinalizeForInsert(offset, key_size, total_size);
    ret = ATOM_CAS(row_meta_ptr->meta,desired_meta.meta, new_meta.meta);
    assert(ret);

    ::mica::util::memory_barrier();
    auto payld = reinterpret_cast<void *>(data_ptr + key_size);
    row_head = reinterpret_cast<AggressiveRowHead<StaticConfig> *>(payld);
//    printf("insert k1:%lu \n.", *reinterpret_cast<uint64_t *>(data_ptr));

#endif
    return rc;
}


template <class StaticConfig>
uint32_t LeafNode<StaticConfig>::GetFirstGreater(char *key){
    assert(this->header.status.IsFrozen());
    uint32_t total_len = 0;
    uint32_t count = this->header.GetStatus().GetRecordCount();
    uint32_t indx = 0;

    if (count == 0){
        return 0;
    }

    for (uint32_t i = 0; i < count; ++i) {
        auto meta = this->row_meta[i];
        char *record_key = nullptr;
        this->GetRawRow(meta, nullptr, &record_key, nullptr);
        uint32_t len = meta.GetKeyLength();
        total_len = meta.GetTotalLength();

        auto cmp = this->KeyCompare(key, len, record_key, len);
        if (cmp < 0){
            indx = i;
        }
    }

    auto moving_sz = count-indx;
    for (uint32_t i = 0; i < moving_sz; ++i) {
        //from end to indx
        auto mvfrom_offset = count-1-i;
        auto mvto_offset = count-1-i+1;

        auto pre_meta = this->row_meta[mvfrom_offset];
        auto dest_meta = &this->row_meta[mvto_offset];
        char *pre_ptr = &(reinterpret_cast<char *>(this))[mvfrom_offset];
        char *dest_ptr = &(reinterpret_cast<char *>(this))[mvto_offset];


        auto expected_row_meta = *dest_meta;
        auto ret = ATOM_CAS(dest_meta->meta,expected_row_meta.meta, pre_meta.meta);
        assert(ret);
        memcpy(dest_ptr, pre_ptr, total_len);
    }

    auto dest_meta = &this->row_meta[indx];
    auto expected_row_meta = *dest_meta;
    auto ret = ATOM_CAS(dest_meta->meta,expected_row_meta.meta, 0);
    assert(ret);

    return indx;
}

template <class StaticConfig>
bool LeafNode<StaticConfig>::Read(uint64_t key, uint32_t key_size, row_m **row_meta) {
    ReturnCode rc;

    rc = SearchRowMeta( key, key_size, row_meta, 0, (uint32_t)-1, false);

    if (rc.IsNotFound()) {
        return RC::ERROR;
    }
//    auto roww = reinterpret_cast<row_t *>(*payload);
//    char *data = roww->get_data();
//    uint32_t total_size = row_meta->GetTotalLength();

    return RC::RCOK;
}

template <class StaticConfig>
Uniqueness LeafNode<StaticConfig>::CheckUnique(uint64_t key, uint32_t key_size) {
//    char *key_ = reinterpret_cast<char *>(&key);
    row_m *row_meta = nullptr;
    ReturnCode rc = SearchRowMeta(key, key_size, &row_meta);
    if (rc.IsNotFound() || !row_meta->IsVisible()) {
        return IsUnique;
    }

    if(row_meta->IsInserting()) {
        return ReCheck;
    }

    M_ASSERT(row_meta->IsVisible(), "LeafNode::CheckUnique metadata Is not Visible.");

    char *curr_key;
    curr_key = this->GetKey(*row_meta);

    if (this->KeyCompare(reinterpret_cast<char *>(&key), key_size, curr_key, key_size) == 0) {
        return Duplicate;
    }

    return ReCheck;
}


template <class StaticConfig>
uint32_t LeafNode<StaticConfig>::SortMetaByKey(std::vector<row_m> &vec, bool visible_only) {
    // Node is frozen at this point
    assert(this->header.status.IsFrozen());
    uint32_t total_size = 0;
    uint32_t count = this->header.GetStatus().GetRecordCount();
    for (uint32_t i = 0; i < count; ++i) {
        auto meta = this->row_meta[i];
//        char *kk = GetKey(meta);
//        printf("k1:%lu \n.", *reinterpret_cast<uint64_t *>(kk));
        if (meta.IsVisible() && visible_only) {
            vec.emplace_back(meta);
            total_size += (meta.GetTotalLength());
            assert(meta.GetTotalLength());
        }
    }

    // Lambda for comparing two keys
    auto key_cmp = [this](row_m &m1, row_m &m2) -> bool {
        auto l1 = m1.GetKeyLength();
        auto l2 = m2.GetKeyLength();
        char *k1 = this->GetKey(m1);
        char *k2 = this->GetKey(m2);
        return this->KeyCompare(k1, l1, k2, l2) < 0;
    };

    std::sort(vec.begin(), vec.end(), key_cmp);
    return total_size;
}


template <class StaticConfig>
void LeafNode<StaticConfig>::CopyFrom(LeafNode *node, std::vector<row_m>::iterator begin_it,
                                      std::vector<row_m>::iterator end_it,
                                      Conflicts<StaticConfig> *conflicts) {
    assert(node->IsFrozen());
    uint32_t offset = this->header.size;
    uint32_t nrecords = 0;
    for (auto it = begin_it; it != end_it; ++it) {
        auto row_meta_ = *it;
        char *key_ptr;
        key_ptr = node->GetKey(row_meta_);
        uint32_t key_len = row_meta_.GetKeyLength();
        uint32_t total_len = row_meta_.GetTotalLength();

#if AGGRESSIVE_INLINING
#if BUFFERING
        char *payload = key_ptr + key_len;
        uint32_t payload_size = total_len - key_len;
        auto orgroww = reinterpret_cast<AggressiveRowHead<StaticConfig> *>(payload);
        if (orgroww->inlined_rv->is_updated){
            auto roww = (AggressiveRowHead<StaticConfig> *)_mm_malloc(payload_size, CL_SIZE) ;
            memcpy(roww, orgroww, payload_size);
            uint64_t ky = *reinterpret_cast<uint64_t *>(key_ptr);
            conflicts->Append( ky, roww);

            continue;
        }
#endif
#endif

        assert(offset >= total_len);
        offset -= total_len;
        char *dest_ptr = &(reinterpret_cast<char *>(this))[offset];

        memcpy(dest_ptr, key_ptr, total_len);

        row_m *row_m_ptr = &this->row_meta[nrecords];
        auto expected_row_meta = *row_m_ptr;
        auto new_row_meta = expected_row_meta;
        new_row_meta.FinalizeForInsert(offset, key_len, total_len);
        auto ret = ATOM_CAS(row_m_ptr->meta,expected_row_meta.meta, new_row_meta.meta);
        assert(ret);

        ++nrecords;
    }

    // Finalize header stats
    this->header.status.SetBlockSize(this->header.size - offset);
    this->header.status.SetRecordCount(nrecords);
    this->header.sorted_count = nrecords;
}


template <class StaticConfig>
bool LeafNode<StaticConfig>::PrepareForSplit( Stack<StaticConfig> &stack,
                                              uint32_t split_threshold,
                                              uint32_t key_size, uint32_t payload_size,
                                              LeafNode **left, LeafNode **right,
                                              InternalNode<StaticConfig> **new_parent, bool backoff,
                                              Conflicts<StaticConfig> *conflicts) {
    assert(key_size<=8);
    if (!this->header.status.IsFrozen()){
        return false;
    }
    if(this->header.GetStatus().GetRecordCount() < 3){
        return false;
    }

    thread_local std::vector<row_m> meta_vec;
    uint32_t total_size = 0;
    uint32_t nleft = 0;
    // Prepare new nodes: a parent node, a left leaf and a right leaf
    LeafNode::New(left, this->header.size );
    LeafNode::New(right, this->header.size );
    uint32_t totalSize = key_size + payload_size;
    uint32_t count = this->header.GetStatus().GetRecordCount();
    if(count <3) return false;

    meta_vec.clear();
    total_size = SortMetaByKey(meta_vec, true );

    int32_t left_size = total_size / 2;
    for (uint32_t i = 0; i < meta_vec.size(); ++i)
    {
        ++nleft;
        left_size -= totalSize;
        if (left_size <= 0)
        {
            break;
        }
    }

    if(nleft <= 0) return false;

    auto left_end_it = meta_vec.begin() + nleft;
    auto node_left = *left;
    auto node_right = *right;

    if (!this->IsFrozen()) return false;
    (*left)->CopyFrom(this, meta_vec.begin(), left_end_it , conflicts);
    (*right)->CopyFrom(this, left_end_it, meta_vec.end(), conflicts );

    row_m separator_meta = meta_vec.at(nleft - 1);
    uint32_t separator_key_size = separator_meta.GetKeyLength();
    char *separator_key = this->GetKey(separator_meta);

//    printf("separator_key:%lu \n.", *reinterpret_cast<uint64_t *>(separator_key));

    if(separator_key == nullptr)
    {
        return false;
    }

    ::mica::util::memory_barrier();

    auto stack_node = stack.Top() ? stack.Top()->node : nullptr;
    auto parent = reinterpret_cast<InternalNode<StaticConfig> *>(stack_node);

    if (parent == nullptr) {
        InternalNode<StaticConfig>::New(
                separator_key , separator_key_size,
                reinterpret_cast<uint64_t>(node_left),
                reinterpret_cast<uint64_t>(node_right),
                new_parent );
        return true;
    }

    thread_local bool frozen_by_me = false;
    while (parent->IsFrozen()){
        ::mica::util::pause();
    }

    frozen_by_me = parent->Freeze();
    if (!frozen_by_me && backoff)
    {
        return false;
    } else {
        auto ret = parent->PrepareForSplit(
                stack, split_threshold,  separator_key, separator_key_size,
                reinterpret_cast<uint64_t>(node_left), reinterpret_cast<uint64_t>(node_right),
                new_parent, backoff );

        parent->UnFreeze();

        return ret;
    }
}



}
}

#endif
#endif