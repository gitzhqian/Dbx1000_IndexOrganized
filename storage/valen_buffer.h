//
// Created by zhangqian on 2022/1/24.
//

#include "object_pool.h"
#include "config.h"
//#include "tbb/concurrent_unordered_map.h"
#include <list>

/**
* Not thread-safe.
*/
class ValenBuffer{
public:
    ValenBuffer(){
//        void *ptr = malloc(BUFFER_SEGMENT_SIZE);
//        if (!ptr) {
//            throw std::bad_alloc();
//        }
        buffer_data_ = new char[BUFFER_SEGMENT_SIZE];
    }

    ~ValenBuffer(){
        delete buffer_data_;
    }

    /**
     * @param size the amount of bytes to check for
     * @return Whether this segment have enough space left for size many bytes
     */
    bool HasBytesLeft(const uint32_t size) {
        auto cur_size = size_  + size;
        bool has_ = cur_size <= BUFFER_SEGMENT_SIZE;

        return  has_;
    }

    /**
     * Reserve space for a delta record of given size to be written in this segment. The segment must have
     * enough space for that record.
     *
     * @param size the amount of bytes to reserve
     * @return pointer to the head of the allocated record
     */
    char *Reserve(const uint32_t size) {
        SpinLatch::ScopedSpinLatch guard(&latch);

        assert(HasBytesLeft(size));
        char *result = buffer_data_ + size_;
        size_ += size;
        count_ = count_ + 1 ;

        return result;
    }

    /**
     * Clears the buffer segment.
     *
     * @return self pointer for chaining
     */
    ValenBuffer *Reset() {
        SpinLatch::ScopedSpinLatch guard(&latch);
        size_ = 0;
        count_ = 0;
        memset(buffer_data_,0,BUFFER_SEGMENT_SIZE);
        return this;
    }

    /**
     * decrease the count of the used segment in the valen buffer
     */
    void Erase(){
        SpinLatch::ScopedSpinLatch guard(&latch);
        count_ = count_ -1;
    }

    /**
     * current count of the used segments in the valen buffer
     * @return
     */
    uint32_t CurrentCount(){
        return count_ ;
    }

private:
    char *buffer_data_ = nullptr;
    SpinLatch latch;
    uint32_t size_ = 0;
    uint32_t count_ = 0;
};

/**
* Custom allocator used for the object pool of buffer segments
*/
class ValenBufferAllocator {
public:
    /**
     * Allocates a new BufferSegment
     * @return a new buffer segment
     */
    ValenBuffer *New() {
        auto *result = new ValenBuffer;
//        M_ASSERT(reinterpret_cast<uintptr_t>(result) % 8 == 0 , "record buffer size % 8. ");
        return result;
    }

    /**
     * Resets the given buffer segment for use
     * @param reused the buffer to reuse
     */
    void Reuse(ValenBuffer *const reused) { reused->Reset(); }

    /**
     * Delete the given buffer segment and frees the memory
     * @param ptr the buffer to delete
     */
    void Delete(ValenBuffer *const ptr) { delete ptr; }
};

/**
* Type alias for an object pool handing out buffer segments
*/
using ValenBufferPool = ObjectPool<ValenBuffer, ValenBufferAllocator>;


/**
 * allocate segments to holds the inner node records
 * because the btree's inner nodes are variable size
 */
class InnerNodeBuffer {
public:

    /**
     * Constructs a new undo buffer, drawing its segments from the given buffer pool.
     * @param buffer_pool buffer pool to draw segments from
     */
    explicit InnerNodeBuffer(ValenBufferPool *buffer_pool) : buffer_pool_(buffer_pool) {}

    /**
     * Destructs this buffer, releases all its segments back to the buffer pool it draws from.
     */
    ~InnerNodeBuffer() {
        for (auto &segment : buffers_){
            buffer_pool_->Release(segment.second, 1);
        }
    }

    /**
     * @return true if UndoBuffer contains no UndoRecords, false otherwise
     */
    bool Empty() const { return buffers_.empty(); }

    void Erase(uint32_t index_) {
        ValenBuffer *val_buffer_;

        for (auto begin = buffers_.begin() ; begin != buffers_.end(); ++begin)
        {
            auto buffer_elem = *begin;
            auto i = buffer_elem.first;
            if (i==index_){
                val_buffer_ = buffer_elem.second;

                val_buffer_->Erase();
                if (val_buffer_->CurrentCount() == 0){
                    val_buffer_->Reset();
                    buffers_.erase(begin);
                    buffer_pool_->Release(val_buffer_, 1);
                }
                break;
            }
        }

//        LOG_DEBUG("erase the record segment of the inner node valen buffer");
    }
    /**
     * Reserve an undo record with the given size.
     * @param size the size of the undo record to allocate
     * @return a new undo record with at least the given size reserved
     */
    std::pair<uint32_t, char *>NewEntry(const uint32_t size) {
        SpinLatch::ScopedSpinLatch guard(&latch);

        auto back_block = buffers_.back().second;
        if (buffers_.empty() || !back_block->HasBytesLeft(size)) {
            // we are out of space in the buffer. Get a new buffer segment.
            ValenBuffer *new_segment = nullptr;
            bool rel = false;

            new_segment = reinterpret_cast<ValenBuffer *>(buffer_pool_->Get(1));
            assert(new_segment != nullptr);

            M_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0,  "a delta entry should be aligned to 8 bytes");

            buffer_size++;
            valen_buffer_pair new_itm = std::make_pair(buffer_size,new_segment);
            buffers_.push_back(new_itm);
        }
        auto buffers_back = buffers_.back();
        ValenBuffer *valen_buffer_ = buffers_back.second;
        char *last_record_ = valen_buffer_->Reserve(size);
        auto last_record_index_ = buffers_back.first;
        assert(last_record_ != nullptr);

        return std::make_pair(last_record_index_, last_record_);
    }

    /**
     * @return a pointer to the beginning of the last record requested,
     *          or nullptr if no record exists.
     */
//    char *LastRecord() const { return last_record_; }

private:
    typedef std::pair<uint64_t ,ValenBuffer *> valen_buffer_pair;
    SpinLatch latch;
    ValenBufferPool *buffer_pool_;
    std::list<valen_buffer_pair> buffers_;
    uint64_t buffer_size=0;

};

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * this only used by initializing a leaf node of the btree
 */
class DramBlock {
    /**
     * Contents of the dram block.
     */
    char content_[DRAM_BLOCK_SIZE];
};

/**
* Custom allocator used for the object pool of buffer segments
* Get, Reuse, Release handled in object pool
*/
class DramBlockAllocator {
public:
    /**
     * Allocates a new object by calling its constructor.
     * @return a pointer to the allocated object.
     */
    DramBlock *New() {

        DramBlock *dram_block = new DramBlock;

        return dram_block;
    }

    /**
     * Reuse a reused chunk of memory to be handed out again
     * @param reused memory location, possibly filled with junk bytes
     */
    void Reuse(DramBlock *reused) {
      /* no need operation, handled in object pool*/
    }

    /**
     * Deletes the object by calling its destructor.
     * @param ptr a pointer to the object to be deleted.
     */
    void Delete(DramBlock *const ptr) {
        delete ptr;
    }
};

using DramBlockPool = ObjectPool<DramBlock, DramBlockAllocator>;

/**
 * A block is a chunk of memory used for storage. It does not have any meaning
 * this only used by initializing a leaf node of the btree
 */
//class DramBlockS1 {
//    /**
//     * Contents of the dram block.
//     */
//    char content_[S1_DRAM_BLOCK_SIZE];
//};

/**
* Custom allocator used for the object pool of buffer segments
* Get, Reuse, Release handled in object pool
*/
//class DramBlockS1Allocator {
//public:
//    /**
//     * Allocates a new object by calling its constructor.
//     * @return a pointer to the allocated object.
//     */
//    DramBlockS1 *New() {
//
//        DramBlockS1 *dram_block = new DramBlockS1;
//
//        return dram_block;
//    }
//
//    /**
//     * Reuse a reused chunk of memory to be handed out again
//     * @param reused memory location, possibly filled with junk bytes
//     */
//    void Reuse(DramBlockS1 *const reused) {
//        /* no need operation, handled in object pool*/
//    }
//
//    /**
//     * Deletes the object by calling its destructor.
//     * @param ptr a pointer to the object to be deleted.
//     */
//    void Delete(DramBlockS1 *const ptr) {
//        delete ptr;
//    }
//};
//
//using DramBlockS1Pool = ObjectPool<DramBlockS1, DramBlockS1Allocator>;
