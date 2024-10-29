
#ifndef PARLAY_INTERNAL_POOL_ALLOCATOR_H
#define PARLAY_INTERNAL_POOL_ALLOCATOR_H

#include <cassert>
#include <cstddef>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <iterator>
#include <memory>
#include <new>
#include <optional>
#include <utility>
#include <vector>

#include "../portability.h"
#include "../utilities.h"

#include "block_allocator.h"

#include "concurrency/hazptr_stack.h"

// IWYU pragma: no_include <array>

namespace parlay {
namespace internal {

// ****************************************
//    pool_allocator
// ****************************************

// Allocates headerless blocks from pools of different sizes.
// A vector of pool sizes is given to the constructor.
// Sizes must be at least 8, and must increase.
// For pools of small blocks (below large_threshold) each thread keeps a
// thread local list of elements from each pool using the block_allocator.
// For large blocks there is only one pool shared by all threads. For
// blocks larger than the maximum pool size, allocation and deallocation
// is performed directly by operator new.
struct pool_allocator {

  // Maximum alignment guaranteed by the allocator
  static inline constexpr size_t max_alignment = 128;
  
 private:
  static inline constexpr size_t large_threshold = (1 << 18);

  size_t num_buckets; //Number of blocks size categories
  size_t num_small; //Number of small block size categories
  size_t max_small; //Largest small block size category
  size_t max_size; //Size of the largest block size category
  std::atomic<size_t> large_allocated{0}; //Number of bytes used by large blocks currently allocated 
  std::atomic<size_t> large_used{0}; //Number of bytes used large blocks currently being used (i.e., allocated by user) 

  std::unique_ptr<size_t[]> sizes;
  std::unique_ptr<internal::hazptr_stack<void*>[]> large_buckets;
  unique_array<block_allocator> small_allocators;

  void* allocate_large(size_t n) {

    size_t bucket = num_small;
    size_t alloc_size;
    large_used += n; //Update total number of large block bytes used by user

    if (n <= max_size) {
      //Get block from large_buckets
      while (n > sizes[bucket]) bucket++;
      std::optional<void*> r = large_buckets[bucket-num_small].pop();
      if (r) return *r; //bucket had block to fulfill desired size
      alloc_size = sizes[bucket]; //We will allocate from this size
    } else alloc_size = n; //Allocate something larger than is found in large buckets

    // Alloc size must be a multiple of the alignment
    // Round up to the next multiple.
    if (alloc_size % max_alignment != 0) {
      alloc_size += (max_alignment - (alloc_size % max_alignment));
    }

    void* a = ::operator new(alloc_size, std::align_val_t{max_alignment});

    large_allocated += n; //Update total number large block of bytes allocated
    return a;
  }

  void deallocate_large(void* ptr, size_t n) {
    large_used -= n; //Update total number of large block bytes used by user
    if (n > max_size) {
      ::operator delete(ptr, std::align_val_t{max_alignment});
      large_allocated -= n; //Update total number of large block bytes allocated
    } else {
      size_t bucket = num_small;
      while (n > sizes[bucket]) bucket++; //Find bucket that block belongs to
      large_buckets[bucket-num_small].push(ptr);
      //large buckets blocks are not deallocted
    }
  }

 public:
  pool_allocator(const pool_allocator&) = delete;
  pool_allocator(pool_allocator&&) = delete;
  pool_allocator& operator=(const pool_allocator&) = delete;
  pool_allocator& operator=(pool_allocator&&) = delete;

  ~pool_allocator() {
    clear();
  }

  explicit pool_allocator(const std::vector<size_t>& sizes_) :
      num_buckets(sizes_.size()),
      sizes(std::make_unique<size_t[]>(num_buckets)) {

    //Copy size array
    std::copy(std::begin(sizes_), std::end(sizes_), sizes.get());
    //Largest size
    max_size = sizes[num_buckets-1];
    //Calculate number of block sizes considered small
    num_small = 0;
    while (num_small < num_buckets && sizes[num_small] < large_threshold)
      num_small++;
    //Largest small block size
    max_small = (num_small > 0) ? sizes[num_small - 1] : 0;

    //Construct shared, large block size pools
    large_buckets = std::make_unique<internal::hazptr_stack<void*>[]>(num_buckets-num_small);
    //Construct thread-local, small block size pools
    small_allocators = make_unique_array<internal::block_allocator>(num_small, [&](size_t i) {
      return internal::block_allocator(sizes[i], max_alignment);
    });

#ifndef NDEBUG
    size_t prev_bucket_size = 0;
    for (size_t i = 0; i < num_small; i++) {
      size_t bucket_size = sizes[i];
      assert(bucket_size >= 8);
      assert(bucket_size > prev_bucket_size);
      prev_bucket_size = bucket_size;
    }
#endif
  }

  void* allocate(size_t n) {
    //Do we consider this allocation large? If yes, allocate from large pools
    if (n > max_small) return allocate_large(n);
    //Calculate smallest bucket needed for this allocation
    size_t bucket = 0;
    while (n > sizes[bucket]) bucket++;
    //Allocate from smallest possible bucket
    return small_allocators[bucket].alloc();
  }

  void deallocate(void* ptr, size_t n) {
    //Was this a large allocation? Return block to large bucket pool
    if (n > max_small) deallocate_large(ptr, n);
    else {
      //Calculate smallest bucket that was needed for this allocation
      size_t bucket = 0;
      while (n > sizes[bucket]) bucket++;
      //Return block to its bucket
      small_allocators[bucket].free(ptr);
    }
  }

  // allocate, touch, and free to make sure space for small blocks is paged in
  [[deprecated]] void reserve(size_t) { }

  void print_stats() {
    size_t total_a = 0;
    size_t total_u = 0;
    for (size_t i = 0; i < num_small; i++) {
      size_t bucket_size = sizes[i];
      size_t allocated = small_allocators[i].num_allocated_blocks();
      size_t used = small_allocators[i].num_used_blocks();
      total_a += allocated * bucket_size;
      total_u += used * bucket_size;
      std::cout << "size = " << bucket_size << ", allocated = " << allocated
                << ", used = " << used << std::endl;
    }
    std::cout << "Large allocated = " << large_allocated << std::endl;
    std::cout << "Total bytes allocated = " << total_a + large_allocated << std::endl;
    std::cout << "Total bytes used = " << total_u << std::endl;
  }

  // pair of total currently used space, and total unused space the allocator has in reserve
  std::pair<size_t,size_t> stats() {
    size_t total_a = large_allocated;
    size_t total_u = large_used;
    for (size_t i = 0; i < num_small; i++) {
      size_t bucket_size = sizes[i];
      size_t allocated = small_allocators[i].num_allocated_blocks();
      size_t used = small_allocators[i].num_used_blocks();
      total_a += allocated * bucket_size;
      total_u += used * bucket_size;
    }
    return {total_u, total_a-total_u};
  }

  //Pop all blocks from large buckets (small buckets are managed by unique_ptr)
  void clear() {
    for (size_t i = num_small; i < num_buckets; i++) {
      std::optional<void*> r = large_buckets[i-num_small].pop();
      while (r) {
        large_allocated -= sizes[i];
        ::operator delete(*r, std::align_val_t{max_alignment});
        r = large_buckets[i-num_small].pop();
      }
    }
  }
};

}  // namespace internal
}  // namespace parlay

#endif  // PARLAY_INTERNAL_POOL_ALLOCATOR_H
