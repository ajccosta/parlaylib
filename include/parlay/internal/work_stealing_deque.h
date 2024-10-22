
#ifndef PARLAY_INTERNAL_WORK_STEALING_DEQUE_H_
#define PARLAY_INTERNAL_WORK_STEALING_DEQUE_H_

//Continuous array version of ABP work-stealing deque

#include <cassert>

#include <atomic>
#include <iostream>
#include <utility>

#include "continuous_array.h"

namespace parlay {
namespace internal {

// Deque based on "Correct and Efficient Work-Stealing for Weak Memory Models"
//  by Nhat Minh Lê, Antoniu Pop, Albert Cohen and Francesco Zappa Nardelli
//
// Instead of using a circular buffer, this implementation uses doubly-linked blocks,
//  similarly to "A dynamic-sized nonblocking work stealing deque" by Danny Hendler,
//  Yossi Lev, Mark Moir and Nir Shavi
//
// Supports:
//
// push_bottom:   Only the owning thread may call this
// pop_bottom:    Only the owning thread may call this
// pop_top:       Non-owning threads may call this
//
template <typename V>
struct alignas(128) Deque {

  //The ordering of these variables matters due to cache contention
  continuous_array<V*> *deq;
  alignas(64) std::atomic<uint64_t> bot; //index of where owner is pushing/popping
  alignas(64) std::atomic<uint64_t> top; //index of where thiefs are stealing

  Deque() : bot(0), top(0) {
    deq = new continuous_array<V*>();
  }

  ~Deque() {
    delete deq;
  }

  // Adds a new val to the bottom of the queue. Only the owning
  // thread can push new items. This must not be called by any
  // other thread.
  bool push_bottom(V* val) {
    auto local_bot = bot.load(std::memory_order_relaxed); // atomic load
    deq->put_head(local_bot, val);
    //std::atomic_thread_fence(std::memory_order_release);
    bot.store(local_bot + 1, std::memory_order_seq_cst);  // shared store
    return true; //We use this to count every successful push op. (which is all of them)
  }

  // Pop an item from the top of the queue, i.e., the end that is not
  // pushed onto. Threads other than the owner can use this function.
  //
  // Returns {val, empty}, where empty is true if val was the
  // only val on the queue, i.e., the queue is now empty
  std::pair<V*, bool> pop_top() {
    auto old_top = top.load(std::memory_order_acquire);    // atomic load
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto local_bot = bot.load(std::memory_order_acquire);  // atomic load
    assert(local_bot + 1 >= old_top); //Check invariant that bot never strays more than 1 from top
    if (local_bot > old_top) {
      if (top.compare_exchange_strong(old_top, old_top + 1)) {
        auto val = deq->get_tail(old_top);
        return {val, (local_bot == old_top + 1)};
      } else {
        return {nullptr, (local_bot == old_top + 1)};
      }
    }
    return {nullptr, true};
  }

  // Pop an item from the bottom of the queue. Only the owning
  // thread can pop from this end. This must not be called by any
  // other thread.
  V* pop_bottom() {
    auto b = bot.load(std::memory_order_relaxed); // atomic load
    if (b == 0) {
      return nullptr;
    }
    b--;
    assert(b != UINT64_MAX); //Check for underflow
    bot.store(b, std::memory_order_relaxed); // shared store
    std::atomic_thread_fence(std::memory_order_seq_cst);
    auto t = top.load(std::memory_order_relaxed); // atomic load
    V* val;
    if (t <= b) {
      val = deq->get_head(b);
      if (t == b) {
        if(!top.compare_exchange_strong(t, t + 1, std::memory_order_seq_cst, std::memory_order_relaxed)) {
          val = nullptr;
        }
        bot.store(b + 1, std::memory_order_relaxed);
      }
    } else {
      val = nullptr;
      bot.store(b + 1, std::memory_order_relaxed);
    }
    return val;
  }
};

}  // namespace internal
}  // namespace parlay

#endif  // PARLAY_INTERNAL_WORK_STEALING_DEQUE_H_
