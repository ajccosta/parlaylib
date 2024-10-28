#include "gtest/gtest.h"

#include <utility>
#include <vector>
#include <thread>

#include <parlay/internal/work_stealing_deque.h>


TEST(TestDeque, TestParlayDequePopWhatYouPushOwner) {
  auto deq = new parlay::internal::Deque<int>();
  constexpr int sz = 10000; //Deque is unbounded, so this is arbitrary
  int *arr = new int[sz];
  for(int i = 0; i < sz; i++) {
    arr[i] = i;
    deq->push_bottom(&arr[i]);
  }
  for(int i = 0; i < sz; i++) {
    int *res = deq->pop_bottom();
    ASSERT_EQ(res != nullptr && *res == sz - i - 1, true);
  }
  delete [] arr;
  delete deq;
}

TEST(TestDeque, TestParlayDequePopWhatYouPushThieves) {
  auto deq = new parlay::internal::Deque<int>();
  constexpr int sz = 10000; //Deque is unbounded, so this is arbitrary
  int *arr = new int[sz];
  for(int i = 0; i < sz; i++) {
    arr[i] = i;
    deq->push_bottom(&arr[i]);
  }
  for(int i = 0; i < sz; i++) {
    std::pair<int*, bool> p = deq->pop_top();
    ASSERT_EQ(p.first != nullptr && *p.first == i, true);
  }
  delete [] arr;
  delete deq;
}

TEST(TestDeque, TestParlayDequeConcurrent) {
  auto deq = new parlay::internal::Deque<int>();
  constexpr int sz = 1000000; //Deque is unbounded, so this is arbitrary
  int *arr = new int[sz];
  for(int i = 1; i < sz; i++) { //Use 0 as ⊥
    arr[i] = i;
    deq->push_bottom(&arr[i]);
  }
  std::vector<std::thread> threads;
  constexpr int nthreads = 32;
  std::atomic<bool> wait {false};
  //<result_arr> is an array filled with 0s, with enough space for each thread to write <sz> results
  int *result_arr = new int[sz * nthreads];
  memset(result_arr, 0, sz * nthreads * sizeof(int));
  for(int tid = 0; tid < nthreads; tid++) {
      threads.push_back(std::thread([tid, &wait,
      &deq, result_arr] { 

        while(wait.load() != true) {}
        int i = 0;
        if(tid == 0) { //owner
          while(true) {
            int *res = deq->pop_bottom();
            if(res == nullptr) //Deque is empty
              break;
            result_arr[i++] = *res;
          }
        } else { //thieves
          int offset = sz * tid;
          while(true) {
            std::pair<int*, bool> p = deq->pop_top();
            if(p.first != nullptr)
              result_arr[offset + i++] = *(p.first);
            if(p.second == true) //Deque is empty
              break;
          }
        }
      }));
  }
  wait.store(true);
  for(int tid = 0; tid < nthreads; tid++) {
    threads[tid].join();
  }
  std::vector<int> vals;
  for(int tid = 0; tid < nthreads; tid++) {
    for(int j = 0; j < sz; j++) {
      auto r = result_arr[tid*sz+j];
      if(r == 0)
        break; //0 is not pushed, it means this thread does not have more results
      vals.push_back(r);
    }
  }
  ASSERT_EQ(vals.size(), sz - 1); //sz - 1 values were popped
  std::sort(vals.begin(), vals.end());
  for (int i = 1; i < sz; i++) //All values pushed must have been popped (either by owner or thieves)
    ASSERT_EQ(vals[i-1], i);
  delete [] result_arr;
  delete [] arr;
  delete deq;
}
