#pragma once

/*
 This is a "continuous array" abstraction specifically designed for work-stealing deques.
 The continuous array aims to allow the work-stealing deque to treat its array as if it were an
 infinite array (bounded by memory, of course).
 It works by allocating and de-allocating block chunks, that are used to store the deque's values.
 It assumes that only the owner of the deque will uses the put_head and get_head methods and that
 thiefs use the get_tail method.
*/

//TODO currently it is not known when it is safe to deallocate blocks, the user of this class is responsible for that.

#define BLOCK_SIZE_LOG 14

template<typename T>
class alignas(64) continuous_array
{
//TODO: add mechanism to reclaim blocks that were allocated due to owner_pushes, but are no longer
//  in use due to owner_pops.
//TODO: At the moment, blocks are only reused (never free'd). Add heuristic to free some blocks

  private:
    
    //number of T elements in each block
    static inline constexpr size_t block_size = (1 << BLOCK_SIZE_LOG); 
    static inline constexpr size_t block_size_mask = block_size - 1; 
    
    struct alignas(64) block {
      block *prev; //Contains the block with this block's block_id-1 (if it exists)
      block *next; //Contains the block with this block's block_id+1 (if it exists)
      //ids are unique and monotonically increasing
      //A block contains indexes from block_id*block_size to (block_id+1)*block_size-1
      uint64_t block_id; 
      std::atomic<T> array[block_size];
    };
    
    //Accessed by owner//
    block *head = nullptr; //Head of block list
    block *block_in_use = nullptr; //Block that is currently in use
    block* blocks_to_reuse = nullptr; //Block pool
    uint64_t curr_block_id = 0; //Last allocated block_id
    //-----------------//
    
    //Accessed by all//
    alignas(64) std::atomic<block*> tail;
    alignas(64) std::atomic<block*> to_retire {nullptr};
    //---------------//
    
    block*
    get_block()
    //Allocate or get a previously allocated block
    {
      block *new_block = pop_reuse_block();
      if(new_block == nullptr) //If no blocks available, allocate one
        new_block = (block*) malloc(sizeof(block));
      assert(new_block != nullptr); //TODO: treat this correctly
      return new_block;
    }
    
    void
    push_reuse_block(block *b)
    //Add block to the reuse list
    {
      b->prev = blocks_to_reuse;
      blocks_to_reuse = b;
    }
    
    block*
    pop_reuse_block()
    //Get block from the reuse list
    {
      block *b = blocks_to_reuse;
      if(b == nullptr)
        return nullptr;
      blocks_to_reuse = b->prev;
      return b;
    }
    
    void
    ca_alloc()
    //Allocate one block and put it at the head
    {
      //Allocate block from pool
      block *new_block = get_block();
      new_block->prev = head;
      new_block->next = nullptr; //new block does not have any next yet
      new_block->block_id = curr_block_id++;
      //Head should only be null on the first call
      //We take care of this corner case by allocating the first block differently.
      head->next = new_block;
      head = new_block;
    }
    
    void
    ca_retire_last_block()
    //Swing tail from last to next to last block and add last block for retirement
    {
      block *old_block = tail.load(std::memory_order_relaxed);
      assert(old_block != nullptr); //Tail should never be null
      block *next_old_block = old_block->next;
      //We shouldn't be trying to pop a block that is not there
      assert(next_old_block != nullptr); 
      while(!tail.compare_exchange_strong(old_block, next_old_block))
      {
        //CAS failed, but should we retry?
        if(next_old_block->block_id < tail.load(std::memory_order_relaxed)->block_id)
          //Someone updated tail to a more up-to-date value
          //This means that the other thread did our job for us as well
          break; 
      }
      //Block tail points to must be "younger" than block we popped
      assert(tail.load(std::memory_order_relaxed)->block_id > old_block->block_id);
      //Now add block to the retire list
      block *old_to_retire;
      do
      {
        old_to_retire = to_retire.load(std::memory_order_relaxed);
        old_block->prev = old_to_retire;
      } while(!to_retire.compare_exchange_strong(old_to_retire, old_block));
    }
    
    block*
    get_retired()
    //Get the list of nodes that were retired, and reset the retired list
    //Called by owner
    {
      block *old_to_retire = nullptr;
      do
      { //Continuously try to empty the retire list
        old_to_retire = to_retire.load(std::memory_order_relaxed);
      } while(old_to_retire != nullptr && !to_retire.compare_exchange_strong(old_to_retire, nullptr));
    
      //If both lists are not empty, the newly retired nodes must be younger
      assert(old_to_retire == nullptr ||
        to_retire.load(std::memory_order_relaxed) == nullptr || 
        to_retire.load(std::memory_order_relaxed)->block_id > old_to_retire->block_id);
             
      return old_to_retire;
    }
        
  public:
    continuous_array()
    {
      //Allocate first block
      head = get_block();
      head->prev = nullptr;
      head->next = nullptr;
      head->block_id = curr_block_id++;
      //At construction, tail is also the first block allocated
      tail.store(head); 
      block_in_use = head;
    }
    
    ~continuous_array()
    //Assumes no concurrent accesses are done anymore
    {
      block *r = get_retired(); //Get all retired (not yet free'd) nodes
      block *aux;
      //If tail or head is null, something went wrong
      assert(tail.load(std::memory_order_relaxed) != nullptr); 
      assert(head != nullptr); 
      tail.load(std::memory_order_relaxed)->prev = r; //Attach head to retired list
      while(head != nullptr)
      { //free blocks in current working set AND retired blocks
        aux = head;
        head = head->prev;
        free(aux);
      }
      while((aux = pop_reuse_block()) != nullptr)
      { //free blocks stored for future reuse
        free(aux);
      }
    }
    
    T
    get_head(uint64_t index)
    //Get from top of array
    //  Read value at position <index>, starting from head block
    {
      uint64_t desired_block_id = (index >> BLOCK_SIZE_LOG);
      uint64_t offset = index & block_size_mask;  
      if(block_in_use->block_id != desired_block_id)
      {
        block_in_use = block_in_use->prev;
      }
      assert(block_in_use->block_id == desired_block_id); //index we are looking for must be allocated
      return block_in_use->array[offset].load(std::memory_order_relaxed); // atomic load
    }
    
    T
    get_tail(uint64_t index)
    //Get from bottom of array
    {
      uint64_t desired_block_id = (index >> BLOCK_SIZE_LOG);
      uint64_t offset = index & block_size_mask;  
      block* t = tail.load(std::memory_order_relaxed);
      block* b = t;
      //Look forward from tail
      while(b != nullptr && b->block_id < desired_block_id) { b = b->next; }
      //We haven't found the block we want, look into blocks to be retired
      //  Another thread performing ca_retire_last_block() might have updated tail and we now have
      //  to search backwards, instead of backwards to find the correct block.
      //TODO: check this with std::memory_order_relaxed memory models
      while(b != nullptr && b->block_id > desired_block_id) { b = b->prev; }
      assert(b->block_id == desired_block_id); //index we are looking for must be allocated
    
      if(offset == 0
        && b != t
        && t->block_id == desired_block_id - 1)
      //We are not accessing tail, which means that (soon) tail block will never be accessed again.
      {
        ca_retire_last_block();
      }
    
      return b->array[offset].load(std::memory_order_relaxed); //atomic load
    }
    
    void
    put_head(uint64_t index, T val)
    //Must be owner calling this
    {
      uint64_t desired_block_id = (index >> BLOCK_SIZE_LOG);
      uint64_t offset = index & block_size_mask;  
      __builtin_prefetch(&block_in_use->array[offset], 1, 1);
      if(offset == 0)
      //We are at the end of a block
      {
        if(curr_block_id == desired_block_id) 
        //Next block is not allocated, allocate it
        {
          ca_alloc();
        }
        if(desired_block_id != block_in_use->block_id)
        {
          assert(block_in_use != nullptr);
          block_in_use = block_in_use->next;
        }
      }
      //We must always find the desired block
      assert(block_in_use->block_id == desired_block_id);
      block_in_use->array[offset].store(val, std::memory_order_relaxed); // shared store
    }
    
    void
    free_retired()
    //Free previously retired blocks
    //Must be called after it is safe to reclaim retired blocks
    {
      block *r = get_retired();
      block *aux;
      while(r != nullptr)
      {
        aux = r;
        r = r->prev;
        //free(aux);
        //TODO: add logic to decide whether to free or add for reuse
        push_reuse_block(aux);
      }
    }
    
    friend std::ostream& operator<<(std::ostream& os, continuous_array *ca)
    {
      block *b = ca->head;
      os << "head";
      while(b != nullptr)
      {
        os << "::<" << b->block_id << ";";
        os << b->array[0];
        for(size_t i = 1; i < block_size; i++)
          os << "," << b->array[i];
        os << ">";
        b = b->prev;
      }
      return os << "::tail";
    }
};


