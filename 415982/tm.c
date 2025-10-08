/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
 **/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L
#ifdef __STDC_NO_ATOMICS__
#error Current C11 compiler does not support atomic operations
#endif

// External headers
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Internal headers
#include <tm.h>

#include "macros.h"

// structures and types --------------------------------------------------------

typedef struct
{
    pthread_mutex_t lock;
    pthread_cond_t cond;

    uint32_t counter;   // current epoch
    uint32_t remaining; // number of active transactions
    uint32_t waiting;   // threads waiting
} batcher;

typedef struct segment_node
{
    struct segment_node *next;
    struct segment_node *prev;

    size_t size;  // size (bytes) of the segment
    size_t words; // bytes / align
    size_t align; // size of a word (bytes)

    // actual data
    uint8_t *copyA;
    uint8_t *copyB;

    // control (TODO: merge with bit fields)
    _Atomic bool readable_copy;       // 0 if A, 1 if B
    _Atomic bool has_read_or_written; // basically the "access set"
    _Atomic bool written_this_epoch;  // true if written in this epoch
    _Atomic uint32_t txid;            // transaction ID of the writer (0 if none)

} segment_node;

typedef struct segment_node *segment_list;

typedef struct region
{
    // batcher stuff
    batcher batcher;

    // write list stuff (global por agora -- so para o segmento base)
    _Atomic size_t wl_size; // nº de entradas validas
    size_t wl_capacity;     // capacidade do array
    size_t *write_list;     // array of word indices that were written in this epoch

    segment_node base; // base segment (non-freeable)

    segment_list allocs; // list of dynamically allocated segments

    void *start;  // start of the shared memory region
    size_t align; // size of a word
    size_t size;  // size of the base segment
} region;

enum
{
    FLAG_READABLE = 1 << 0, // 0 if A, 1 if B
    FLAG_WRITTEN = 1 << 1,  // 1 if written in this epoch
    FLAG_LISTED = 1 << 2    // 1 if in the write list
};

typedef struct
{
    bool is_ro;
    bool aborted;
    uint32_t epoch;
    uint32_t id;
} txrecord;

// -----------------------------------------------------------------------------

// batcher stuff ---------------------------------------------------------------
// these functions should be atomic
uint32_t get_epoch(batcher *bat)
{
    // can only be called by a thread after it entered the batcher, and before it left
    pthread_mutex_lock(&bat->lock);
    uint32_t epoch = bat->counter;
    pthread_mutex_unlock(&bat->lock);
    return epoch;
}
void enter(batcher *bat)
{
    pthread_mutex_lock(&bat->lock);
    if (bat->remaining == 0)
    {
        bat->remaining = 1;
        pthread_mutex_unlock(&bat->lock);
        return;
    }

    // didnt work, wait for next batch
    bat->waiting++;
    uint32_t my_epoch = bat->counter;
    do
    {
        pthread_cond_wait(&bat->cond, &bat->lock);
    } while (my_epoch == bat->counter);
    pthread_mutex_unlock(&bat->lock);
}
void leave(batcher *bat)
{
    pthread_mutex_lock(&bat->lock);
    bat->remaining--;
    if (bat->remaining == 0)
    {
        bat->counter++;
        bat->remaining = bat->waiting;
        bat->waiting = 0;
        pthread_cond_broadcast(&bat->cond);
    }
    pthread_mutex_unlock(&bat->lock);
}

void batcher_init(batcher *bat)
{
    pthread_mutex_init(&bat->lock, NULL);
    pthread_cond_init(&bat->cond, NULL);
    bat->counter = 0;
    bat->remaining = 0;
    bat->waiting = 0;
}
void batcher_destroy(batcher *bat)
{
    pthread_mutex_destroy(&bat->lock);
    pthread_cond_destroy(&bat->cond);
}

// -----------------------------------------------------------------------------

// helper functions ------------------------------------------------------------

txrecord *get_transaction_record(tx_t tx) { return (txrecord *)(uintptr_t)tx; }

bool read_word(shared_t unused(shared), tx_t unused(tx), unused(void *index), unused(void *target))
{
    txrecord *t = get_transaction_record(tx);
    if (t->is_ro)
    {
        // read the readable copy into target
        return true;
    }
    if (1 /* the word has been written in the current epoch*/)
    {
        if (1 /* this transaction is already in the "access set" */)
        {
            // read the writable copy into target
            return true;
        }
        return false; // abort transaction
    }
    // read the readable copy into target
    // add the transaction to the "access set" if not there already
    return true;
}
bool write_word(shared_t unused(shared), tx_t unused(tx), unused(void *index), unused(void const *source))
{
    txrecord *t = get_transaction_record(tx);
    if (1 /* the word has been written in the current epoch*/)
    {
        if (1 /* this transaction is already in the "access set" */)
        {
            // write the content at source into the writable copy
            return true;
        }
        return false; // abort transaction
    }
    if (1 /* at least one other transaction is in the "access set"*/)
    {
        return false; // abort transaction
    }
    // write the content at source into the writable copy
    // add the transaction to the "access set" if not there already
    // mark the word as written in this epoch
    return true;
}

// -----------------------------------------------------------------------------

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t
tm_create(size_t unused(size), size_t unused(align))
{

    region *region = (struct region *)malloc(sizeof(region));
    if (unlikely(!region))
    {
        return invalid_shared;
    }

    // check if alignment is a power of 2 and size is a positive multiple of the alignment
    if (align == 0 || (align & (align - 1)) != 0 || size % align != 0)
    {
        free(region);
        return invalid_shared;
    }

    if (posix_memalign(&(region->start), align, size) != 0)
    {
        free(region);
        return invalid_shared;
    }

    region->align = align;
    region->size = size;

    // initialize base segment
    region->base.align = align;
    region->base.size = size;
    region->base.words = size / align;
    /*region->base.data = aligned_alloc(64, region->base.words * sizeof(word_t));
    region->base.ctrl = aligned_alloc(64, region->base.words * sizeof(ctrl_t));
    if (unlikely(!region->base.data || !region->base.ctrl))
    {
        free(region->base.data);
        free(region->base.ctrl);
        free(region);
        return invalid_shared;
    }*/

    // TODO: initialize the base segment, epoch, write list, etc

    region->allocs = NULL;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t unused(shared))
{
    // TODO: tm_destroy(shared_t)
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
 **/
void *tm_start(shared_t unused(shared))
{
    return ((struct region *)shared)->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t unused(shared))
{
    return ((struct region *)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t unused(shared))
{
    return ((struct region *)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro))
{
    // TODO: tm_begin(shared_t)
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t unused(shared), tx_t unused(tx))
{
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
 **/
bool tm_read(shared_t unused(shared), tx_t unused(tx), void const *unused(source), size_t unused(size), void *unused(target))
{
    // TODO (checks): size must be positive multiple of alignment
    // source and target must be at least size bytes long
    // source and target addresses must be a positive multiple of alignment

    // for each word index within [source, source + size[
    for (int i = 0;;)
    {
        if (!read_word(shared, tx, i, target /*+ offset*/))
        {
            return false;
        }
    }
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
 **/
bool tm_write(shared_t unused(shared), tx_t unused(tx), void const *unused(source), size_t unused(size), void *unused(target))
{
    // TODO (checks): size must be positive multiple of alignment
    // source and target must be at least size bytes long
    // source and target addresses must be a positive multiple of alignment

    // for each word index within [target, target+size[
    for (int i = 0;;)
    {
        if (!write_word(shared, tx, i, source /*+ offset*/))
        {
            return false;
        }
    }
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
 **/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void **unused(target))
{
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)

    // allocate enough space for a segment of size size
    // if allocation fails, return abort_alloc

    // initialize the control structure(s) (one per word) in the segment;
    // initialize each copy of each word in the segment to zeroes;
    // register the segment in the set of allocated segments;
    return 0;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void *unused(target))
{
    // TODO: tm_free(shared_t, tx_t, void*)

    // mark target for deregistering (from the set of allocated segments)
    // and freeing once the last transaction of the current epoch leaves
    // the Batcher, if the calling transaction ends up being committed;

    return true;
}

bool commit()
{
    // for each written word index:
    //      defer the swap, of which copy for word index is the “valid”
    //      copy, just after the last transaction from the current epoch
    //      leaves the Batcher and before the next batch starts running;
    return true;
}
