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
#include "batcher.h"

#include "macros.h"

// structures and types --------------------------------------------------------

typedef struct
{
    _Atomic bool readable_copy;       // 0 if A, 1 if B
    _Atomic bool has_read_or_written; // basically the "access set"
    _Atomic bool written_this_epoch;  // true if written in this epoch
    _Atomic uint32_t txid;            // transaction ID of the writer (0 if none)
} ctrl;

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

    // control
    ctrl *control;

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
    segment_list pending_free;

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

// helper functions ------------------------------------------------------------

txrecord *get_transaction_record(tx_t tx) { return (txrecord *)(uintptr_t)tx; }

bool read_word(region *unused(r), txrecord *t, size_t unused(index), void *unused(target))
{
    if (t->is_ro)
    {
        // read the readable copy into target
        if (1)
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
bool write_word(region *unused(r), txrecord *unused(tx), size_t unused(index), void const *unused(source))
{
    // txrecord *t = get_transaction_record(tx);
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

inline uint8_t *readable_ptr(segment_node *seg, size_t index, bool readable_copy)
{
    return readable_copy ? (seg->copyB + index * seg->align) : (seg->copyA + index * seg->align);
}

inline uint8_t *writable_ptr(segment_node *seg, size_t index, bool readable_copy)
{
    return readable_copy ? (seg->copyA + index * seg->align) : (seg->copyB + index * seg->align);
}

inline int is_power_of_2(size_t x) { return x && ((x & (x - 1)) == 0); }

// returns the index of the word corresponding to the address
bool base_addr_to_index(region *R, void const *addr, size_t *out_idx)
{
    const segment_node *S = &R->base;
    uintptr_t a = (uintptr_t)addr;
    uintptr_t beg = (uintptr_t)S->copyA;
    uintptr_t end = beg + S->size; // size == words*align
    if (a < beg || a >= end)
        return false;
    *out_idx = (a - beg) / S->align;
    return true;
}

void wl_append(region *reg, size_t idx)
{
    size_t position = atomic_fetch_add_explicit(&reg->wl_size, 1, memory_order_acq_rel);
    if (position < reg->wl_capacity)
        reg->write_list[position] = idx;
}

segment_node *find_segment(region *r, void *p)
{
    if (r->base.copyA == (uint8_t *)p)
        return &r->base;
    for (segment_node *sn = r->allocs; sn; sn = sn->next)
        if (sn->copyA == (uint8_t *)p)
            return sn;
    return NULL;
}

void free_segment_node(segment_node *sn)
{
    free(sn->control);
    free(sn->copyA);
    free(sn->copyB);
    free(sn);
}

void epoch_boundary(region *unused(r))
{
    // TODO
}

// -----------------------------------------------------------------------------

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t unused(size), size_t unused(align))
{
    if (!is_power_of_2(align))
        return invalid_shared;
    if (size == 0 || size % align != 0)
        return invalid_shared;
    if (size > (1ULL << 48))
        return invalid_shared;

    region *region = (struct region *)malloc(sizeof(struct region));
    if (unlikely(!region))
    {
        return invalid_shared;
    }

    region->align = align;
    region->size = size;

    // initialize base segment
    segment_node *base = &region->base;
    base->align = align;
    base->size = size;
    base->words = size / align;

    // allocate the 2 copies and control struct
    if (posix_memalign((void **)&(base->copyA), align, size) != 0)
    {
        free(region);
        return invalid_shared;
    }
    if (posix_memalign((void **)&(base->copyB), align, size) != 0)
    {
        free(base->copyA);
        free(region);
        return invalid_shared;
    }
    if (posix_memalign((void **)&(base->control), sizeof(void *), sizeof(ctrl) * base->words) != 0)
    {
        free(base->copyA);
        free(base->copyB);
        free(region);
        return invalid_shared;
    }

    // initialize the data with 0s and initialize the control struct
    memset(base->copyA, 0, base->words * align);
    memset(base->copyB, 0, base->words * align);
    for (size_t i = 0; i < base->words; i++)
    {
        atomic_init(&base->control[i].readable_copy, false);
        atomic_init(&base->control[i].has_read_or_written, false);
        atomic_init(&base->control[i].written_this_epoch, false);
        atomic_init(&base->control[i].txid, 0);
    }

    // initialize write list
    region->wl_capacity = base->words; // TODO: check
    region->write_list = (size_t *)malloc(region->wl_capacity * sizeof(size_t));
    if (!region->write_list)
    {
        free(base->control);
        free(base->copyA);
        free(base->copyB);
        free(region);
        return invalid_shared;
    }
    atomic_init(&region->wl_size, 0);

    // initialize batcher
    batcher_init(&region->batcher);
    region->start = (void *)base->copyA;
    region->allocs = NULL;
    region->pending_free = NULL;

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t unused(shared))
{
    struct region *region = (struct region *)shared;
    // free all segments
    while (region->allocs)
    {
        segment_list next = region->allocs->next;
        free(region->allocs->control);
        free(region->allocs->copyA);
        free(region->allocs->copyB);
        free(region->allocs);
        region->allocs = next;
    }
    // write list
    free(region->write_list);
    // batcher
    batcher_destroy(&region->batcher);
    // base segment
    free(region->base.control);
    free(region->base.copyA);
    free(region->base.copyB);
    // region
    free(region);
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
    // TODO: what are failure conditions ?

    region *r = (region *)shared;

    txrecord *t = (txrecord *)malloc(sizeof(txrecord));
    if (unlikely(!t))
        return invalid_tx;

    t->is_ro = is_ro;
    t->aborted = false;

    enter_batcher(&r->batcher);
    t->epoch = get_epoch(&r->batcher);
    t->id = (uint32_t)((uintptr_t)t & 0x7ffffffff);

    return (tx_t)t;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t unused(shared), tx_t unused(tx))
{
    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    bool committed = !t->aborted;
    leave_batcher(&r->batcher);
    return committed;
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

    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    if (size == 0 || (size % r->align) != 0)
    {
        t->aborted = true;
        return false;
    }

    size_t index;
    if (!base_addr_to_index(r, source, &index))
    {
        t->aborted = true;
        return false;
    }

    size_t n = size / r->align;
    uint8_t *out = (uint8_t *)target;

    // for each word index within [source, source + size[
    for (size_t i = 0; i < n; ++i, ++index, out += r->align)
    {
        if (index >= r->base.words)
        {
            t->aborted = true;
            return false;
        }
        if (!read_word(r, t, index, out))
        {
            t->aborted = true;
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

    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    if (size == 0 || (size % r->align != 0))
    {
        t->aborted = true;
        return false;
    }

    size_t index;
    if (!base_addr_to_index(r, target, &index))
    {
        t->aborted = true;
        return false;
    }

    size_t n = size / r->align;
    uint8_t const *in = (uint8_t const *)source;

    // for each word index within [target, target+size[
    for (size_t i = 0; i < n; ++i, ++index, in += r->align)
    {
        if (index >= r->base.words)
        {
            t->aborted = true;
            return false;
        }
        if (!write_word(r, t, index, in))
        {
            t->aborted = true;
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

    region *r = (region *)shared;
    if (!r || !target)
        return abort_alloc;
    if (size == 0 || (size % r->align) != 0)
        return abort_alloc;

    size_t align = ((struct region *)shared)->align;
    size_t words = size / align;

    // data segment
    struct segment_node *sn = (struct segment_node *)malloc(sizeof(struct segment_node));
    if (unlikely(!sn))
        return nomem_alloc;
    sn->size = size;
    sn->words = words;
    sn->align = align;

    // copies
    size_t data = align < sizeof(void *) ? sizeof(void *) : align; // sizeof(segment) or void*
    if (posix_memalign((void **)&sn->copyA, data, size) != 0)
    {
        free(sn);
        return nomem_alloc;
    }
    if (posix_memalign((void **)&sn->copyB, data, size) != 0)
    {

        free(sn->copyA);
        free(sn);
        return nomem_alloc;
    }

    // control
    size_t ctrl = sizeof(void *);
    if (posix_memalign((void **)&sn->control, ctrl, sizeof(ctrl) * words) != 0)
    {
        free(sn->copyA);
        free(sn->copyB);
        free(sn);
        return nomem_alloc;
    }

    // zero the data
    memset(sn->copyA, 0, size);
    memset(sn->copyB, 0, size);
    for (size_t i = 0; i < words; i++)
    {
        atomic_init(&sn->control[i].readable_copy, false);
        atomic_init(&sn->control[i].has_read_or_written, false);
        atomic_init(&sn->control[i].written_this_epoch, false);
        atomic_init(&sn->control[i].txid, 0);
    }

    pthread_mutex_lock(&r->batcher.lock);
    sn->prev = NULL;
    sn->next = r->allocs;
    if (sn->next)
        sn->next->prev = sn;
    r->allocs = sn;
    pthread_mutex_unlock(&r->batcher.lock);

    *target = (void *)sn->copyA;

    return success_alloc;
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

    region *r = (region *)shared;
    (void)tx;
    if (!r || !target)
        return false;

    pthread_mutex_lock(&r->batcher.lock);
    segment_node *sn = find_segment(r, target);
    if (!sn || sn == &r->base)
    {
        pthread_mutex_unlock(&r->batcher.lock);
        return false;
    }

    // unlink
    if (sn->prev)
        sn->prev->next = sn->next;
    else
        r->allocs = sn->next;
    if (sn->next)
        sn->next->prev = sn->prev;
    sn->prev = sn->next = NULL;

    // add to pending
    sn->next = r->pending_free;
    if (r->pending_free)
        r->pending_free->prev = sn;
    r->pending_free = sn;
    pthread_mutex_unlock(&r->batcher.lock);

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
