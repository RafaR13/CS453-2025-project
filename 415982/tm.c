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
#include <stddef.h>
#include <stdio.h>

// Internal headers
#include <tm.h>

void epoch_boundary(void *ctx);
#include "batcher.h"

#include "macros.h"

#define SEGMENT_ID_BITS 16u
#define WORD_INDEX_BITS 48u
#define SEGMENT_ID_MASK (MAX_SEGMENTS - 1u)
#define WORD_INDEX_MASK ((1ULL << WORD_INDEX_BITS) - 1ULL)

#define MAX_SEGMENTS (1u << SEGMENT_ID_BITS)
#define BITSET_BYTES ((MAX_SEGMENTS + 7u) / 8u)

// structures and types --------------------------------------------------------

typedef struct ctrl
{
    _Atomic uint64_t written_in_epoch_and_readable;
    _Atomic uint64_t owner; // epoch || txid
    struct ctrl *next;      // linked list of written words
} ctrl;

typedef struct segment_node
{
    uint16_t id;

    // frees
    _Atomic bool pending_free;

    struct segment_node *next;
    struct segment_node *prev;

    struct segment_node *prev_free;
    struct segment_node *next_free;

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
    pthread_mutex_t written_lock;
    pthread_mutex_t free_lock;
    _Atomic uint32_t transaction_id_counter;

    segment_list allocs;       // list of dynamically allocated segments
    segment_list pending_free; // list of pending frees

    // tabela de segments
    segment_node **segment_table;
    _Atomic uint16_t segment_count;

    size_t align; // size of a word
    uint32_t log2_align;
    size_t size; // size of the base segment
} region;

typedef struct
{
    bool is_ro;
    bool aborted;
    uint32_t epoch;
    uint32_t id;

    // pending frees
    uint64_t *free_map; //[MAX_SEGMENTS / 64];
    segment_node *allocated_segments;
    ctrl *written_head;
    ctrl *written_tail;
} txrecord;

typedef struct
{
    segment_node *seg;
    size_t word_index;
} segment_and_index;

// -----------------------------------------------------------------------------

// Helpers ---------------------------------------------------------------------
static txrecord *get_transaction_record(tx_t tx) { return (txrecord *)(uintptr_t)tx; }

static inline uint8_t *readable_ptr(segment_node *seg, size_t index, bool readable_copy)
{
    return readable_copy ? (seg->copyB + index * seg->align) : (seg->copyA + index * seg->align);
}

static inline uint8_t *writable_ptr(segment_node *seg, size_t index, bool readable_copy)
{
    return readable_copy ? (seg->copyA + index * seg->align) : (seg->copyB + index * seg->align);
}

static inline int is_power_of_2(size_t x) { return x && ((x & (x - 1)) == 0); }

static inline void *encode_pointer(uint16_t segment_id, uint64_t byte_offset)
{
    return (void *)(((uint64_t)segment_id << WORD_INDEX_BITS) | (byte_offset & WORD_INDEX_MASK));
}

static inline void clear_bitmap(uint64_t bitmap[MAX_SEGMENTS / 64])
{
    memset(bitmap, 0, MAX_SEGMENTS / 8);
}

static inline void bitmap_set(uint64_t bitmap[MAX_SEGMENTS / 64], uint16_t segment_index)
{
    bitmap[segment_index >> 6] |= (1ULL << (segment_index & 63));
}

static inline bool abort_tx(region *r, txrecord *t)
{
    if (!t)
        return false;
    t->aborted = true;

    ctrl *c = t->written_head;
    while (c)
    {
        ctrl *next = c->next;
        uint64_t w_and_r = atomic_load_explicit(&c->written_in_epoch_and_readable, memory_order_acquire);
        uint64_t epoch_reset = w_and_r & 0xFFFFFFFFULL;
        epoch_reset ^= 1ULL;
        atomic_store_explicit(&c->written_in_epoch_and_readable, epoch_reset, memory_order_release);
        atomic_store_explicit(&c->owner, 0u, memory_order_relaxed);
        c->next = NULL;
        c = next;
    }
    t->written_head = NULL;
    t->written_tail = NULL;

    // alocs
    segment_node *sn = t->allocated_segments;
    while (sn)
    {
        segment_node *next = sn->next;
        free(sn->control);
        free(sn->copyA);
        free(sn->copyB);
        free(sn);
        sn = next;
    }
    t->allocated_segments = NULL;

    leave_batcher(&r->batcher, r, t->is_ro);
    if (t->free_map)
        free(t->free_map);
    free(t);
    return false;
}

static inline uint16_t get_segment_id_from_pointer(const void *ptr)
{
    return (uint16_t)((uintptr_t)ptr >> WORD_INDEX_BITS);
}

static inline uint64_t get_byte_offset_from_pointer(const void *ptr)
{
    return (uint64_t)((uintptr_t)ptr & WORD_INDEX_MASK);
}

static bool address_to_segment_and_index(region *r, txrecord *t, const void *address, segment_and_index *out)
{
    if (!r || !address || !out)
        return false;
    uint16_t segment_id = get_segment_id_from_pointer(address);

    uint64_t byte_offset = get_byte_offset_from_pointer(address);
    if ((byte_offset & (r->align - 1u)) != 0)
        return false;

    uint64_t word_index = byte_offset >> r->log2_align;

    segment_node *segment = NULL;

    // check region
    segment = r->segment_table[segment_id];

    if (!segment && t)
    {
        // check allocated segments in transaction
        for (segment_node *sn = t->allocated_segments; sn; sn = sn->next)
        {
            if (sn->id == segment_id)
            {
                segment = sn;
                break;
            }
        }
    }

    if (!segment)
    {
        return false;
    }
    if (word_index >= segment->words)
    {
        return false;
    }
    out->seg = segment;
    out->word_index = (size_t)word_index;
    return true;
}

static void add_pending_free(region *r, segment_node *sn)
{
    if (!sn || sn->id < 2 || r->segment_table[sn->id] == NULL)
        return;

    uint8_t already_pending = atomic_exchange_explicit(&sn->pending_free, true, memory_order_acq_rel);
    if (already_pending)
        return;

    pthread_mutex_lock(&r->free_lock);
    sn->next_free = r->pending_free;
    r->pending_free = sn;
    pthread_mutex_unlock(&r->free_lock);
}

static segment_node *allocate_segment(region *r, uint16_t id, size_t size)
{
    size_t align = r->align;
    size_t words = size / align;

    segment_node *sn = (segment_node *)malloc(sizeof(segment_node));
    if (unlikely(!sn))
        return NULL;
    sn->size = size;
    sn->words = words;
    sn->align = align;
    sn->id = id;

    // copies
    size_t data = align < sizeof(void *) ? sizeof(void *) : align;
    if (posix_memalign((void **)&sn->copyA, data, size) != 0)
    {
        free(sn);
        return NULL;
    }
    if (posix_memalign((void **)&sn->copyB, data, size) != 0)
    {
        free(sn->copyA);
        free(sn);
        return NULL;
    }

    // control
    size_t ctrl_align = sizeof(void *);
    if (posix_memalign((void **)&sn->control, ctrl_align, sizeof(*sn->control) * words) != 0)
    {
        free(sn->copyA);
        free(sn->copyB);
        free(sn);
        return NULL;
    }

    // zero data
    memset(sn->copyA, 0, size);
    memset(sn->copyB, 0, size);
    for (size_t i = 0; i < words; i++)
    {
        // atomic_init(&sn->control[i].readable_copy, false);
        // atomic_init(&sn->control[i].written_this_epoch, 0u);
        uint64_t w_and_r = ((uint64_t)0u << 32) | (uint64_t)0u;
        atomic_init(&sn->control[i].written_in_epoch_and_readable, w_and_r);
        atomic_init(&sn->control[i].owner, 0u);
        sn->control[i].next = NULL;
    }

    sn->next = NULL;
    sn->prev_free = sn->next_free = NULL;
    atomic_init(&sn->pending_free, false);
    return sn;
}

// -----------------------------------------------------------------------------

// helpers mais importantes ----------------------------------------------------

static bool read_word(region *r, txrecord *t, segment_node *segment, size_t word_index, void *target)
{
    ctrl *c = &segment->control[word_index];

    if (t->is_ro)
    {
        uint64_t w_and_r = atomic_load_explicit(&c->written_in_epoch_and_readable, memory_order_acquire);
        uint32_t written = (uint32_t)(w_and_r >> 32);
        bool readable = (bool)(w_and_r & 1ULL);
        if (written == t->epoch)
        {
            readable = !readable;
        }
        // read the readable copy into target
        memcpy(target, readable_ptr(segment, word_index, readable), r->align);
        return true;
    }

    uint64_t w_and_r = atomic_load_explicit(&c->written_in_epoch_and_readable, memory_order_acquire);
    uint32_t written = (uint32_t)(w_and_r >> 32);
    bool readable = (bool)(w_and_r & 1ULL);

    if (written == t->epoch /* the word has been written in the current epoch*/)
    {
        uint64_t expected = atomic_load_explicit(&c->owner, memory_order_acquire);
        uint32_t ownerEpoch = (uint32_t)(expected >> 32);
        uint32_t ownerId = (uint32_t)(expected & 0xFFFFFFFFu);

        // if transaction is not in the access set, abort
        if (!(ownerEpoch == t->epoch && ownerId == t->id))
            return false;

        // read the writable copy into target
        readable = !readable;
        memcpy(target, writable_ptr(segment, word_index, readable), r->align);
        return true;
    }

    // im now the owner (if there wasnt one already in this epoch)

    uint64_t expected = atomic_load_explicit(&c->owner, memory_order_acquire);
    if ((uint32_t)(expected >> 32) != t->epoch)
        atomic_compare_exchange_strong_explicit(&c->owner, &expected, ((uint64_t)t->epoch << 32) | t->id, memory_order_acq_rel, memory_order_acquire);

    // read the readable copy into target
    memcpy(target, readable_ptr(segment, word_index, readable), r->align);
    return true;
}

static bool write_word(region *r, txrecord *t, segment_node *segment, size_t word_index, void const *source)
{
    ctrl *c = &segment->control[word_index];

    uint64_t w_and_r = atomic_load_explicit(&c->written_in_epoch_and_readable, memory_order_acquire);
    uint32_t written = (uint32_t)(w_and_r >> 32);
    bool readable = (bool)(w_and_r & 1ULL);

    if (written == t->epoch)
    {
        uint64_t expected = atomic_load_explicit(&c->owner, memory_order_acquire);
        uint32_t ownerEpoch = (uint32_t)(expected >> 32);
        uint32_t ownerId = (uint32_t)(expected & 0xFFFFFFFFu);

        if (!(ownerId == t->id && ownerEpoch == t->epoch))
            return false;

        readable = !readable;
        memcpy(writable_ptr(segment, word_index, readable), source, r->align);
        return true;
    }

    // word hasnt been written this epoch yet
    uint64_t expected = atomic_load_explicit(&c->owner, memory_order_acquire);
    uint32_t ownerEpoch = (uint32_t)(expected >> 32);
    uint32_t ownerId = (uint32_t)(expected & 0xFFFFFFFFu);

    if (ownerEpoch == t->epoch && ownerId != t->id)
        // someone else is in the access set
        return false;

    // try to get ownership, if it doesnt work abort
    if (!atomic_compare_exchange_strong_explicit(&c->owner, &expected, ((uint64_t)t->epoch << 32) | t->id, memory_order_acq_rel, memory_order_acquire))
        // someone else got in the access set in the meantime
        return false;

    // mark that the word has been written this epoch
    readable = !readable;
    uint64_t new_w_and_r = ((uint64_t)t->epoch << 32) | (((uint32_t)w_and_r + 1u));
    atomic_store_explicit(&c->written_in_epoch_and_readable, new_w_and_r, memory_order_release);

    // write to writable copy
    memcpy(writable_ptr(segment, word_index, !readable), source, r->align);

    c->next = t->written_head;
    t->written_head = c;
    if (t->written_tail == NULL)
        t->written_tail = c;

    return true;
}

void epoch_boundary(void *ctx)
{
    region *r = (region *)ctx;

    // 2) libertar segments
    segment_node *head = r->pending_free;
    r->pending_free = NULL;
    while (head)
    {
        segment_node *sn = head;
        head = head->next_free;

        sn->next_free = NULL;

        // remove de estruturas (allocs/segment_table) e libera memória
        if (sn->prev)
            sn->prev->next = sn->next;
        if (sn->next)
            sn->next->prev = sn->prev;
        if (r->allocs == sn)
            r->allocs = sn->next;

        if (r->segment_table[sn->id])
        {
            r->segment_table[sn->id] = NULL;
            free(sn->control);
            free(sn->copyA);
            free(sn->copyB);
            free(sn);
        }
    }
}

// -----------------------------------------------------------------------------

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
 **/
shared_t tm_create(size_t unused(size), size_t unused(align))
{
    if (!is_power_of_2(align) || size == 0 || size % align != 0 || size > (1ULL << 48))
    {
        return invalid_shared;
    }

    region *region = (struct region *)malloc(sizeof(struct region));
    if (unlikely(!region))
    {
        return invalid_shared;
    }

    region->align = align;
    region->log2_align = __builtin_ctz(align);
    region->size = size;

    // initialize segment table
    region->segment_table = (segment_node **)calloc(MAX_SEGMENTS, sizeof(segment_node *));
    if (unlikely(!region->segment_table))
    {
        free(region);
        return invalid_shared;
    }
    atomic_init(&region->segment_count, 2);
    atomic_init(&region->transaction_id_counter, 1);

    // base segment
    segment_node *base = allocate_segment(region, 1, size);
    if (unlikely(!base))
    {
        free(region->segment_table);
        free(region);
        return invalid_shared;
    }
    region->segment_table[1] = base;

    // initialize batcher
    batcher_init(&region->batcher);
    pthread_mutex_init(&region->written_lock, NULL);
    pthread_mutex_init(&region->free_lock, NULL);
    region->allocs = base;
    region->pending_free = NULL;

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t shared)
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
    // batcher
    batcher_destroy(&region->batcher);
    pthread_mutex_destroy(&region->written_lock);
    pthread_mutex_destroy(&region->free_lock);
    // segment table
    free(region->segment_table);
    // region
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
 **/
void *tm_start(shared_t unused(shared))
{
    return encode_pointer(1, 0);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared)
{
    return ((struct region *)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared)
{
    return ((struct region *)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool is_ro)
{
    region *r = (region *)shared;

    txrecord *t = (txrecord *)malloc(sizeof(txrecord));
    if (unlikely(!t))
    {
        return invalid_tx;
    }

    t->is_ro = is_ro;
    t->aborted = false;
    t->free_map = NULL;

    t->allocated_segments = NULL;
    t->written_head = NULL;
    t->written_tail = NULL;

    uint64_t epoch_and_id = enter_batcher(&r->batcher, is_ro);
    t->epoch = (uint32_t)(epoch_and_id >> 32);
    t->id = (uint32_t)(epoch_and_id & 0xFFFFFFFFu);
    /*uint32_t id = atomic_fetch_add_explicit(&r->transaction_id_counter, 1, memory_order_relaxed);
    if (id == 0)
        id = atomic_fetch_add_explicit(&r->transaction_id_counter, 1, memory_order_relaxed);
    t->id = id;*/
    return (tx_t)t;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
 **/
bool tm_end(shared_t shared, tx_t tx)
{
    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    bool committed = !t->aborted;

    if (committed)
    {
        if (t->free_map)
        {
            for (uint32_t i = 0; i < MAX_SEGMENTS / 64; ++i)
            {
                uint64_t bm = t->free_map[i];
                if (!bm)
                    continue;
                const uint32_t base = i << 6;
                while (bm)
                {
                    unsigned b = (unsigned)__builtin_ctzll(bm);
                    uint16_t seg = (uint16_t)(base + b);
                    segment_node *sn = r->segment_table[seg];
                    if (sn)
                        add_pending_free(r, sn);
                    bm &= (bm - 1);
                }
            }
        }

        // alocs
        segment_node *sn = t->allocated_segments;
        while (sn)
        {
            segment_node *next = sn->next;
            r->segment_table[sn->id] = sn;
            sn->prev = NULL;
            sn->next = r->allocs;
            if (sn->next)
                sn->next->prev = sn;
            r->allocs = sn;
            sn = next;
        }
        t->allocated_segments = NULL;
    }

    leave_batcher(&r->batcher, r, t->is_ro);
    if (t->free_map)
        free(t->free_map);
    free(t);
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
bool tm_read(shared_t shared, tx_t tx, void const *source, size_t size, void *target)
{
    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);

    if (!r || size == 0 || (size % r->align) != 0)
        return abort_tx(r, t);

    segment_and_index si;
    if (!address_to_segment_and_index(r, t, source, &si))
        return abort_tx(r, t);

    size_t words = size / r->align;
    uint8_t *out = (uint8_t *)target;

    // for each word index within [source, source + size[
    for (size_t i = 0; i < words; ++i, ++si.word_index, out += r->align)
    {

        if (si.word_index >= si.seg->words)
            return abort_tx(r, t);

        if (!read_word(r, t, si.seg, si.word_index, out))
            return abort_tx(r, t);
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
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size, void *target)
{
    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);

    if (!r || size == 0 || (size % r->align) != 0)
        return abort_tx(r, t);

    segment_and_index si;
    if (!address_to_segment_and_index(r, t, target, &si))
        return abort_tx(r, t);

    size_t words = size / r->align;
    uint8_t const *in = (uint8_t const *)source;

    // for each word index within [target, target+size[
    for (size_t i = 0; i < words; ++i, ++si.word_index, in += r->align)
    {
        if (si.word_index >= si.seg->words)
            return abort_tx(r, t);

        if (!write_word(r, t, si.seg, si.word_index, in))
            return abort_tx(r, t);
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
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void **target)
{
    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    if (!r || !target)
    {
        printf("invalid parameters in tm_alloc\n");
        abort_tx(r, t);
        return abort_alloc;
    }
    if (size == 0 || (size % r->align) != 0)
    {
        printf("invalid size in tm_alloc\n");
        abort_tx(r, t);
        return abort_alloc;
    }

    uint16_t segment_id = atomic_fetch_add_explicit(&r->segment_count, 1, memory_order_acq_rel);
    if (segment_id >= MAX_SEGMENTS)
    {
        printf("no more segment IDs available in tm_alloc\n");
        return nomem_alloc;
    }

    segment_node *sn = allocate_segment(r, segment_id, size);
    if (!sn)
    {
        printf("failed to allocate new segment in tm_alloc\n");
        return nomem_alloc;
    }

    sn->prev = NULL;
    sn->next = t->allocated_segments;
    if (sn->next)
        sn->next->prev = sn;
    t->allocated_segments = sn;
    *target = encode_pointer(segment_id, 0);
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
 **/
bool tm_free(shared_t shared, tx_t tx, void *target)
{
    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    if (!r || !target || !t)
    {
        return abort_tx(r, t);
    }

    uint16_t segment_id = get_segment_id_from_pointer(target);
    if (segment_id < 2)
    { // base
        return abort_tx(r, t);
    }

    // check if its still a local aloc
    segment_node *local = NULL;
    for (segment_node *sn = t->allocated_segments; sn; sn = sn->next)
    {
        if (sn->id == segment_id)
        {
            local = sn;
            break;
        }
    }

    if (local)
    {
        if (local->prev)
            local->prev->next = local->next;
        if (local->next)
            local->next->prev = local->prev;
        if (t->allocated_segments == local)
            t->allocated_segments = local->next;

        // libertar imediatamente
        free(local->control);
        free(local->copyA);
        free(local->copyB);
        free(local);
        return true;
    }

    segment_node *s = r->segment_table[segment_id];
    if (!s)
    {
        return abort_tx(r, t);
    }
    if (!t->free_map)
    {
        t->free_map = (uint64_t *)calloc(MAX_SEGMENTS / 64, sizeof(uint64_t));
        if (unlikely(!t->free_map))
        {
            return abort_tx(r, t);
        }
    }
    bitmap_set(t->free_map, segment_id);
    return true;
}
