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

// Internal headers
#include <tm.h>
// #include "batcher.h"

#include "macros.h"

#define SEGMENT_ID_BITS 16u
#define MAX_SEGMENTS (1u << SEGMENT_ID_BITS)
#define SEGMENT_ID_MASK (MAX_SEGMENTS - 1u)
#define WORD_INDEX_BITS 48u
#define WORD_INDEX_MASK ((1ULL << WORD_INDEX_BITS) - 1ULL)
#define BITSET_BYTES ((MAX_SEGMENTS + 7u) / 8u)

// structures and types --------------------------------------------------------

typedef struct
{
    _Atomic bool readable_copy;       // 0 if A, 1 if B
    _Atomic bool has_read_or_written; // basically the "access set"
    _Atomic bool written_this_epoch;  // true if written in this epoch
    _Atomic uint32_t txid;            // transaction ID of the writer (0 if none)
} ctrl;

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
    uint16_t id;

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

typedef struct
{
    uint16_t segment_id;
    size_t word_index;
} write_list_item;

typedef struct region
{
    // batcher stuff
    batcher batcher;

    // write list stuff (global por agora -- so para o segmento base)
    _Atomic size_t wl_size;      // nº de entradas validas
    size_t wl_capacity;          // capacidade do array
    write_list_item *write_list; // array of word indices that were written in this epoch

    segment_node base; // base segment (non-freeable)

    segment_list allocs; // list of dynamically allocated segments
    segment_list pending_free;

    // tabela de segments
    segment_node **segment_table;
    uint32_t segment_count;

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

    // segmentos para libertar no fim da epoch
    uint8_t *free_map;
    uint16_t *segments_to_free;
    size_t segments_to_free_count;
    size_t segments_to_free_capacity;
} txrecord;

typedef struct
{
    segment_node *seg;
    size_t word_index;
} segment_and_index;

// -----------------------------------------------------------------------------

// helper functions ------------------------------------------------------------

static txrecord *get_transaction_record(tx_t tx) { return (txrecord *)(uintptr_t)tx; }

static inline uint8_t *readable_ptr(segment_node *seg, size_t index, bool readable_copy)
{
    return readable_copy ? (seg->copyB + index * seg->align) : (seg->copyA + index * seg->align);
}

static inline uint8_t *writable_ptr(segment_node *seg, size_t index, bool readable_copy)
{
    return readable_copy ? (seg->copyA + index * seg->align) : (seg->copyB + index * seg->align);
}

static void wl_append(region *reg, uint16_t segment_id, size_t word_index)
{
    size_t position = atomic_fetch_add_explicit(&reg->wl_size, 1, memory_order_acq_rel);
    if (position < reg->wl_capacity)
    {
        reg->write_list[position].segment_id = segment_id;
        reg->write_list[position].word_index = word_index;
    }
    // TODO: a write list pode dar overflow ?
}

static bool read_word(region *r, txrecord *t, segment_node *segment, size_t word_index, void *target)
{
    ctrl *c = &segment->control[word_index];
    if (t->is_ro)
    {
        // read the readable copy into target
        bool readable = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
        memcpy(target, readable_ptr(segment, word_index, readable), r->align);
        return true;
    }

    bool written = atomic_load_explicit(&c->written_this_epoch, memory_order_acquire);

    if (written /* the word has been written in the current epoch*/)
    {
        uint32_t owner = atomic_load_explicit(&c->txid, memory_order_acquire);
        if (owner != t->id /* this transaction is already in the "access set" */)
            return false;

        // read the writable copy into target
        bool readable = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
        memcpy(target, writable_ptr(segment, word_index, readable), r->align);
        return true;
    }

    /*if (owner != t->id) {
        uint32_t expected = 0;
        if (!atomic_compare_exchange_strong_explicit(&c->txid, &expected, t->id,
                                                     memory_order_acq_rel, memory_order_acquire)) {
            // Another tx has already claimed this word (reader/writer) → abort
            if (expected != t->id) return false;
        }
    }*/

    // add the transaction to the "access set" if not there already
    atomic_store_explicit(&c->has_read_or_written, true, memory_order_release);
    // read the readable copy into target
    bool rc = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
    memcpy(target, readable_ptr(segment, word_index, rc), r->align);
    return true;
}

static bool write_word(region *r, txrecord *t, segment_node *segment, size_t word_index, void const *source)
{
    ctrl *c = &segment->control[word_index];

    bool written = atomic_load_explicit(&c->written_this_epoch, memory_order_acquire);
    if (written)
    {
        uint32_t owner = atomic_load_explicit(&c->txid, memory_order_acquire);
        if (owner != t->id)
            return false;

        bool readable = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
        memcpy(writable_ptr(segment, word_index, readable), source, r->align);
        return true;
    }

    // word hasnt been written this epoch yet
    uint32_t owner = atomic_load_explicit(&c->txid, memory_order_acquire);
    if (owner != 0 && owner != t->id)
        return false;

    if (owner == 0)
    {
        uint32_t expected = 0;
        if (!atomic_compare_exchange_strong_explicit(&c->txid, &expected, t->id,
                                                     memory_order_acq_rel, memory_order_acquire))
        {
            // some transaction has already claimed this word
            if (expected != t->id)
                return false;
        }
    }

    atomic_store_explicit(&c->has_read_or_written, true, memory_order_release);
    atomic_store_explicit(&c->written_this_epoch, true, memory_order_release);

    bool readable = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
    memcpy(writable_ptr(segment, word_index, readable), source, r->align);
    wl_append(r, segment->id, word_index);
    return true;
}

static inline int is_power_of_2(size_t x) { return x && ((x & (x - 1)) == 0); }

static inline void *encode_pointer(uint16_t segment_id, uint64_t word_index)
{
    return (void *)(((uint64_t)segment_id << WORD_INDEX_BITS) | (word_index & WORD_INDEX_MASK));
}

static inline uint16_t get_segment_id_from_pointer(const void *ptr)
{
    return (uint16_t)((uintptr_t)ptr >> WORD_INDEX_BITS);
}

static inline uint64_t get_word_index_from_pointer(const void *ptr)
{
    return (uint64_t)((uintptr_t)ptr & WORD_INDEX_MASK);
}

static bool address_to_segment_and_index(region *r, const void *address, segment_and_index *out)
{
    if (!r || !address || !out)
        return false;
    uint16_t segment_id = get_segment_id_from_pointer(address);
    uint64_t word_index = get_word_index_from_pointer(address);

    segment_node *segment = r->segment_table[segment_id];
    if (!segment)
        return false;
    if (word_index >= segment->words)
        return false;

    out->seg = segment;
    out->word_index = (size_t)word_index;
    return true;
}

static bool bit_test_and_set(uint8_t *bitmap, uint16_t index)
{
    uint8_t *byte = &bitmap[index >> 3];
    uint8_t mask = (uint8_t)(1u << (index & 7));
    bool was = (*byte & mask) != 0;
    *byte = (uint8_t)(*byte | mask);
    return was;
}

static bool push_segment_to_free(txrecord *t, uint16_t segment_id)
{
    if (!t) // TODO: acho que isto é sempre falso mas yh
        return false;
    // TODO: se ja estava marcado por outra transacao, aborta ou aceita ?
    if (bit_test_and_set(t->free_map, segment_id))
        return true;

    // se ja estiver cheio, temos que duplicar cap
    if (t->segments_to_free_count == t->segments_to_free_capacity)
    {
        size_t new_cap = t->segments_to_free_capacity * 2;
        void *new_array = realloc(t->segments_to_free, new_cap * sizeof(uint16_t));
        if (!new_array)
            return false;
        t->segments_to_free = new_array;
        t->segments_to_free_capacity = new_cap;
    }
    t->segments_to_free[t->segments_to_free_count++] = segment_id;
    return true;
}

static inline bool test_bits(const uint8_t *bitmap, uint16_t index)
{
    // true if bit at index "index" is 1
    return (bitmap[index >> 3] >> (index & 7)) & 1u;
}

void epoch_boundary(void *ctx)
{
    region *r = (region *)ctx;

    // 1) Publicar writes deste epoch
    size_t n = atomic_exchange_explicit(&r->wl_size, 0, memory_order_acq_rel);
    for (size_t i = 0; i < n && i < r->wl_capacity; ++i)
    {
        uint16_t segid = r->write_list[i].segment_id;
        size_t idx = r->write_list[i].word_index;

        // pode já não estar na segment_table (se foi deregistered no commit)
        segment_node *seg = r->segment_table[segid];
        if (!seg)
        {
            // tenta encontrá-lo na pending_free: ainda existe até aqui
            for (segment_node *p = r->pending_free; p; p = p->next)
            {
                if (p->id == segid)
                {
                    seg = p;
                    break;
                }
            }
            if (!seg)
                continue;
        }

        ctrl *c = &seg->control[idx];
        bool rc = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
        atomic_store_explicit(&c->readable_copy, !rc, memory_order_release);
        atomic_store_explicit(&c->written_this_epoch, false, memory_order_release);
        atomic_store_explicit(&c->has_read_or_written, false, memory_order_release);
        atomic_store_explicit(&c->txid, 0, memory_order_release);
    }

    // 2) Libertar segmentos pendentes (deregistered no commit)
    segment_node *sn = r->pending_free;
    r->pending_free = NULL;
    while (sn)
    {
        segment_node *next = sn->next;
        // remove de r->allocs (se manténs esta lista)
        if (sn->prev)
            sn->prev->next = sn->next;
        if (sn->next)
            sn->next->prev = sn->prev;
        if (r->allocs == sn)
            r->allocs = sn->next;

        free(sn->control);
        free(sn->copyA);
        free(sn->copyB);
        free(sn);
        sn = next;
    }
}

// -----------------------------------------------------------------------------
uint32_t get_epoch(batcher *bat)
{
    // can only be called by a thread after it entered the batcher, and before it left
    pthread_mutex_lock(&bat->lock);
    uint32_t epoch = bat->counter;
    pthread_mutex_unlock(&bat->lock);
    return epoch;
}

void enter_batcher(batcher *bat)
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

void leave_batcher(batcher *bat)
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

bool leave_batcher2(batcher *bat, void *region)
{
    pthread_mutex_lock(&bat->lock);

    bat->remaining--;
    if (bat->remaining > 0)
    {
        pthread_mutex_unlock(&bat->lock);
        return false;
    }

    // im the last
    bat->counter++;
    bat->remaining = bat->waiting;
    bat->waiting = 0;

    if (region)
        epoch_boundary(region);

    pthread_cond_broadcast(&bat->cond);
    pthread_mutex_unlock(&bat->lock);
    return true;
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
        return invalid_shared;

    region->align = align;
    region->size = size;

    // initialize segment table
    region->segment_table = (segment_node **)calloc(MAX_SEGMENTS, sizeof(segment_node *));
    if (!region->segment_table)
    {
        free(region);
        return invalid_shared;
    }
    region->segment_count = 0; // base segment is segment 0
    region->segment_table[0] = &region->base;

    // initialize base segment
    segment_node *base = &region->base;
    base->id = 0;
    base->align = align;
    base->size = size;
    base->words = size / align;

    // allocate the 2 copies and control struct
    if (posix_memalign((void **)&(base->copyA), align, size) != 0)
    {
        free(region->segment_table);
        free(region);
        return invalid_shared;
    }
    if (posix_memalign((void **)&(base->copyB), align, size) != 0)
    {
        free(base->copyA);
        free(region->segment_table);
        free(region);
        return invalid_shared;
    }
    if (posix_memalign((void **)&(base->control), sizeof(void *), sizeof(ctrl) * base->words) != 0)
    {
        free(base->copyA);
        free(base->copyB);
        free(region->segment_table);
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
    region->write_list = (write_list_item *)malloc(region->wl_capacity * sizeof(*region->write_list));
    if (!region->write_list)
    {
        free(base->control);
        free(base->copyA);
        free(base->copyB);
        free(region->segment_table);
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
    // return ((struct region *)shared)->start;
    return encode_pointer(0, 0);
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
    t->segments_to_free_capacity = 16; // TODO: maybe bigger ?
    t->segments_to_free_count = 0;
    t->segments_to_free = malloc(t->segments_to_free_capacity * sizeof(uint16_t));
    t->free_map = calloc(BITSET_BYTES, 1);
    if (!t->segments_to_free || !t->free_map)
    {
        free(t->segments_to_free);
        free(t->free_map);
        free(t);
        return invalid_tx;
    }

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

    if (committed && t->segments_to_free_count > 0)
    {
        pthread_mutex_lock(&r->batcher.lock);
        for (size_t i = 0; i < t->segments_to_free_count; i++)
        {
            uint16_t segment_id = t->segments_to_free[i];
            segment_node *s = r->segment_table[segment_id];
            if (!s)
                continue;

            // r->segment_table[segment_id] = NULL;

            s->prev = NULL;
            s->next = r->pending_free;
            if (s->next)
                s->next->prev = s;
            r->pending_free = s;
        }
        pthread_mutex_unlock(&r->batcher.lock);
    }

    // leave_batcher(&r->batcher);
    leave_batcher2(&r->batcher, r);
    free(t->segments_to_free);
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
    // TODO (checks): size must be positive multiple of alignment
    // source and target must be at least size bytes long
    // source and target addresses must be a positive multiple of alignment

    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);

    if (!r || size == 0 || (size % r->align) != 0)
    {
        t->aborted = true;
        return false;
    }

    segment_and_index si;
    if (!address_to_segment_and_index(r, source, &si))
    {
        t->aborted = true;
        return false;
    }

    size_t words = size / r->align;
    uint8_t *out = (uint8_t *)target;

    // for each word index within [source, source + size[
    for (size_t i = 0; i < words; ++i, ++si.word_index, out += r->align)
    {

        if (si.word_index >= si.seg->words)
        {
            t->aborted = true;
            return false;
        }
        if (!read_word(r, t, si.seg, si.word_index, out))
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
bool tm_write(shared_t shared, tx_t tx, void const *source, size_t size, void *target)
{
    // TODO (checks): size must be positive multiple of alignment
    // source and target must be at least size bytes long
    // source and target addresses must be a positive multiple of alignment

    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);

    if (!r || size == 0 || (size % r->align) != 0)
    {
        t->aborted = true;
        return false;
    }

    segment_and_index si;
    if (!address_to_segment_and_index(r, target, &si))
    {
        t->aborted = true;
        return false;
    }

    size_t words = size / r->align;
    uint8_t const *in = (uint8_t const *)source;

    // for each word index within [target, target+size[
    for (size_t i = 0; i < words; ++i, ++si.word_index, in += r->align)
    {
        if (si.word_index >= si.seg->words)
        {
            t->aborted = true;
            return false;
        }
        if (!write_word(r, t, si.seg, si.word_index, in))
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

    if (r->segment_count + 1 >= MAX_SEGMENTS)
    {
        free(sn);
        return nomem_alloc; // sem IDs livres
    }
    uint16_t segment_id = ++r->segment_count;
    r->segment_table[segment_id] = sn;
    sn->id = segment_id;

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

    *target = encode_pointer(segment_id, 0);

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
    txrecord *t = get_transaction_record(tx);
    if (!r || !target || !t)
        return false;

    uint16_t segment_id = get_segment_id_from_pointer(target);
    if (segment_id == 0) // base
        return false;

    segment_node *s = r->segment_table[segment_id];
    if (!s)
        return false;

    return push_segment_to_free(t, segment_id);
}

bool commit()
{
    // for each written word index:
    //      defer the swap, of which copy for word index is the “valid”
    //      copy, just after the last transaction from the current epoch
    //      leaves the Batcher and before the next batch starts running;
    return true;
}
