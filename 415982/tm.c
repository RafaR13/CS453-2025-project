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

typedef struct
{
    _Atomic bool readable_copy;       // 0 if A, 1 if B
    _Atomic bool has_read_or_written; // basically the "access set"
    _Atomic bool written_this_epoch;  // true if written in this epoch
    _Atomic uint32_t txid;            // transaction ID of the writer (0 if none)
} ctrl;

typedef struct segment_node
{
    uint16_t id;

    // frees
    _Atomic bool pending_free;
    //_Atomic uint32_t freer_txid;

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

    // writes
    struct segment_node *next_write;
    _Atomic bool written_in_epoch;
    uint8_t *write_bitmap;

} segment_node;

typedef struct segment_node *segment_list;

typedef struct region
{
    // batcher stuff
    batcher batcher;
    pthread_mutex_t segments_lock;
    pthread_mutex_t written_lock;
    pthread_mutex_t free_lock;
    _Atomic uint32_t transaction_id_counter;

    // write list
    segment_node *written;

    segment_list allocs;       // list of dynamically allocated segments
    segment_list pending_free; // list of pending frees

    // tabela de segments
    segment_node **segment_table;
    uint32_t segment_count;

    size_t align; // size of a word
    uint32_t log2_align;
    size_t size; // size of the base segment
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

    // pending frees
    uint64_t free_map[MAX_SEGMENTS / 64];
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

static inline void clear_bitmap_writes(uint8_t *bitmap, size_t bytes)
{
    memset(bitmap, 0, bytes);
}

static inline void bits_set(uint8_t *bits, size_t idx)
{
    bits[idx >> 3] |= (uint8_t)(1u << (idx & 7));
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
    t->aborted = true;
    leave_batcher(&r->batcher, r);
    free(t);
    return false;
}

static void add_written_segment(region *r, segment_node *sn)
{
    if (!sn)
        return;

    uint8_t already_written = atomic_exchange_explicit(&sn->written_in_epoch, true, memory_order_acq_rel);
    if (already_written)
        return;

    pthread_mutex_lock(&r->written_lock);
    sn->next_write = r->written;
    r->written = sn;
    pthread_mutex_unlock(&r->written_lock);
}

static inline uint16_t get_segment_id_from_pointer(const void *ptr)
{
    return (uint16_t)((uintptr_t)ptr >> WORD_INDEX_BITS);
}

static inline uint64_t get_byte_offset_from_pointer(const void *ptr)
{
    return (uint64_t)((uintptr_t)ptr & WORD_INDEX_MASK);
}

static bool address_to_segment_and_index(region *r, const void *address, segment_and_index *out)
{
    if (!r || !address || !out)
        return false;
    uint16_t segment_id = get_segment_id_from_pointer(address);
    if (segment_id >= MAX_SEGMENTS)
        return false;

    uint64_t byte_offset = get_byte_offset_from_pointer(address);
    if ((byte_offset & (r->align - 1u)) != 0)
        return false;

    uint64_t word_index = byte_offset >> r->log2_align;

    segment_node *segment = r->segment_table[segment_id];
    if (!segment)
        return false;
    if (word_index >= segment->words)
        return false;

    out->seg = segment;
    out->word_index = (size_t)word_index;
    return true;
}

static void add_pending_free(region *r, segment_node *sn)
{
    if (!sn || sn->id == 0 || r->segment_table[sn->id] == NULL)
        return;

    uint8_t already_pending = atomic_exchange_explicit(&sn->pending_free, true, memory_order_acq_rel);
    if (already_pending)
        return;

    // TODO: será que dá para fazer lock free?
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
        atomic_init(&sn->control[i].readable_copy, false);
        atomic_init(&sn->control[i].has_read_or_written, false);
        atomic_init(&sn->control[i].written_this_epoch, false);
        atomic_init(&sn->control[i].txid, 0);
    }

    // bitmap
    size_t bitmap_bytes = (words + 7u) / 8u;
    sn->write_bitmap = (uint8_t *)calloc(bitmap_bytes, 1);
    if (!sn->write_bitmap)
    {
        free(sn->control);
        free(sn->copyA);
        free(sn->copyB);
        free(sn);
        return NULL;
    }

    sn->next = NULL;
    sn->prev_free = sn->next_free = NULL;
    atomic_init(&sn->pending_free, false);
    sn->next_write = NULL;
    atomic_init(&sn->written_in_epoch, false);
    // atomic_init(&sn->freer_txid, 0);
    return sn;
}
// -----------------------------------------------------------------------------

// helpers mais importantes ----------------------------------------------------

static bool read_word(region *r, txrecord *t, segment_node *segment, size_t word_index, void *target)
{
    ctrl *c = &segment->control[word_index];
    /*uint32_t pending_free_txid = atomic_load_explicit(&segment->freer_txid, memory_order_acquire);
    if (pending_free_txid != 0 && pending_free_txid != t->id)
        return false;*/

    if (t->is_ro)
    {
        // read the readable copy into target
        bool readable = atomic_load_explicit(&c->readable_copy, memory_order_acquire);
        memcpy(target, readable_ptr(segment, word_index, readable), r->align);
        // atomic_store_explicit(&c->has_read_or_written, true, memory_order_release);
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
    /*uint32_t pending_free_txid = atomic_load_explicit(&segment->freer_txid, memory_order_acquire);
    if (pending_free_txid != 0 && pending_free_txid != t->id)
        return false;*/

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
    bits_set(segment->write_bitmap, word_index);
    add_written_segment(r, segment);
    return true;
}

void epoch_boundary(void *ctx)
{
    region *r = (region *)ctx;

    // 1) Writes
    segment_node *whead;
    pthread_mutex_lock(&r->written_lock);
    whead = r->written;
    r->written = NULL;
    pthread_mutex_unlock(&r->written_lock);

    while (whead)
    {
        segment_node *seg = whead;
        whead = whead->next_write;
        seg->next_write = NULL;
        atomic_store_explicit(&seg->written_in_epoch, false, memory_order_relaxed);

        // percorre bits a 1 do dirty_bits: só essas palavras foram escritas
        size_t nbytes = (seg->words + 7) / 8;
        for (size_t byte = 0; byte < nbytes; ++byte)
        {
            uint8_t b = seg->write_bitmap[byte];
            while (b)
            {
                unsigned bit = (unsigned)__builtin_ctz(b);
                size_t idx = (byte << 3) + bit;
                if (idx < seg->words)
                {
                    ctrl *c = &seg->control[idx];
                    bool rc = atomic_load_explicit(&c->readable_copy, memory_order_relaxed);
                    atomic_store_explicit(&c->readable_copy, !rc, memory_order_relaxed);
                    atomic_store_explicit(&c->written_this_epoch, false, memory_order_relaxed);
                    atomic_store_explicit(&c->has_read_or_written, false, memory_order_relaxed);
                    atomic_store_explicit(&c->txid, 0, memory_order_relaxed);
                }
                b &= (uint8_t)(b - 1);
            }
        }
        // limpa bitmap para próximo epoch
        clear_bitmap_writes(seg->write_bitmap, nbytes);
    }

    // 2) libertar segments
    segment_node *head = r->pending_free;
    r->pending_free = NULL;
    while (head)
    {
        segment_node *sn = head;
        head = head->next_free;

        // corta encadeamento e (por segurança) limpa flag
        sn->next_free = NULL;
        // TODO: maybe unnecessary
        atomic_store_explicit(&sn->pending_free, 0, memory_order_relaxed);

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
            free(sn->write_bitmap);
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
    printf("ola cheguei ao tm_create\n");
    if (!is_power_of_2(align))
    {
        printf("align nao é power of 2\n");
        return invalid_shared;
    }
    if (size == 0 || size % align != 0)
    {
        printf("size is zero or not a multiple of align\n");
        return invalid_shared;
    }
    if (size > (1ULL << 48))
    {
        printf("size is too big\n");
        return invalid_shared;
    }

    region *region = (struct region *)malloc(sizeof(struct region));
    if (unlikely(!region))
    {
        printf("failed to allocate region\n");
        return invalid_shared;
    }

    region->align = align;
    region->log2_align = __builtin_ctz(align);
    region->size = size;

    // initialize segment table
    region->segment_table = (segment_node **)calloc(MAX_SEGMENTS, sizeof(segment_node *));
    if (!region->segment_table)
    {
        free(region);
        printf("failed to allocate segment table\n");
        return invalid_shared;
    }
    region->segment_count = 1; // base segment is segment 0
    atomic_init(&region->transaction_id_counter, 1);

    // base segment
    segment_node *base = allocate_segment(region, 1, size);
    if (!base)
    {
        free(region->segment_table);
        free(region);
        printf("failed to allocate base segment\n");
        return invalid_shared;
    }
    region->segment_table[1] = base;
    region->written = NULL;

    // initialize batcher
    batcher_init(&region->batcher);
    pthread_mutex_init(&region->segments_lock, NULL);
    pthread_mutex_init(&region->written_lock, NULL);
    pthread_mutex_init(&region->free_lock, NULL);
    region->allocs = base;
    region->pending_free = NULL;

    printf("tm_create succeeded\n");
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
 **/
void tm_destroy(shared_t shared)
{
    printf("ola cheguei ao tm_destroy\n");
    struct region *region = (struct region *)shared;
    // free all segments
    while (region->allocs)
    {
        segment_list next = region->allocs->next;
        free(region->allocs->control);
        free(region->allocs->copyA);
        free(region->allocs->copyB);
        free(region->allocs->write_bitmap);
        free(region->allocs);
        region->allocs = next;
    }
    // batcher
    batcher_destroy(&region->batcher);
    // segments_lock
    pthread_mutex_destroy(&region->segments_lock);
    pthread_mutex_destroy(&region->written_lock);
    pthread_mutex_destroy(&region->free_lock);
    // segment table
    free(region->segment_table);
    // region
    free(region);
    printf("tm_destroy succeeded\n");
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
 **/
void *tm_start(shared_t unused(shared))
{
    // return ((struct region *)shared)->start;
    printf("tm_start called e vou retornar %p\n", encode_pointer(1, 0));
    return encode_pointer(1, 0);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
 **/
size_t tm_size(shared_t shared)
{
    printf("tm_size called e vou retornar %zu\n", ((struct region *)shared)->size);
    return ((struct region *)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
 **/
size_t tm_align(shared_t shared)
{
    printf("tm_align called e vou retornar %zu\n", ((struct region *)shared)->align);
    return ((struct region *)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
 **/
tx_t tm_begin(shared_t shared, bool is_ro)
{
    // TODO: what are failure conditions ?
    // printf("tm_begin called\n");

    region *r = (region *)shared;

    txrecord *t = (txrecord *)malloc(sizeof(txrecord));
    if (unlikely(!t))
    {
        printf("failed to allocate transaction record\n");
        return invalid_tx;
    }

    t->is_ro = is_ro;
    t->aborted = false;

    clear_bitmap(t->free_map);

    // printf("about to enter batcher\n");
    enter_batcher(&r->batcher);
    // printf("entered batcher\n");
    t->epoch = get_epoch(&r->batcher);
    //  printf("got epoch %u\n", t->epoch);
    uint32_t id = atomic_fetch_add_explicit(&r->transaction_id_counter, 1, memory_order_relaxed);
    if (id == 0)
        id = atomic_fetch_add_explicit(&r->transaction_id_counter, 1, memory_order_relaxed);
    t->id = id;
    // printf("tm_begin succeeded with tx id %u\n", t->id);
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
    /*else
    {
        // se aplicar a cena de registar que o segmento esta a ser freed, tem que se resettar as variaveis aqui
    }*/

    leave_batcher(&r->batcher, r);
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
        // t->aborted = true;
        // return false;
        return abort_tx(r, t);
    }

    segment_and_index si;
    if (!address_to_segment_and_index(r, source, &si))
    {
        // t->aborted = true;
        // return false;
        return abort_tx(r, t);
    }

    size_t words = size / r->align;
    uint8_t *out = (uint8_t *)target;

    // for each word index within [source, source + size[
    for (size_t i = 0; i < words; ++i, ++si.word_index, out += r->align)
    {

        if (si.word_index >= si.seg->words)
        {
            // t->aborted = true;
            // return false;
            return abort_tx(r, t);
        }
        if (!read_word(r, t, si.seg, si.word_index, out))
        {
            // t->aborted = true;
            // return false;
            return abort_tx(r, t);
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
        // t->aborted = true;
        // return false;
        return abort_tx(r, t);
    }

    segment_and_index si;
    if (!address_to_segment_and_index(r, target, &si))
    {
        // t->aborted = true;
        // return false;
        return abort_tx(r, t);
    }

    size_t words = size / r->align;
    uint8_t const *in = (uint8_t const *)source;

    // for each word index within [target, target+size[
    for (size_t i = 0; i < words; ++i, ++si.word_index, in += r->align)
    {
        if (si.word_index >= si.seg->words)
        {
            // t->aborted = true;
            // return false;
            return abort_tx(r, t);
        }
        if (!write_word(r, t, si.seg, si.word_index, in))
        {
            // t->aborted = true;
            // return false;
            return abort_tx(r, t);
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
alloc_t tm_alloc(shared_t shared, tx_t tx, size_t size, void **target)
{
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)

    // allocate enough space for a segment of size size
    // if allocation fails, return abort_alloc

    // initialize the control structure(s) (one per word) in the segment;
    // initialize each copy of each word in the segment to zeroes;
    // register the segment in the set of allocated segments;

    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    if (!r || !target)
    {
        // t->aborted = true;

        abort_tx(r, t);
        return abort_alloc;
    }
    if (size == 0 || (size % r->align) != 0)
    {
        // t->aborted = true;
        abort_tx(r, t);
        return abort_alloc;
    }

    pthread_mutex_lock(&r->segments_lock);
    if (r->segment_count + 1 >= MAX_SEGMENTS)
    {
        pthread_mutex_unlock(&r->segments_lock);
        return nomem_alloc;
    }
    uint16_t segment_id = ++r->segment_count;
    pthread_mutex_unlock(&r->segments_lock);
    segment_node *sn = allocate_segment(r, segment_id, size);
    if (!sn)
        return nomem_alloc;
    pthread_mutex_lock(&r->segments_lock);
    r->segment_table[segment_id] = sn;
    pthread_mutex_unlock(&r->segments_lock);

    pthread_mutex_lock(&r->segments_lock);
    sn->prev = NULL;
    sn->next = r->allocs;
    if (sn->next)
        sn->next->prev = sn;
    r->allocs = sn;
    pthread_mutex_unlock(&r->segments_lock);

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
    // TODO: tm_free(shared_t, tx_t, void*)

    // mark target for deregistering (from the set of allocated segments)
    // and freeing once the last transaction of the current epoch leaves
    // the Batcher, if the calling transaction ends up being committed;

    region *r = (region *)shared;
    txrecord *t = get_transaction_record(tx);
    if (!r || !target || !t)
    {
        // t->aborted = true;
        // return false;
        return abort_tx(r, t);
    }

    uint16_t segment_id = get_segment_id_from_pointer(target);
    if (segment_id < 2)
    { // base
        return abort_tx(r, t);
    }

    segment_node *s = r->segment_table[segment_id];
    if (!s)
        return abort_tx(r, t);

    // atomic_store_explicit(&s->freer_txid, t->id, memory_order_release);

    bitmap_set(t->free_map, segment_id);
    return true;
}
