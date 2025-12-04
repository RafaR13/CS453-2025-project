// batcher.c
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "batcher.h"

extern void epoch_boundary(void *region);

uint32_t get_epoch(batcher *bat)
{
    pthread_mutex_lock(&bat->lock);
    uint32_t epoch = bat->counter;
    pthread_mutex_unlock(&bat->lock);
    return epoch;
}

uint64_t enter_batcher(batcher *bat, bool is_ro)
{
    pthread_mutex_lock(&bat->lock);

    uint32_t id = is_ro ? 0u : bat->tx_id_counter++;
    uint32_t epoch = bat->counter;

    if (bat->remaining == 0 ||
        bat->started_rw_txs == 0 ||
        (/*is_ro && */ bat->completed_rw_txs == 0))
    {
        bat->remaining++;
        if (!is_ro)
            bat->started_rw_txs++;
        pthread_mutex_unlock(&bat->lock);
        return ((uint64_t)id << 32) | epoch;
    }

    bat->waiting++;
    uint32_t my_epoch = epoch;

    do
    {
        pthread_cond_wait(&bat->cond, &bat->lock);
    } while (my_epoch == bat->counter);

    // we are now in a new epoch
    epoch = bat->counter;

    if (!is_ro)
        bat->started_rw_txs++;

    pthread_mutex_unlock(&bat->lock);
    return ((uint64_t)id << 32) | epoch;
}

void leave_batcher(batcher *bat, void *region, bool is_ro)
{
    pthread_mutex_lock(&bat->lock);

    bat->remaining--;
    if (!is_ro)
        bat->completed_rw_txs++;

    if (bat->remaining > 0 || bat->started_rw_txs == 0 || bat->completed_rw_txs == 0 || bat->completed_rw_txs < bat->started_rw_txs)
    {
        pthread_mutex_unlock(&bat->lock);
        return;
    }

    bat->counter++;
    bat->remaining = bat->waiting;
    bat->waiting = 0;
    bat->started_rw_txs = 0;
    bat->completed_rw_txs = 0;

    if (region)
        epoch_boundary(region);

    pthread_cond_broadcast(&bat->cond);
    pthread_mutex_unlock(&bat->lock);
}

void batcher_init(batcher *bat)
{
    pthread_mutex_init(&bat->lock, NULL);
    pthread_cond_init(&bat->cond, NULL);
    bat->counter = 1;
    bat->tx_id_counter = 1;
    bat->remaining = 0;
    bat->waiting = 0;
    bat->started_rw_txs = 0;
    bat->completed_rw_txs = 0;
}

void batcher_destroy(batcher *bat)
{
    pthread_mutex_destroy(&bat->lock);
    pthread_cond_destroy(&bat->cond);
}
