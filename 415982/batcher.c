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

    if (bat->remaining == 0)
    {
        bat->remaining = 1;
        uint32_t epoch = bat->counter;
        uint32_t tx_id = bat->tx_counter++;
        pthread_mutex_unlock(&bat->lock);
        return ((uint64_t)epoch << 32) | tx_id;
    }

    if (!bat->completed_rw_txs)
    {
        bat->remaining++;
        uint32_t epoch = bat->counter;
        uint32_t tx_id = bat->tx_counter++;
        pthread_mutex_unlock(&bat->lock);
        return ((uint64_t)epoch << 32) | tx_id;
    }

    bat->waiting++;
    uint32_t my_epoch = bat->counter;
    do
    {
        pthread_cond_wait(&bat->cond, &bat->lock);
    } while (my_epoch == bat->counter);
    uint32_t epoch = bat->counter;
    uint32_t tx_id = bat->tx_counter++;
    pthread_mutex_unlock(&bat->lock);
    return ((uint64_t)epoch << 32) | tx_id;
}

void leave_batcher(batcher *bat, void *region, bool is_ro)
{
    pthread_mutex_lock(&bat->lock);

    bat->remaining--;
    if (!is_ro)
    {
        bat->completed_rw_txs = true;
    }

    if (bat->remaining > 0)
    {
        pthread_mutex_unlock(&bat->lock);
        return;
    }

    if (!bat->completed_rw_txs)
    {
        pthread_mutex_unlock(&bat->lock);
        return;
    }

    // último da epoch
    bat->counter++;
    bat->remaining = bat->waiting;
    bat->waiting = 0;
    bat->completed_rw_txs = false;
    // bat->started_rw_txs = false;

    // epoch_boundary(region) continua definido em tm.c
    if (region)
        epoch_boundary(region);

    pthread_cond_broadcast(&bat->cond);
    pthread_mutex_unlock(&bat->lock);
    return;
}

void batcher_init(batcher *bat)
{
    pthread_mutex_init(&bat->lock, NULL);
    pthread_cond_init(&bat->cond, NULL);
    bat->counter = 1;
    bat->remaining = 0;
    bat->waiting = 0;
    bat->tx_counter = 1;
    bat->completed_rw_txs = false;
}

void batcher_destroy(batcher *bat)
{
    pthread_mutex_destroy(&bat->lock);
    pthread_cond_destroy(&bat->cond);
}
