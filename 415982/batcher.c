#include <pthread.h>
#include <stdint.h>
#include "batcher.h"

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
