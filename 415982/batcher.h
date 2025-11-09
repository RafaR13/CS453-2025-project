#ifndef BATCHER_H
#define BATCHER_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef struct batcher
{
    pthread_mutex_t lock;
    pthread_cond_t cond;

    uint32_t counter;   // current epoch
    uint32_t remaining; // number of active transactions
    uint32_t waiting;   // threads waiting
} batcher;

// API do batcher
void batcher_init(batcher *bat);
void batcher_destroy(batcher *bat);
void enter_batcher(batcher *bat);
bool leave_batcher(batcher *bat, void *region);
uint32_t get_epoch(batcher *bat);

#endif // BATCHER_H
