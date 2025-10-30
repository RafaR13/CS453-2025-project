#include <pthread.h>
#include <stdint.h>

typedef struct
{
    pthread_mutex_t lock;
    pthread_cond_t cond;

    uint32_t counter;   // current epoch
    uint32_t remaining; // number of active transactions
    uint32_t waiting;   // threads waiting
} batcher;

void batcher_init(batcher *b);
void batcher_destroy(batcher *b);
void enter_batcher(batcher *b);
void leave_batcher(batcher *b);
bool leave_batcher2(batcher *bat, void *region);
uint32_t get_epoch(batcher *b);