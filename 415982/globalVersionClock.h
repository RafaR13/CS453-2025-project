#ifndef GLOBAL_VERSION_CLOCK_H
#define GLOBAL_VERSION_CLOCK_H

#include <stdatomic.h>
#include <stdint.h>

typedef struct
{
    atomic_uint_least64_t value;
} tl2_version_clock;

void tl2_vc_init(tl2_version_clock *vc, uint64_t initial);
uint64_t tl2_vc_increment(tl2_version_clock *vc);
uint64_t tl2_vc_load(const tl2_version_clock *vc);

#endif
