#include "globalVersionClock.h"

void tl2_vc_init(tl2_version_clock *vc, uint64_t initial)
{
    atomic_init(&vc->value, initial);
}

uint64_t tl2_vc_increment(tl2_version_clock *vc)
{
    return atomic_fetch_add(&vc->value, 1) + 1;
}

uint64_t tl2_vc_load(const tl2_version_clock *vc)
{
    return atomic_load(&vc->value);
}
