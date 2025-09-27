#include "globalVersionClock.h"

void tl2_vc_init(tl2_version_clock *vc, uint64_t initial)
{
    atomic_init(&vc->value, initial);
}

uint64_t tl2_vc_increment(tl2_version_clock *vc)
{
    // seq_cst is the simple/safe default for TL2's global clock
    uint64_t old = atomic_fetch_add_explicit(&vc->value, 1, memory_order_seq_cst);
    return old + 1;
}

uint64_t tl2_vc_load(const tl2_version_clock *vc)
{
    // acquire is enough for the read-version at tx begin
    return atomic_load_explicit(&vc->value, memory_order_acquire);
}
