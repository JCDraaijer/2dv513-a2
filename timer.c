//
// Created by jona on 2019-11-27.
//

#include "timer.h"

void starttimer(timekeeper_t *timer) {
    clock_gettime(CLOCK_MONOTONIC_RAW, &timer->start);
}

void stoptimer(timekeeper_t *timer) {
    clock_gettime(CLOCK_MONOTONIC_RAW, &timer->stop);
    uint64_t totalNanosDiff =
            ((timer->stop.tv_sec - timer->start.tv_sec) * 1000000000) + (timer->stop.tv_nsec - timer->start.tv_nsec);
    timer->seconds = totalNanosDiff / 1000000000;
    timer->nanos = (totalNanosDiff % 1000000000);
}