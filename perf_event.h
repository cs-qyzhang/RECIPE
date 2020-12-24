#pragma once

#include <stdlib.h>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>
#include <linux/perf_event.h>
#include <asm/unistd.h>

static long perf_event_open(struct perf_event_attr *hw_event, pid_t pid,
                            int cpu, int group_fd, unsigned long flags) {
  return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

void init_perf_event_attr(struct perf_event_attr& attr, perf_hw_cache_id id,
                          perf_hw_cache_op_id op_id, perf_hw_cache_op_result_id result_id) {
  memset(&attr, 0, sizeof(struct perf_event_attr));
  attr.type = PERF_TYPE_HW_CACHE;
  attr.size = sizeof(struct perf_event_attr);
  attr.config = (id) | (op_id << 8) | (result_id << 16);
  attr.disabled = 1;
  attr.exclude_kernel = 1;
  attr.exclude_hv = 1;
}

struct PerfEvent {
  // 0: read access
  // 1: read miss
  // 2: write access
  // 3: write miss
  struct perf_event_attr attr[4];
  int fd[4];
  long long count[4];

  PerfEvent() {
    init_perf_event_attr(attr[0], PERF_COUNT_HW_CACHE_LL, PERF_COUNT_HW_CACHE_OP_READ, PERF_COUNT_HW_CACHE_RESULT_ACCESS);
    init_perf_event_attr(attr[1], PERF_COUNT_HW_CACHE_LL, PERF_COUNT_HW_CACHE_OP_READ, PERF_COUNT_HW_CACHE_RESULT_MISS);
    init_perf_event_attr(attr[2], PERF_COUNT_HW_CACHE_LL, PERF_COUNT_HW_CACHE_OP_WRITE, PERF_COUNT_HW_CACHE_RESULT_ACCESS);
    init_perf_event_attr(attr[3], PERF_COUNT_HW_CACHE_LL, PERF_COUNT_HW_CACHE_OP_WRITE, PERF_COUNT_HW_CACHE_RESULT_MISS);
    for (int i = 0; i < 4; ++i) {
      fd[i] = perf_event_open(&attr[i], 0, -1, -1, 0);
      if (fd[i] == -1) {
        std::cerr << "Error open perf event " << i << "!" << std::endl;
        exit(-1);
      }
      count[i] = -1;
    }
  }

  void Start() {
    for (int i = 0; i < 4; ++i) {
      ioctl(fd[i], PERF_EVENT_IOC_RESET, 0);
      ioctl(fd[i], PERF_EVENT_IOC_ENABLE, 0);
    }
  }

  void Stop() {
    for (int i = 0; i < 4; ++i) {
      ioctl(fd[i], PERF_EVENT_IOC_DISABLE, 0);
      read(fd[i], &count[i], sizeof(count[i]));
    }
  }

  long long ReadAccess()  { return count[0]; }
  long long ReadMiss()    { return count[1]; }
  long long WriteAccess() { return count[2]; }
  long long WriteMiss()   { return count[3]; }
};