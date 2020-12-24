#include <iostream>
#include "perf_event.h"

int main(void) {
  PerfEvent perf;
  perf.Start();

  int* array = new int[1000000];
  for (int i = 0; i < 1000000; ++i) {
    array[i] = i;
  }

  perf.Stop();

  std::cout << perf.ReadAccess() << std::endl;
  std::cout << perf.ReadMiss() << std::endl;
  std::cout << perf.WriteAccess() << std::endl;
  std::cout << perf.WriteMiss() << std::endl;
}