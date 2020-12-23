#include <iostream>
#include <thread>
#include "papi_llc_cache_miss.h"

int main(void) {
  if(PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
      std::cerr << "PAPI_library_init ERROR!" << std::endl;
      return -1;
  }

  CacheMissStat stat;
  stat.Start();

  std::thread t([](){
    CacheMissStat s;

    s.Start();
    int* array = new int[1000000];
    for (int i = 0; i < 1000000; ++i) {
      array[i] = i;
    }

    s.Stop();
    s.PrintStat();
  });
  t.join();

  stat.Stop();
  stat.PrintStat();

  PAPI_shutdown();
  return 0;
}