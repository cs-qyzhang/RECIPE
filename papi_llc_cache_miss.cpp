#include <iostream>
#include <vector>
#include <thread>
#include "papi_llc_cache_miss.h"

int main(void) {
  if(PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
      std::cerr << "PAPI_library_init ERROR!" << std::endl;
      return -1;
  }

  if(PAPI_thread_init(pthread_self) != PAPI_OK) {
    fprintf(stderr, "ERROR: PAPI library failed to initialize for pthread\n");
    exit(1);
  }

  CacheMissStat stat;
  stat.Start();

  std::vector<std::thread> threads;
  for (int i = 0; i < 16; ++i) {
    threads.emplace_back([](){
      CacheMissStat s;

      s.Start();
      int* array = new int[1000000];
      for (int i = 0; i < 1000000; ++i) {
        array[i] = i;
      }

      s.Stop();
      s.PrintStat();
    });
  }

  for (auto& t : threads)
    t.join();

  stat.Stop();
  stat.PrintStat();

  PAPI_shutdown();
  return 0;
}