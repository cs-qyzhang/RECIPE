#include <iostream>
#include <papi.h>

const int event_list[] = {
  PAPI_LD_INS,       // Load instructions
  PAPI_SR_INS,       // Store instructions
  PAPI_L3_TCA,       // L3 total cache access
  PAPI_L3_TCM,       // L3 total cache misses
};
const int NUM_EVENTS = sizeof(event_list) / sizeof(int);

#define ERROR_RETURN(retval) { std::cerr << "Error " << retval << ": " << __FILE__ << ":line " << __LINE__ << std::endl;  exit(retval); }

#ifdef STAT_PAPI

#define llc_stat_start() CacheMissStat cache_stat;\
                         cache_stat.Start();
#define llc_stat_stop(i) \
                    cache_stat.Stop();\
                    load_count[i] = cache_stat.GetLoadCount();\
                    store_count[i] = cache_stat.GetStoreCount();\
                    llc_access[i] = cache_stat.GetL3AccessCount();\
                    llc_miss[i] = cache_stat.GetL3MissCount();

class CacheMissStat {
 public:
  CacheMissStat() : event_set_(PAPI_NULL) {
    int retval;

    /* Creating the eventset */
    if ( (retval = PAPI_create_eventset(&event_set_)) != PAPI_OK)
      ERROR_RETURN(retval);

    // If any of the required events do not exist we just exit
    for(int i = 0; i < NUM_EVENTS; i++) {
      if(PAPI_query_event(event_list[i]) != PAPI_OK) {
        std::cerr << "ERROR: PAPI event " << i << "ed is not supported" << std::endl;
        exit(1);
      }
    }

    for (int i = 0; i < NUM_EVENTS; ++i) {
      if ( (retval = PAPI_add_event(event_set_, event_list[i])) != PAPI_OK)
        ERROR_RETURN(retval);
    }

    /* get the number of events in the event set */
    int number = 0;
    if ( (retval = PAPI_list_events(event_set_, NULL, &number)) != PAPI_OK)
      ERROR_RETURN(retval);

    if (number != NUM_EVENTS) {
      fprintf(stderr, "There are %d events in the event set\n", number);
      exit(-1);
    }

    for (int i = 0; i < NUM_EVENTS; ++i)
      values_[i] = 0;
  }

  void Start() {
    int retval;
    if ( (retval = PAPI_start(event_set_)) != PAPI_OK)
      ERROR_RETURN(retval);
  }

  void Stop() {
    int retval;
    if ( (retval = PAPI_stop(event_set_, values_)) != PAPI_OK)
      ERROR_RETURN(retval);
  }

  long long GetLoadCount() { return values_[0]; }
  long long GetStoreCount() { return values_[1]; }
  long long GetL3AccessCount() { return values_[2]; }
  long long GetL3MissCount() { return values_[3]; }

  void PrintStat() {
    std::cout << "Load Instructions:  " << GetLoadCount() << std::endl;
    std::cout << "Store Instructions: " << GetStoreCount() << std::endl;
    std::cout << "L3 Cache Access:    " << GetL3AccessCount() << std::endl;
    std::cout << "L3 Cache Misses:    " << GetL3MissCount() << std::endl;
    std::cout << "L3 Cache Miss Rate: " << (double)GetL3MissCount() / (double)GetL3AccessCount() * 100.0 << "%" << std::endl;
  }

 private:
  int event_set_;
  long long values_[NUM_EVENTS];
};

#else
#endif