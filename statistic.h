#include <mutex>
#include <iomanip>
#include <chrono>
#include <sys/time.h>

#define STAT_SPACE_USAGE
// #undef STAT_LATENCY

#ifdef STAT_SPACE_USAGE
void space_usage(std::string name) {
    int pid = getpid();
    char cmd_buf[100];
    sprintf(cmd_buf, "pmap %d > ./usage-%s.txt", pid, name.c_str());
    system(cmd_buf);
}
#else
void space_usage(std::string name) {}
#endif

#ifdef STAT_LATENCY
#define LATENCY_INTERVAL 10000

uint64_t GetMilliseconds() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

#define stat_latency_start()  uint64_t begin_millisec = GetMilliseconds()
#define stat_latency_stop(i) rec_latency[i].first = begin_millisec;\
                                            rec_latency[i].second = (GetMilliseconds()-begin_millisec)

void OutputLatency(std::vector<std::pair<uint64_t,uint64_t>> &latency) {
  std::sort(latency.begin(), latency.end());
  std::cout << latency.size() << std::endl;
  std::ofstream out("latency.txt");
  if (!out.good()) {
    std::cout << "can't open latency.txt!" << std::endl;
  } else {
    uint64_t sum = 0;
    for (uint64_t i = 0; i < latency.size(); ++i) {
      sum += latency[i].second;
      if (((i+1) % LATENCY_INTERVAL) == 0) {
        out << (double)sum / (double)LATENCY_INTERVAL << std::endl;
        sum = 0;
      }
    }
  }
}
#else // STAT_LATENCY
#define stat_latency_start()
#define stat_latency_stop(i)
#endif

// return human readable string of size
std::string human_readable(double size) {
  static const std::string suffix[] = {
    "B",
    "KB",
    "MB",
    "GB"
  };
  const int arr_len = 4;

  std::ostringstream out;
  out.precision(2);
  for (int divs = 0; divs < arr_len; ++divs) {
    if (size >= 1024.0) {
      size /= 1024.0;
    } else {
      out << std::fixed << size;
      return out.str() + suffix[divs];
    }
  }
  out << std::fixed << size;
  return out.str() + suffix[arr_len - 1];
}
