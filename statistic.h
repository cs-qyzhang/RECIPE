#include <mutex>
#include <chrono>

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
std::mutex stat_latency_lock;
std::vector<double> latency;
class StatLatency {
  public:
    StatLatency(size_t record_interval = 1000)
        : record_interval_(record_interval), cnt_(0) {}

    inline __attribute__((always_inline)) void start() {
        if (cnt_++ == 0) {
            start_time_ = std::chrono::high_resolution_clock::now();
        }
    }

    inline __attribute__((always_inline)) void stop() {
        if (cnt_ == record_interval_) {
            auto stop_time = std::chrono::high_resolution_clock::now();
            cnt_ = 0;
            double lat = std::chrono::duration_cast<std::chrono::microseconds>(
                            stop_time-start_time_).count();
            latency_.push_back(lat);
            stat_latency_lock.lock();
            latency.push_back(lat);
            stat_latency_lock.unlock();
        }
    }

  private:
    std::vector<double> latency_;
    size_t record_interval_;
    size_t cnt_;
    std::chrono::high_resolution_clock::time_point start_time_;
};
#else
class StatLatency {
  public:
    StatLatency(size_t record_interval = 1000);

    inline __attribute__((always_inline)) void start() {}

    inline __attribute__((always_inline)) void stop() {}
};
#endif
