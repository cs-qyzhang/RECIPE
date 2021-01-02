#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <sys/time.h>

using namespace std;

// #define STAT_LATENCY
// #define STAT_PAPI
// #define STAT_PERF
// #define STAT_SPACE_USAGE

#include "P-ART/Tree.h"
#include "third-party/FAST_FAIR/btree.h"
#include "third-party/CCEH/src/Level_hashing.h"
#include "third-party/CCEH/src/CCEH.h"
#include "third-party/WOART/woart.h"
#include "masstree.h"
#include "P-BwTree/src/bwtree.h"
#include "clht.h"
#include "ssmem.h"
#include "combotree/combotree.h"
#include "statistic.h"
#include "papi_llc_cache_miss.h"
#include "perf_event.h"

#ifdef HOT
#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>
#endif

using namespace wangziqi2013::bwtree;

// index types
enum {
    TYPE_ART,
    TYPE_HOT,
    TYPE_BWTREE,
    TYPE_MASSTREE,
    TYPE_CLHT,
    TYPE_FASTFAIR,
    TYPE_LEVELHASH,
    TYPE_CCEH,
    TYPE_WOART,
    TYPE_COMBOTREE,
};

enum {
    OP_INSERT,
    OP_UPDATE,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_D,
    WORKLOAD_E,
    WORKLOAD_F,
    WORKLOAD_LOAD,
};

namespace Dummy {
    inline void mfence() {asm volatile("mfence":::"memory");}

    inline void clflush(char *data, int len, bool front, bool back)
    {
        if (front)
            mfence();
        volatile char *ptr = (char *)((unsigned long)data & ~(64 - 1));
        for (; ptr < data+len; ptr += 64){
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
        }
        if (back)
            mfence();
    }
}


////////////////////////Helper functions for P-BwTree/////////////////////////////
/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 < k2;
  }

  inline bool operator()(const uint64_t k1, const uint64_t k2) const {
      return k1 < k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      return memcmp(k1, k2, strlen(k1) > strlen(k2) ? strlen(k1) : strlen(k2)) < 0;
  }

  KeyComparator(int dummy) {
    (void)dummy;

    return;
  }

  KeyComparator() = delete;
  //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
 public:
  inline bool operator()(const long int k1, const long int k2) const {
    return k1 == k2;
  }

  inline bool operator()(uint64_t k1, uint64_t k2) const {
      return k1 == k2;
  }

  inline bool operator()(const char *k1, const char *k2) const {
      if (strlen(k1) != strlen(k2))
          return false;
      else
          return memcmp(k1, k2, strlen(k1)) == 0;
  }

  KeyEqualityChecker(int dummy) {
    (void)dummy;

    return;
  }

  KeyEqualityChecker() = delete;
  //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};
/////////////////////////////////////////////////////////////////////////////////

////////////////////////Helper functions for P-HOT/////////////////////////////
typedef struct IntKeyVal {
    uint64_t key;
    uintptr_t value;
} IntKeyVal;

template<typename ValueType = IntKeyVal *>
class IntKeyExtractor {
    public:
    typedef uint64_t KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->key;
    }
};

template<typename ValueType = Key *>
class KeyExtractor {
    public:
    typedef char const * KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return (char const *)value->fkey;
    }
};
/////////////////////////////////////////////////////////////////////////////////

////////////////////////Helper functions for P-CLHT/////////////////////////////
typedef struct thread_data {
    uint32_t id;
    clht_t *ht;
} thread_data_t;

typedef struct barrier {
    pthread_cond_t complete;
    pthread_mutex_t mutex;
    int count;
    int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n) {
    pthread_cond_init(&b->complete, NULL);
    pthread_mutex_init(&b->mutex, NULL);
    b->count = n;
    b->crossing = 0;
}

void barrier_cross(barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->crossing++;
    if (b->crossing < b->count) {
        pthread_cond_wait(&b->complete, &b->mutex);
    } else {
        pthread_cond_broadcast(&b->complete);
        b->crossing = 0;
    }
    pthread_mutex_unlock(&b->mutex);
}

barrier_t barrier;

static uint64_t LOAD_SIZE = 400000000;
static uint64_t RUN_SIZE  = 10000000;

void loadKey(TID tid, Key &key) {
    return ;
}

#define start_end_key(total) \
    uint64_t start_key = total / num_thread * i;    \
    uint64_t thread_size = (i != num_thread-1) ? (total / num_thread) : (total - (total / num_thread * (num_thread - 1)));  \
    uint64_t end_key = start_key + thread_size;

void ycsb_load_run_randint(int index_type, int wl, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (wl == WORKLOAD_A) {
        init_file = "./index-microbench/workloads/loada_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnsa_unif_int.dat";
    } else if (wl == WORKLOAD_B) {
        init_file = "./index-microbench/workloads/loadb_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnsb_unif_int.dat";
    } else if (wl == WORKLOAD_C) {
        init_file = "./index-microbench/workloads/loadc_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnsc_unif_int.dat";
    } else if (wl == WORKLOAD_D) {
        init_file = "./index-microbench/workloads/loadd_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnsd_unif_int.dat";
    } else if (wl == WORKLOAD_E) {
        init_file = "./index-microbench/workloads/loade_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnse_unif_int.dat";
    } else if (wl == WORKLOAD_F) {
        init_file = "./index-microbench/workloads/loadf_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnsf_unif_int.dat";
    } else if (wl == WORKLOAD_LOAD) {
        init_file = "./index-microbench/workloads/loadload_unif_int.dat";
        txn_file = "./index-microbench/workloads/txnsload_unif_int.dat";
    }

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    int count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }
        init_keys.push_back(key);
        count++;
    }

    LOAD_SIZE = count;
    fprintf(stderr, "LOAD SIZE: %d\n", LOAD_SIZE);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(update) == 0) {
            ops.push_back(OP_UPDATE);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(key);
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    RUN_SIZE = count;
    fprintf(stderr, "RUN SIZE: %d\n", RUN_SIZE);

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);

    std::vector<std::pair<uint64_t,uint64_t>> rec_latency;
#ifdef STAT_LATENCY
    rec_latency.resize(LOAD_SIZE);
#endif

#ifdef STAT_PAPI
    long long* load_count  = new long long[num_thread];
    long long* store_count = new long long[num_thread];
    long long* llc_access  = new long long[num_thread];
    long long* llc_miss    = new long long[num_thread];
#endif

    space_usage("before");

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree.getThreadInfo();
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        Key *key = key->make_leaf(init_keys[j], sizeof(uint64_t), init_keys[j]);
                        tree.insert(key, t);
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("art");

        {
            // Run
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree.getThreadInfo();
                    llc_stat_start();
                    for (size_t j = start_key; j < end_key; ++j) {
                        if (ops[j] == OP_INSERT) {
                            Key *key = key->make_leaf(keys[j], sizeof(uint64_t), keys[j]);
                            tree.insert(key, t);
                        } else if (ops[j] == OP_READ) {
                            Key *key = key->make_leaf(keys[j], sizeof(uint64_t), 0);
                            uint64_t *val = reinterpret_cast<uint64_t *>(tree.lookup(key, t));
                            if (*val != keys[j]) {
                                std::cout << "[ART] wrong key read: " << val << " expected:" << keys[j] << std::endl;
                                exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            Key *results[200];
                            Key *continueKey = NULL;
                            size_t resultsFound = 0;
                            size_t resultsSize = ranges[j];
                            Key *start = start->make_leaf(keys[j], sizeof(uint64_t), 0);
                            tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
#ifdef HOT
    } else if (index_type == TYPE_HOT) {
        hot::rowex::HOTRowex<IntKeyVal *, IntKeyExtractor> mTrie;

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        IntKeyVal *key;
                        posix_memalign((void **)&key, 64, sizeof(IntKeyVal));
                        key->key = init_keys[j]; key->value = init_keys[j];
                        Dummy::clflush((char *)key, sizeof(IntKeyVal), true, true);
                        if (!(mTrie.insert(key))) {
                            fprintf(stderr, "[HOT] load insert fail\n");
                            exit(1);
                        }
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("hot");

        {
            // Run
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();
                    for (size_t j = start_key; j < end_key; ++j) {
                        if (ops[j] == OP_INSERT) {
                            IntKeyVal *key;
                            posix_memalign((void **)&key, 64, sizeof(IntKeyVal));
                            key->key = keys[j]; key->value = keys[j];
                            Dummy::clflush((char *)key, sizeof(IntKeyVal), true, true);
                            if (!(mTrie.insert(key))) {
                                fprintf(stderr, "[HOT] run insert fail\n");
                                exit(1);
                            }
                        } else if (ops[j] == OP_READ) {
                            idx::contenthelpers::OptionalValue<IntKeyVal *> result = mTrie.lookup(keys[j]);
                            if (!result.mIsValid || result.mValue->value != keys[j]) {
                                printf("mIsValid = %d\n", result.mIsValid);
                                printf("Return value = %lu, Correct value = %lu\n", result.mValue->value, keys[j]);
                                exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            idx::contenthelpers::OptionalValue<IntKeyVal *> result = mTrie.scan(keys[j], ranges[j]);
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
#endif
    } else if (index_type == TYPE_BWTREE) {
        auto t = new BwTree<uint64_t, uint64_t, KeyComparator, KeyEqualityChecker>{true, KeyComparator{1}, KeyEqualityChecker{1}};
        t->UpdateThreadLocal(1);
        t->AssignGCID(0);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            t->UpdateThreadLocal(num_thread);

            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    t->AssignGCID(i);
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        t->Insert(init_keys[j], init_keys[j]);
                        stat_latency_stop(j);
                    }
                    t->UnregisterThread(i);
                });
            }

            for (auto& t : threads)
                t.join();
            t->UpdateThreadLocal(1);
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("bwtree");

        {
            // Run
            auto starttime = std::chrono::high_resolution_clock::now();
            t->UpdateThreadLocal(num_thread);
            std::vector<std::thread> threads;

            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();
                    std::vector<uint64_t> v{};
                    v.reserve(1);
                    t->AssignGCID(i);
                    for (uint64_t j = start_key; j < end_key; j++) {
                        if (ops[j] == OP_INSERT) {
                            t->Insert(keys[j], keys[j]);
                        } else if (ops[j] == OP_READ) {
                            v.clear();
                            t->GetValue(keys[j], v);
                            if (v[0] != keys[j]) {
                                std::cout << "[BWTREE] wrong key read: " << v[0] << " expected:" << keys[j] << std::endl;
                            }
                        } else if (ops[j] == OP_SCAN) {
                            uint64_t buf[200];
                            auto it = t->Begin(keys[j]);

                            int resultsFound = 0;
                            while (it.IsEnd() != true && resultsFound != ranges[j]) {
                                buf[resultsFound] = it->second;
                                resultsFound++;
                                it++;
                            }
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    t->UnregisterThread(i);
                    llc_stat_stop();
                });
            }

            for (auto& t : threads)
                t.join();
            t->UpdateThreadLocal(1);
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_MASSTREE) {
        masstree::masstree *tree = new masstree::masstree();

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree->getThreadInfo();
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        tree->put(init_keys[j], &init_keys[j], t);
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("masstree");

        {
            // Run
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();
                    auto t = tree->getThreadInfo();
                    for (size_t j = start_key; j < end_key; ++j) {
                        if (ops[j] == OP_INSERT) {
                            tree->put(keys[j], &keys[j], t);
                        } else if (ops[j] == OP_READ) {
                            uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[j], t));
                            if (*ret != keys[j]) {
                                printf("[MASS] search key = %lu, search value = %lu\n", keys[j], *ret);
                                exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            uint64_t buf[200];
                            int ret = tree->scan(keys[j], ranges[j], buf, t);
                        } else if (ops[j] == OP_DELETE) {
                            tree->del(keys[j], t);
                        } else if (ops[j] == OP_UPDATE) {
                            tree->put(keys[j], &keys[j], t);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_CLHT) {
        clht_t *hashtable = clht_create(512);

        barrier_init(&barrier, num_thread);

        thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));

        struct timeval tv;
        gettimeofday(&tv,NULL);
        long long start_micro_sec = (long long)1000000*(long long)tv.tv_sec+(long long)tv.tv_usec;
        printf("start at %lld\n", start_micro_sec);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    tds[i].id = i;
                    tds[i].ht = hashtable;
                    clht_gc_thread_init(tds[i].ht, tds[i].id);
                    barrier_cross(&barrier);
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        clht_put(tds[i].ht, init_keys[j], init_keys[j]);
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        barrier.crossing = 0;

        space_usage("clht");

        {
            // Run
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();
                    tds[i].id = i;
                    tds[i].ht = hashtable;
                    clht_gc_thread_init(tds[i].ht, tds[i].id);
                    barrier_cross(&barrier);

                    for (uint64_t j = start_key; j < end_key; j++) {
                        if (ops[j] == OP_INSERT) {
                            clht_put(tds[i].ht, keys[j], keys[j]);
                        } else if (ops[j] == OP_READ) {
                            uintptr_t val = clht_get(tds[i].ht->ht, keys[j]);
                            if (val != keys[j]) {
                                std::cout << "[CLHT] wrong key read: " << val << "expected: " << keys[j] << std::endl;
                                exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
        clht_gc_destroy(hashtable);
    } else if (index_type == TYPE_CCEH) {
        Hash *table = new CCEH(2);

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        table->Insert(init_keys[j], reinterpret_cast<const char*>(&init_keys[j]));
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("cceh");

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();

                    for (uint64_t j = start_key; j < end_key; j++) {
                        if (ops[j] == OP_INSERT) {
                            table->Insert(keys[j], reinterpret_cast<const char*>(&keys[j]));
                        } else if (ops[j] == OP_READ) {
                            uint64_t *val = reinterpret_cast<uint64_t *>(const_cast<char *>(table->Get(keys[j])));
                            if (val == NULL) {
                                //std::cout << "[CCEH] wrong value is read <expected:> " << keys[i] << std::endl;
                                //exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_FASTFAIR) {
        fastfair::btree *bt = new fastfair::btree();

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        bt->btree_insert(init_keys[j], (char *) &init_keys[j]);
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("fastfair");

        {
            // Run
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();
                    for (size_t j = start_key; j < end_key; ++j) {
                        if (ops[j] == OP_INSERT) {
                            bt->btree_insert(keys[j], (char *) &keys[j]);
                        } else if (ops[j] == OP_READ) {
                            uint64_t *ret = reinterpret_cast<uint64_t *>(bt->btree_search(keys[j]));
                            if (ret == NULL) {
                                //printf("NULL is found\n");
                            } else if (*ret != keys[j]) {
                                //printf("[FASTFAIR] wrong value is returned: <expected> %lu\n", keys[i]);
                                //exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            uint64_t buf[200];
                            int resultsFound = 0;
                            bt->btree_search_range (keys[j], UINT64_MAX, buf, ranges[j], resultsFound);
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_LEVELHASH) {
        Hash *table = new LevelHashing(10);

        struct timeval tv;
        gettimeofday(&tv,NULL);
        long long start_micro_sec = (long long)1000000*(long long)tv.tv_sec+(long long)tv.tv_usec;
        printf("start at %lld\n", start_micro_sec);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        table->Insert(init_keys[j], reinterpret_cast<const char*>(&init_keys[j]));
                        stat_latency_stop(j);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        space_usage("levelhash");

        {
            // Run
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,i,start_key,end_key](){
                    llc_stat_start();
                    for (size_t j = start_key; j < end_key; ++j) {
                        if (ops[j] == OP_INSERT) {
                            table->Insert(keys[j], reinterpret_cast<const char*>(&keys[j]));
                        } else if (ops[j] == OP_READ) {
                            auto val = table->Get(keys[j]);
                            if (val == NONE) {
                                std::cout << "[Level Hashing] wrong key read: " << *(uint64_t *)val << " expected: " << keys[j] << std::endl;
                                exit(1);
                            }
                        } else if (ops[j] == OP_SCAN) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_COMBOTREE) {
        combotree::ComboTree *tree = new combotree::ComboTree("/pmem0/combotree", (100*1024*1024*1024UL), true);

        struct timeval tv;
        gettimeofday(&tv,NULL);
        long long start_micro_sec = (long long)1000000*(long long)tv.tv_sec+(long long)tv.tv_usec;
        printf("start at %lld\n", start_micro_sec);

        {
            // Load
            std::vector<std::thread> threads;
            std::atomic<int> wait_cnt = 0;
            auto starttime = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        stat_latency_start();
                        tree->Put(init_keys[j], init_keys[j]);
                        stat_latency_stop(j);
#ifdef STAT_LATENCY
                        // some thread sleep when expanding
                        if (rec_latency[j].second > 10000) {
                            // rec_latency[j].second = 3;
                            wait_cnt++;
                        }
#endif
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
#ifdef STAT_LATENCY
            printf("ComboTree very large latency count: %d\n", wait_cnt.load());
#endif
        }

        std::cout << "ComboTree Usage: " << human_readable(tree->Usage()) << std::endl;

        space_usage("combotree");

        {
            // Run
            std::vector<std::thread> threads;
            auto starttime = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    llc_stat_start();
                    uint64_t value;
                    for (size_t j = start_key; j < end_key; ++j) {
                        if (ops[j] == OP_INSERT) {
                            tree->Put(keys[j], keys[j]);
                        } else if (ops[j] == OP_READ) {
                            uint64_t value;
                            bool ret = tree->Get(keys[j], value);
                            if (ret != true)
                                printf("GET ERROR!\n");
                        } else if (ops[j] == OP_SCAN) {
                            uint64_t buf[200];
                            int resultsFound = 0;
                            combotree::ComboTree::NoSortIter iter(tree, keys[j]);
                            for (size_t k = 0; k < ranges[j]; ++k) {
                                if (iter.key() != iter.value())
                                    printf("SCAN ERROR!\n");
                                iter.key() == iter.value();
                                if (!iter.next())
                                    break;
                            }
                        } else if (ops[j] == OP_UPDATE) {
                            std::cout << "NOT SUPPORTED CMD!\n";
                            exit(0);
                        }
                    }
                    llc_stat_stop();
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    }

#ifdef STAT_LATENCY
    OutputLatency(rec_latency);
#endif

#ifdef STAT_PAPI
    long long total_load_count = 0;
    long long total_store_count = 0;
    long long total_llc_access = 0;
    long long total_llc_miss = 0;
    for (int i = 0; i < num_thread; ++i) {
        total_load_count += load_count[i];
        total_store_count += store_count[i];
        total_llc_access += llc_access[i];
        total_llc_miss += llc_miss[i];

        std::cout << "Thread " << i << ":" << std::endl;
        std::cout << "Load Instructions:  " << load_count[i] << std::endl;
        std::cout << "Store Instructions: " << store_count[i] << std::endl;
        std::cout << "L3 Cache Access:    " << llc_access[i] << std::endl;
        std::cout << "L3 Cache Misses:    " << llc_miss[i] << std::endl;
        std::cout << "L3 Cache Miss Rate: " << (double)llc_miss[i] / (double)llc_access[i] * 100.0 << "%" << std::endl;
        std::cout << std::endl;
    }
    std::cout << "Total:" << std::endl;
    std::cout << "Load Instructions:  " << total_load_count << std::endl;
    std::cout << "Store Instructions: " << total_store_count << std::endl;
    std::cout << "L3 Cache Access:    " << total_llc_access << std::endl;
    std::cout << "L3 Cache Misses:    " << total_llc_miss << std::endl;
    std::cout << "L3 Cache Miss Rate: " << (double)total_llc_miss / (double)total_llc_access * 100.0 << "%" << std::endl;
#endif
}

int main(int argc, char **argv) {
    if (argc != 4) {
        std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [number of threads]\n";
        std::cout << "1. index type: art hot bwtree masstree clht\n";
        std::cout << "               fastfair levelhash cceh woart\n";
        std::cout << "2. ycsb workload type: a, b, c, e\n";
        std::cout << "3. number of threads (integer)\n";
        return 1;
    }

    int index_type;
    if (strcmp(argv[1], "art") == 0)
        index_type = TYPE_ART;
    else if (strcmp(argv[1], "hot") == 0) {
#ifdef HOT
        index_type = TYPE_HOT;
#else
        return 1;
#endif
    } else if (strcmp(argv[1], "bwtree") == 0)
        index_type = TYPE_BWTREE;
    else if (strcmp(argv[1], "masstree") == 0)
        index_type = TYPE_MASSTREE;
    else if (strcmp(argv[1], "clht") == 0)
        index_type = TYPE_CLHT;
    else if (strcmp(argv[1], "fastfair") == 0)
        index_type = TYPE_FASTFAIR;
    else if (strcmp(argv[1], "levelhash") == 0)
        index_type = TYPE_LEVELHASH;
    else if (strcmp(argv[1], "cceh") == 0)
        index_type = TYPE_CCEH;
    else if (strcmp(argv[1], "woart") == 0)
        index_type = TYPE_WOART;
    else if (strcmp(argv[1], "combotree") == 0)
        index_type = TYPE_COMBOTREE;
    else {
        fprintf(stderr, "Unknown index type: %s\n", argv[1]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[2], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[2], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[2], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[2], "d") == 0) {
        wl = WORKLOAD_D;
    } else if (strcmp(argv[2], "e") == 0) {
        wl = WORKLOAD_E;
    } else if (strcmp(argv[2], "load") == 0) {
        wl = WORKLOAD_LOAD;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int num_thread = atoi(argv[3]);

    std::vector<uint64_t> init_keys;
    std::vector<uint64_t> keys;
    std::vector<int> ranges;
    std::vector<int> ops;

    init_keys.reserve(LOAD_SIZE);
    keys.reserve(RUN_SIZE);
    ranges.reserve(RUN_SIZE);
    ops.reserve(RUN_SIZE);

    memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
    memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
    memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
    memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

    printf("%s, workload%s, threads %s\n", argv[1], argv[2], argv[3]);

#ifdef STAT_PAPI
    std::cout << "STAT_PAPI enabled" << std::endl;
#endif

#ifdef STAT_PERF
    std::cout << "STAT_PERF enabled" << std::endl;
#endif

#ifdef STAT_SPACE_USAGE
    std::cout << "STAT_SPACE_USAGE enabled" << std::endl;
#endif

#ifdef STAT_LATENCY
    std::cout << "STAT_LATENCY enabled" << std::endl;
#endif

#ifdef STAT_PAPI
    if(PAPI_library_init(PAPI_VER_CURRENT) != PAPI_VER_CURRENT) {
        std::cerr << "PAPI_library_init ERROR!" << std::endl;
        return -1;
    }
    if(PAPI_thread_init(pthread_self) != PAPI_OK) {
        fprintf(stderr, "ERROR: PAPI library failed to initialize for pthread\n");
        exit(1);
    }
#endif

    ycsb_load_run_randint(index_type, wl, num_thread, init_keys, keys, ranges, ops);

#ifdef STAT_PAPI
    PAPI_shutdown();
#endif
    return 0;
}
