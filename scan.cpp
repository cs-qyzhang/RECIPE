#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include "tbb/tbb.h"

using namespace std;

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

#ifdef HOT
#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>
#endif

#undef STAT_LATENCY

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

void ycsb_load_run_randint(int index_type, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        size_t range,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    init_file = "./index-microbench/workloads/loadscan_unif_int.dat";
    txn_file = "./index-microbench/workloads/txnsscan_unif_int.dat";

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;

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
        if (op.compare(scan) == 0) {
            keys.push_back(key);
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
                        Key *key = key->make_leaf(init_keys[j], sizeof(uint64_t), init_keys[j]);
                        tree.insert(key, t);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Scan
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree.getThreadInfo();
                    Key **results = new Key*[range];
                    for (size_t j = start_key; j < end_key; ++j) {
                        Key *continueKey = NULL;
                        size_t resultsFound = 0;
                        size_t resultsSize = range;
                        Key *start = start->make_leaf(keys[j], sizeof(uint64_t), 0);
                        tree.lookupRange(start, end, continueKey, results, resultsSize, resultsFound, t);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: scan, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
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
                        IntKeyVal *key;
                        posix_memalign((void **)&key, 64, sizeof(IntKeyVal));
                        key->key = init_keys[j]; key->value = init_keys[j];
                        Dummy::clflush((char *)key, sizeof(IntKeyVal), true, true);
                        if (!(mTrie.insert(key))) {
                            fprintf(stderr, "[HOT] load insert fail\n");
                            exit(1);
                        }
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Scan
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        idx::contenthelpers::OptionalValue<IntKeyVal *> result = mTrie.scan(keys[j], range);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: scan, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
#endif
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
                        tree->put(init_keys[j], &init_keys[j], t);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Scan
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree->getThreadInfo();
                    uint64_t* buf = new uint64_t[range];
                    for (size_t j = start_key; j < end_key; ++j) {
                        int ret = tree->scan(keys[j], range, buf, t);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: scan, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
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
                        bt->btree_insert(init_keys[j], (char *) &init_keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Scan
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    uint64_t* buf = new uint64_t[range];
                    for (size_t j = start_key; j < end_key; ++j) {
                        int resultsFound = 0;
                        bt->btree_search_range(keys[j], UINT64_MAX, buf, range, resultsFound);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: scan, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_COMBOTREE) {
        combotree::ComboTree *tree = new combotree::ComboTree("/pmem0/combotree", (100*1024*1024*1024UL), true);

        {
            // Load
            std::vector<std::thread> threads;
            auto starttime = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([=,&init_keys,&rec_latency](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        tree->Put(init_keys[j], init_keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        std::cout << "ComboTree Usage: " << human_readable(tree->Usage()) << std::endl;

        {
            // Run
            std::vector<std::thread> threads;
            auto starttime = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                uint64_t* buf = new uint64_t[range];
                threads.emplace_back([=,&keys,&ops](){
                    uint64_t value;
                    for (size_t j = start_key; j < end_key; ++j) {
                        uint64_t start_key = keys[j];
                        combotree::ComboTree::NoSortIter iter(tree, start_key);
                        for (size_t k = 0; k < range; ++k) {
                            buf[k] = iter.key();
                            if (!iter.next())
                                break;
                        }
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: scan, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 4) {
        std::cout << "Usage: ./ycsb [index type] [number of threads] [scan range]\n";
        std::cout << "1. index type: art hot bwtree masstree clht\n";
        std::cout << "               fastfair levelhash cceh woart\n";
        std::cout << "2. number of threads (integer)\n";
        std::cout << "3. scan range\n";
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

    int num_thread = atoi(argv[2]);
    tbb::task_scheduler_init init(num_thread);

    int range = atoi(argv[3]);

    std::vector<uint64_t> init_keys;
    std::vector<uint64_t> keys;
    std::vector<int> ops;

    init_keys.reserve(LOAD_SIZE);
    keys.reserve(RUN_SIZE);
    ops.reserve(RUN_SIZE);

    memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
    memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
    memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

    printf("%s, threads %s, scan range %s\n", argv[1], argv[2], argv[3]);

    ycsb_load_run_randint(index_type, num_thread, init_keys, keys, range, ops);

    return 0;
}
