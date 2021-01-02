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

void ycsb_load_run_randint(int index_type, int wl, int num_thread,
        std::vector<uint64_t> &init_keys,
        std::vector<uint64_t> &keys,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    init_file = "./index-microbench/workloads/loadload_unif_int.dat";
    txn_file = "./index-microbench/workloads/get_put_unif_int.dat";

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    std::ifstream data("./build/data.dat");
    if (!data.good()) {
        std::cout << "can not open data.dat!" << std::endl;
        assert(0);
    }
    for (size_t i = 0; i < LOAD_SIZE; ++i) {
        uint64_t k;
        data >> k;
        init_keys.push_back(k);
    }
    for (size_t i = 0; i < RUN_SIZE/2; ++i)
        keys.push_back(init_keys[i]);
    for (size_t i = 0; i < RUN_SIZE/2; ++i) {
        uint64_t k;
        data >> k;
        keys.push_back(k);
    }
    std::cout << "finish read data.dat" << std::endl;

    if (init_keys.size() != LOAD_SIZE)
        std::cerr << "load size error: " << init_keys.size() << std::endl;

    if (keys.size() != RUN_SIZE)
        std::cerr << "run size error: " << keys.size() << std::endl;

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);

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
            // Delete
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree.getThreadInfo();
                    for (size_t j = start_key; j < end_key; ++j) {
                        Key *key = key->make_leaf(keys[j], sizeof(uint64_t), 0);
                        tree.remove(key, t);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
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
            // Delete
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    auto t = tree->getThreadInfo();
                    for (size_t j = start_key; j < end_key; ++j) {
                        tree->del(keys[j], t);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_CLHT) {
        clht_t *hashtable = clht_create(512);

        barrier_init(&barrier, num_thread);

        thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));

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
                        clht_put(tds[i].ht, init_keys[j], init_keys[j]);
                    }
                });
            }
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        barrier.crossing = 0;

        {
            // Delete
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    tds[i].id = i;
                    tds[i].ht = hashtable;
                    clht_gc_thread_init(tds[i].ht, tds[i].id);
                    barrier_cross(&barrier);

                    for (uint64_t j = start_key; j < end_key; j++) {
                        clht_remove(tds[i].ht, keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
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
                        table->Insert(init_keys[j], reinterpret_cast<const char*>(&init_keys[j]));
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Delete
            auto starttime = std::chrono::system_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (uint64_t j = start_key; j < end_key; j++) {
                        table->Delete(keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
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
            // Delete
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        bt->btree_delete(keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_LEVELHASH) {
        Hash *table = new LevelHashing(10);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        table->Insert(init_keys[j], reinterpret_cast<const char*>(&init_keys[j]));
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
            // Delete
            auto starttime = std::chrono::high_resolution_clock::now();
            std::vector<thread> threads;
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,i,start_key,end_key](){
                    for (size_t j = start_key; j < end_key; ++j) {
                        table->Delete(keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_COMBOTREE) {
        combotree::ComboTree *tree = new combotree::ComboTree("/pmem0/combotree", (100*1024*1024*1024UL), true);

        {
            std::vector<std::thread> threads;
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(LOAD_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
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

        {
            // Delete
            std::vector<std::thread> threads;
            auto starttime = std::chrono::high_resolution_clock::now();
            for (uint64_t i = 0; i < num_thread; ++i) {
                start_end_key(RUN_SIZE);
                threads.emplace_back([&,start_key,end_key,i](){
                    uint64_t value;
                    for (size_t j = start_key; j < end_key; ++j) {
                        tree->Delete(keys[j]);
                    }
                });
            }
            for (auto& t : threads)
                t.join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: delete, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    }
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
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int num_thread = atoi(argv[3]);
    tbb::task_scheduler_init init(num_thread);

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

    ycsb_load_run_randint(index_type, wl, num_thread, init_keys, keys, ranges, ops);

    return 0;
}
