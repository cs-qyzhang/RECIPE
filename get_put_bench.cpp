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

    int count = 0;
    while (count < LOAD_SIZE && infile_load.good()) {
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
    assert(LOAD_SIZE == 400000000);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while (count < RUN_SIZE && infile_txn.good()) {
        infile_txn >> op >> key;
        keys.push_back(key);
        count++;
    }

    RUN_SIZE = count;
    fprintf(stderr, "RUN SIZE: %d\n", RUN_SIZE);
    assert(RUN_SIZE == 20000000);

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);

    if (index_type == TYPE_ART) {
        ART_ROWEX::Tree tree(loadKey);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(init_keys[i], sizeof(uint64_t), init_keys[i]);
                    tree.insert(key, t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(keys[i], sizeof(uint64_t), 0);
                    uint64_t *val = reinterpret_cast<uint64_t *>(tree.lookup(key, t));
                    if (*val != keys[i]) {
                        std::cout << "[ART] wrong key read: " << val << " expected:" << keys[i] << std::endl;
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            Key *end = end->make_leaf(UINT64_MAX, sizeof(uint64_t), 0);
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree.getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    Key *key = key->make_leaf(keys[i], sizeof(uint64_t), keys[i]);
                    tree.insert(key, t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: put, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
#ifdef HOT
    } else if (index_type == TYPE_HOT) {
        hot::rowex::HOTRowex<IntKeyVal *, IntKeyExtractor> mTrie;

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    IntKeyVal *key;
                    posix_memalign((void **)&key, 64, sizeof(IntKeyVal));
                    key->key = init_keys[i]; key->value = init_keys[i];
                    Dummy::clflush((char *)key, sizeof(IntKeyVal), true, true);
                    if (!(mTrie.insert(key))) {
                        fprintf(stderr, "[HOT] load insert fail\n");
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    idx::contenthelpers::OptionalValue<IntKeyVal *> result = mTrie.lookup(keys[i]);
                    if (!result.mIsValid || result.mValue->value != keys[i]) {
                        printf("mIsValid = %d\n", result.mIsValid);
                        printf("Return value = %lu, Correct value = %lu\n", result.mValue->value, keys[i]);
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    IntKeyVal *key;
                    posix_memalign((void **)&key, 64, sizeof(IntKeyVal));
                    key->key = keys[i]; key->value = keys[i];
                    Dummy::clflush((char *)key, sizeof(IntKeyVal), true, true);
                    if (!(mTrie.insert(key))) {
                        fprintf(stderr, "[HOT] run insert fail\n");
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
#endif
    } else if (index_type == TYPE_MASSTREE) {
        masstree::masstree *tree = new masstree::masstree();

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree->getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    tree->put(init_keys[i], &init_keys[i], t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree->getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i], t));
                    if (*ret != keys[i]) {
                        printf("[MASS] search key = %lu, search value = %lu\n", keys[i], *ret);
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                auto t = tree->getThreadInfo();
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    tree->put(keys[i], &keys[i], t);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: put, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_CLHT) {
        clht_t *hashtable = clht_create(512);

        barrier_init(&barrier, num_thread);

        thread_data_t *tds = (thread_data_t *) malloc(num_thread * sizeof(thread_data_t));

        std::atomic<int> next_thread_id;

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            next_thread_id.store(0);
            auto func = [&]() {
                int thread_id = next_thread_id.fetch_add(1);
                tds[thread_id].id = thread_id;
                tds[thread_id].ht = hashtable;

                uint64_t start_key = LOAD_SIZE / num_thread * (uint64_t)thread_id;
                uint64_t thread_size = (thread_id != num_thread-1) ? (LOAD_SIZE/num_thread) : (LOAD_SIZE - (LOAD_SIZE/num_thread*(num_thread-1)));
                uint64_t end_key = start_key + thread_size;

                clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
                barrier_cross(&barrier);

                for (uint64_t i = start_key; i < end_key; i++) {
                    clht_put(tds[thread_id].ht, init_keys[i], init_keys[i]);
                }
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        barrier.crossing = 0;

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            next_thread_id.store(0);
            auto func = [&]() {
                int thread_id = next_thread_id.fetch_add(1);
                tds[thread_id].id = thread_id;
                tds[thread_id].ht = hashtable;

                uint64_t size = RUN_SIZE / 2;
                uint64_t start_key = size / num_thread * (uint64_t)thread_id;
                uint64_t thread_size = (thread_id != num_thread-1) ? (size/num_thread) : (size - (size/num_thread*(num_thread-1)));
                uint64_t end_key = start_key + thread_size;

                clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
                barrier_cross(&barrier);

                for (uint64_t i = start_key; i < end_key; i++) {
                    uintptr_t val = clht_get(tds[thread_id].ht->ht, keys[i]);
                    if (val != keys[i]) {
                        std::cout << "[CLHT] wrong key read: " << val << "expected: " << keys[i] << std::endl;
                        exit(1);
                    }
                }
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            next_thread_id.store(0);
            auto func = [&]() {
                int thread_id = next_thread_id.fetch_add(1);
                tds[thread_id].id = thread_id;
                tds[thread_id].ht = hashtable;

                uint64_t size = RUN_SIZE / 2;
                uint64_t start_key = size / num_thread * (uint64_t)thread_id + RUN_SIZE/2;
                uint64_t thread_size = (thread_id != num_thread-1) ? (size/num_thread) : (size - (size/num_thread*(num_thread-1)));
                uint64_t end_key = start_key + thread_size;

                clht_gc_thread_init(tds[thread_id].ht, tds[thread_id].id);
                barrier_cross(&barrier);

                for (uint64_t i = start_key; i < end_key; i++) {
                    clht_put(tds[thread_id].ht, keys[i], keys[i]);
                }
            };

            std::vector<std::thread> thread_group;

            for (int i = 0; i < num_thread; i++)
                thread_group.push_back(std::thread{func});

            for (int i = 0; i < num_thread; i++)
                thread_group[i].join();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: put, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
        clht_gc_destroy(hashtable);
    } else if (index_type == TYPE_FASTFAIR) {
        fastfair::btree *bt = new fastfair::btree();

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    bt->btree_insert(init_keys[i], (char *) &init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    uint64_t *ret = reinterpret_cast<uint64_t *>(bt->btree_search(keys[i]));
                    if (ret == NULL) {
                        //printf("NULL is found\n");
                    } else if (*ret != keys[i]) {
                        //printf("[FASTFAIR] wrong value is returned: <expected> %lu\n", keys[i]);
                        //exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    bt->btree_insert(keys[i], (char *) &keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: put, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_LEVELHASH) {
        Hash *table = new LevelHashing(10);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    table->Insert(init_keys[i], reinterpret_cast<const char*>(&init_keys[i]));
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    auto val = table->Get(keys[i]);
                    if (val == NONE) {
                        std::cout << "[Level Hashing] wrong key read: " << *(uint64_t *)val << " expected: " << keys[i] << std::endl;
                        exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    table->Insert(keys[i], reinterpret_cast<const char*>(&keys[i]));
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: put, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_CCEH) {
        Hash *table = new CCEH(2);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    table->Insert(init_keys[i], reinterpret_cast<const char*>(&init_keys[i]));
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    uint64_t *val = reinterpret_cast<uint64_t *>(const_cast<char *>(table->Get(keys[i])));
                    if (val == NULL) {
                        //std::cout << "[CCEH] wrong value is read <expected:> " << keys[i] << std::endl;
                        //exit(1);
                    }
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    table->Insert(keys[i], reinterpret_cast<const char*>(&keys[i]));
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }
    } else if (index_type == TYPE_COMBOTREE) {
        combotree::ComboTree *tree = new combotree::ComboTree("/pmem0/combotree", (100*1024*1024*1024UL), true);

        {
            // Load
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    tree->Put(init_keys[i], init_keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Get
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE/2), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    uint64_t value;
                    assert(tree->Get(keys[i], value) == true);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: get, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
        }

        {
            // Put
            auto starttime = std::chrono::high_resolution_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(RUN_SIZE/2, RUN_SIZE), [&](const tbb::blocked_range<uint64_t> &scope) {
                for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                    tree->Put(keys[i], keys[i]);
                }
            });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - starttime);
            printf("Throughput: put, %f ,ops/us\n", (RUN_SIZE/2.0 * 1.0) / duration.count());
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
