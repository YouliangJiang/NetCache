#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/queue.h>
#include <time.h>
#include <assert.h>
#include <arpa/inet.h>
#include <getopt.h>

#include <rte_memory.h>
#include <rte_memzone.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_debug.h>
#include <rte_cycles.h>
#include <rte_mbuf.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <rte_ethdev.h>

#include "util.h"
#include "zipf.h"
#include "key_hash.h"

/*
 * constants
 */


/*
 * custom types
 */

typedef struct node_statistics_ {
    uint64_t cache_load;
    uint64_t backend_load;
    uint64_t trace_load;
    uint64_t uniform_load;
} node_statistics;

/*
 * global variables
 */
uint32_t mode = 0;
uint32_t cache_size = 0;
uint32_t zipf_alpha = 0;
uint32_t write_ratio = 0;
uint64_t kv_trace[TRACE_LENGTH];
uint32_t kv_trace_idx;
uint32_t kv_trace_p;
node_statistics node_stat[NODE_NUM];

static void print() {
    uint32_t i;
    uint64_t max_load = 0;
    for (i = 0; i < NODE_NUM; i++) {
        if (max_load < node_stat[i].backend_load) {
            max_load = node_stat[i].backend_load;
        }
    }

    float total_cache = 0;
    float total_backend = 0;
    for (i = 0; i < NODE_NUM; i++) {
        uint64_t total_load = node_stat[i].cache_load
            + node_stat[i].backend_load;
        printf("%"PRIu32"\t%.2f\t%.2f\t%.2f\t%.2f\n",
            i,
            node_stat[i].cache_load * 100.0 / max_load,
            node_stat[i].backend_load * 100.0 / max_load,
            node_stat[i].trace_load * 100.0 / total_load,
            node_stat[i].uniform_load * 100.0 / total_load);
        total_cache += node_stat[i].cache_load * 100.0 / max_load;
        total_backend += node_stat[i].backend_load * 100.0 / max_load;
    }
    printf("cache: %.2f\tbackend: %.2f\tall: %.2f\n",
        total_cache, total_backend, total_cache + total_backend);
}

static void simulate_zipf(void) {
    memset(&node_stat, 0, sizeof(node_stat));
    struct zipf_gen_state zipf_gen;
    zipf_init(&zipf_gen, KEY_SPACE_SIZE, zipf_alpha * 0.01, 0);

    uint32_t i, is_cache;
    uint64_t key;
    for (i = 0; i < SIMULATION_LENGTH; i++) {
        // generate key
        if (zipf_alpha == 0) {
            key = rte_rand() % KEY_SPACE_SIZE;
        } else {
            key = zipf_next(&zipf_gen);
        }

        if (key < cache_size) {
            is_cache = 1;
        } else {
            is_cache = 0;
        }

        // map to backend
        char key_buf[100];
        sprintf(key_buf, "%"PRIu64, key);
        uint64_t key_hash = StringToHash(key_buf, strlen(key_buf));
        uint64_t node_id = key_hash % NODE_NUM;
        if (is_cache) {
            node_stat[node_id].cache_load++;
        } else {
            node_stat[node_id].backend_load++;
        }
    }
    print();
}

static void simulate_trace(void) {
    memset(&node_stat, 0, sizeof(node_stat));
    uint32_t i, is_trace, is_cache;
    uint64_t key;
    for (i = 0; i < SIMULATION_LENGTH; i++) {
        // generate key
        if (zipf_alpha == 0
            || rte_rand() % 100 > kv_trace_p
            || rte_rand() % 100 < write_ratio) {
            key = rte_rand() % KEY_SPACE_SIZE;
            is_trace = 0;
        } else {
            key = kv_trace[kv_trace_idx];
            is_trace = 1;

            kv_trace_idx++;
            if (unlikely(kv_trace_idx == TRACE_LENGTH)) {
                kv_trace_idx = 0;
            }
        }

        if (key < cache_size) {
            is_cache = 1;
        } else {
            is_cache = 0;
        }

        // map to backend
        char key_buf[100];
        sprintf(key_buf, "%"PRIu64, key);
        uint64_t key_hash = StringToHash(key_buf, strlen(key_buf));
        //uint64_t key_hash = StringToHash(&key, 8);
        uint64_t node_id = key_hash % NODE_NUM;
        if (is_cache) {
            node_stat[node_id].cache_load++;
        } else {
            node_stat[node_id].backend_load++;
        }
        if (is_trace) {
            node_stat[node_id].trace_load++;
        } else {
            node_stat[node_id].uniform_load++;
        }
    }
    print();
}

static void generate_key_hash(void) {
    char buffer[100];
    uint64_t i;
    for (i = 0; i < 100*1000; i++) {
        sprintf(buffer, "%"PRIu64, i);
        uint64_t key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
        printf("%"PRIu64"\n", key_hash);
    }
}

static void generate_key_hash_dynamic_workload(void) {
    uint64_t i;
    for (i = 0; i < 100*1000; i++) {
        uint64_t key_hash = StringToHash(&i, 8) % KEY_SPACE_SIZE;
        printf("%"PRIu64"\n", key_hash);
    }
}

// initialization
static void init(void) {
    // initialize trace
    if (zipf_alpha > 0) {
        FILE * file;
        char file_name[100];
        sprintf(file_name, "trace_zipf_%"PRIu32"_key_1M_len_10M",
            zipf_alpha);
        file = fopen(file_name, "r");
        if (file) {
            int i, ret, key;
            float p;

            // scan trace
            for (i = 0; i < TRACE_LENGTH; i++) {
                if(fscanf(file, "%d", &key) != EOF) {
                    kv_trace[i] = key;
                } else {
                    rte_exit(EXIT_FAILURE, "trace file ends unexpectedly\n");
                }
            }

            // scan probablity
            if(fscanf(file, "%d %f", &key, &p) == EOF) {
                rte_exit(EXIT_FAILURE, "trace file ends unexpectedly\n");
            }
            kv_trace_p = (uint32_t) (p+0.5);
            fclose(file);

            // print
            /*for (i = 0; i < TRACE_LENGTH; i++) {
                printf("%"PRIu64"\n", kv_trace[i]);
            }
            printf("%"PRIu32"\n", kv_trace_p);*/
        } else {
            rte_exit(EXIT_FAILURE, "trace file does not exist\n");
        }
    }
}

// print help function
static void nc_parse_args_help(void) {
    printf("client\n"
        "  -m mode (0: trace; 1: simulator)\n"
        "  -z zipf alpha (e.g., 0, 90, 95, 99)\n"
        "  -c cache size (e.g., 1000, 10000)\n"
        "  -w write ratio ([0, 100])\n");
}

// parse arguments
static int parse_args(int argc, char **argv) {
    int opt, num;
    while ((opt = getopt(argc, argv, "m:z:c:w:")) != -1) {
        switch (opt) {
        case 'm':
            num = atoi(optarg);
            if (num == 0 || num == 1 || num == 2) {
                mode = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        case 'z':
            num = atoi(optarg);
            if (num >= 0 && num < 100) {
                zipf_alpha = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        case 'c':
            num = atoi(optarg);
            if (num >= 0 && num < 1000*1000) {
                cache_size = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        case 'w':
            num = atoi(optarg);
            if (num >= 0 && num <= 100) {
                write_ratio = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        default:
            nc_parse_args_help();
            return -1;
        }
    }
    printf("parsed arguments: mode: %"PRIu32
        ", zipf alpha: %"PRIu32
        ", cache size: %"PRIu32
        ", write ratio: %"PRIu32
        "\n",
        mode, zipf_alpha, cache_size, write_ratio);
    return 1;
}

/*
 * main function
 */

int main(int argc, char **argv) {
    int ret = parse_args(argc, argv);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "invalid netcache arguments\n");
    }

    if (mode == 0) {
        init();
        simulate_trace();
    } else if (mode == 1) {
        simulate_zipf();
    } else {
        generate_key_hash_dynamic_workload();
    }

    return 0;
}
