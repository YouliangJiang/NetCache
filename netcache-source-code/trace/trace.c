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

/*
 * functions for processing
 */

uint32_t zipf_alpha = 0;

static void analyze(void) {
    const uint64_t rand_seed = 0;
    const uint64_t partial_size = 30;
    const uint64_t key_space_size = 1000*1000*1000;

    struct zipf_gen_state zipf_gen;
    zipf_init(&zipf_gen, key_space_size, zipf_alpha / (double) 100, rand_seed);
    printf("finish initialization\n");

    uint64_t sum = 0;
    uint64_t partial_sum[partial_size];
    memset(&partial_sum, 0, sizeof(partial_sum));
    uint64_t i, j, partial;
    for (i = 0; i < 1000*1000*100; i++) {
        uint64_t key = zipf_next(&zipf_gen);
        sum++;

        partial = 1;
        for (j = 0; j < partial_size; j++) {
            if (key < partial) {
                partial_sum[j]++;
            }
            partial *= 2;
        }
    }

    for (i = 0; i < partial_size; i++) {
        printf("%"PRIu64"\t%.2f\n", i, partial_sum[i] / (double) sum * 100);
    }
}

static void generate(void) {
    const uint64_t sample_size = 1000*1000;
    const uint64_t sample_len = 1000*1000*10;
    const uint64_t rand_seed = 0;
    const uint64_t key_space_size = 1000*1000*1000;

    struct zipf_gen_state zipf_gen;
    zipf_init(&zipf_gen, key_space_size, zipf_alpha / (double) 100, rand_seed);

    uint64_t sum = 0;
    uint64_t partial_sum = 0;
    memset(&partial_sum, 0, sizeof(partial_sum));
    while (partial_sum < sample_len) {
        uint64_t key = zipf_next(&zipf_gen);
        sum++;

        if (key < sample_size) {
            printf("%"PRIu64"\n", key);
            partial_sum++;
        }
    }
    printf("%"PRIu64"\t%.2f\n", sample_size, partial_sum / (double) sum * 100);
}

static void nc_parse_args_help(void) {
    printf("client\n"
        "  -z zipf alpha (e.g., 0, 90, 95, 99)\n");
}

static int parse_args(int argc, char **argv) {
    int opt, num;
    while ((opt = getopt(argc, argv, "z:")) != -1) {
        switch (opt) {
        case 'z':
            num = atoi(optarg);
            if (num >= 0 && num < 100) {
                zipf_alpha = num;
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
    /*printf("parsed arguments: zipf alpha: %"PRIu32
        "\n",
        zipf_alpha);*/
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
    generate();
    return 0;
}
