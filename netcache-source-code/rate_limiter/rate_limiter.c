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

/*
 * global variables
 */

uint64_t clean_cycle = 1;
uint32_t backend_ip[NODE_NUM];

/*
 * functions for processing
 */

// generate request packet TODO
static void generate_request_pkt(uint32_t lcore_id,
    struct rte_mbuf *mbuf, uint8_t index) {
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    assert(mbuf != NULL);

    // init packet header
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    struct ipv4_hdr *ip = (struct ipv4_hdr *)((uint8_t*) eth
        + sizeof(struct ether_hdr));
    rte_memcpy(eth, header_template, sizeof(header_template));
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->pkt_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->next = NULL;
    mbuf->nb_segs = 1;
    mbuf->ol_flags = 0;

    MessageHeader* message_header = (MessageHeader*) ((uint8_t*)eth
        + sizeof(header_template));
    message_header->type = (uint8_t) TYPE_RESET_COUNTER;
    message_header->key = 0;
    message_header->seq = 0;

    ip->dst_addr = backend_ip[index];
}

// loop for rate limiter
static int32_t nc_loop(__attribute__((unused)) void *arg) {
    uint32_t lcore_id = rte_lcore_id();
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering main loop on lcore %u mode TX\n", (long long)time(NULL), lcore_id);

    struct rte_mbuf *mbuf;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;
    uint64_t clean_tsc = rte_get_tsc_hz() / 1000 * clean_cycle;
    uint64_t next_clean_tsc = cur_tsc + clean_tsc;

    uint32_t i;
    while (1) {
        // read current time
        cur_tsc = rte_rdtsc();

        // print stats at master lcore
        if (update_tsc > 0) {
            if (unlikely(cur_tsc > next_update_tsc)) {
                if (lcore_id == rte_get_master_lcore()) {
                    print_per_core_throughput();
                }
                next_update_tsc += update_tsc;
            }
        }

        // clean cycle for rate limiter
        if (unlikely(cur_tsc > next_clean_tsc)) {
            for (i = 0; i < NODE_NUM; i++) {
                mbuf = rte_pktmbuf_alloc(pktmbuf_pool);
                generate_request_pkt(lcore_id, mbuf, i);
                //print_packet(mbuf);
                enqueue_pkt(lcore_id, mbuf);
            }
            send_pkt_burst(lcore_id);
            next_clean_tsc += clean_tsc;
        }
    }
    return 0;
}

// initialization
static void custom_init(void) {
    // initialize per-lcore stats
    memset(&tput_stat, 0, sizeof(tput_stat));

    // initialize backend ip
    uint32_t i;
    char buffer[100];
    for (i = 0; i < NODE_NUM; i++) {
        sprintf(buffer, "192.168.12.%d", i);
        inet_pton(AF_INET, buffer, &backend_ip[i]);
    }

    printf("finish initialization\n");
    printf("==============================\n");
}

/*
 * functions for parsing arguments
 */

static void nc_parse_args_help(void) {
    printf("rate_limiter [EAL options] --\n"
        "  -t clean cycle ms (e.g., 10)\n");
}

static int nc_parse_args(int argc, char **argv) {
    int opt, num;
    double fnum;
    while ((opt = getopt(argc, argv, "t:")) != -1) {
        switch (opt) {
        case 't':
            num = atoi(optarg);
            if (num > 0 && num <= 1000) {
                clean_cycle = num;
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
    printf("parsed arguments: clean cycle: %"PRIu64
        "\n",
        clean_cycle);
    return 1;
}

/*
 * main function
 */

int main(int argc, char **argv) {
    int ret;
    uint32_t lcore_id;

    // parse default arguments
    ret = rte_eal_init(argc, argv);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "invalid EAL arguments\n");
    }
    argc -= ret;
    argv += ret;

    // parse netcache arguments
    ret = nc_parse_args(argc, argv);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "invalid netcache arguments\n");
    }

    // init
    nc_init();
    custom_init();

    // launch main loop in every lcore
    rte_eal_mp_remote_launch(nc_loop, NULL, CALL_MASTER);
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        if (rte_eal_wait_lcore(lcore_id) < 0) {
            ret = -1;
            break;
        }
    }

    return 0;
}
