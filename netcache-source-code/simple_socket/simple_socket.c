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

uint32_t mode = 0; // 0 is sender, 1 is receiver

/*
 * functions for processing
 */

// generate request packet TODO
static void generate_request_pkt(uint32_t lcore_id, struct rte_mbuf *mbuf) {
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    assert(mbuf != NULL);

    // init packet header
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    rte_memcpy(eth, header_template, sizeof(header_template));
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->pkt_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->next = NULL;
    mbuf->nb_segs = 1;
    mbuf->ol_flags = 0;

    MessageHeader* message_header = (MessageHeader*) ((uint8_t*)eth + sizeof(header_template));

    // type
    message_header->type = (uint8_t) TYPE_GET_REQUEST;

    // key
    message_header->key = rte_cpu_to_be_64(0);

    // seq: record generation time for latency measurement
    message_header->seq = rte_rdtsc();
}

// TX loop for test
static int32_t nc_test_tx_loop(__attribute__((unused)) void *arg) {
    uint32_t lcore_id = rte_lcore_id();
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering main loop on lcore %u mode TX\n", (long long)time(NULL), lcore_id);

    struct rte_mbuf *mbuf;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;
    uint64_t ms_tsc = rte_get_tsc_hz() / 1000;
    uint64_t next_ms_tsc = cur_tsc + ms_tsc;
    uint64_t drain_tsc = (rte_get_tsc_hz() + US_PER_S - 1) / US_PER_S * 100;
    uint64_t next_drain_tsc = cur_tsc + drain_tsc;
    uint64_t pkts_send_ms = 0;
    uint64_t pkts_send_limit_ms = 100000;

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

        // clean packet counters for each ms
        if (unlikely(cur_tsc > next_ms_tsc)) {
            pkts_send_ms = 0;
            next_ms_tsc += ms_tsc;
        }

        // TX: send packets, drain TX queue
        if (unlikely(cur_tsc > next_drain_tsc)) {
            send_pkt_burst(lcore_id);
            next_drain_tsc += drain_tsc;
        }

        // TX: generate packet, put in TX queue
        if (pkts_send_ms < pkts_send_limit_ms) {
            mbuf = rte_pktmbuf_alloc(pktmbuf_pool);
            generate_request_pkt(lcore_id, mbuf);
            enqueue_pkt(lcore_id, mbuf);
            pkts_send_ms++;
        }
    }
    return 0;
}

// RX loop for test
static int32_t nc_test_rx_loop(__attribute__((unused)) void *arg) {
    uint32_t lcore_id = rte_lcore_id();
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering main loop on lcore %u mode RX\n", (long long)time(NULL), lcore_id);

    struct rte_mbuf *mbuf;
    struct rte_mbuf *mbuf_burst[NC_MAX_BURST_SIZE];
    uint32_t i, j, nb_rx;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;

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

        // RX
        for (i = 0; i < lconf->n_rx_queue; i++) {
            nb_rx = rte_eth_rx_burst(lconf->port, lconf->rx_queue_list[i], 
                   mbuf_burst, NC_MAX_BURST_SIZE);
            tput_stat[lconf->vid].rx += nb_rx;
            for (j = 0; j < nb_rx; j++) {
                mbuf = mbuf_burst[j];
                rte_prefetch0(rte_pktmbuf_mtod(mbuf, void *));
                rte_pktmbuf_free(mbuf);
            }
        }
    }
    return 0;
}

// initialization
static void custom_init(void) {
    // initialize per-lcore stats
    memset(&tput_stat, 0, sizeof(tput_stat));

    printf("finish initialization\n");
    printf("==============================\n");
}

/*
 * functions for parsing arguments
 */

static void nc_parse_args_help(void) {
    printf("simple_socket [EAL options] --\n"
        "  -m mode (0:TX 1:RX)\n"
        "  -p port mask (>0)\n");
}

static int nc_parse_args(int argc, char **argv) {
    int opt, num;
    double fnum;
    while ((opt = getopt(argc, argv, "m:p:w:z:s:")) != -1) {
        switch (opt) {
        case 'm':
            num = atoi(optarg);
            if (num >= 0 && num <= 1) {
                mode = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        case 'p':
            num = atoi(optarg);
            if (num > 0) {
                enabled_port_mask = num;
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
        ", port mask: %"PRIu32
        "\n",
        mode, enabled_port_mask);
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
    switch (mode) {
    case 0:
        rte_eal_mp_remote_launch(nc_test_tx_loop, NULL, CALL_MASTER);
        break;
    case 1:
        rte_eal_mp_remote_launch(nc_test_rx_loop, NULL, CALL_MASTER);
        break;
    default:
        rte_exit(EXIT_FAILURE, "invalid mode\n");
    }
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        if (rte_eal_wait_lcore(lcore_id) < 0) {
            ret = -1;
            break;
        }
    }

    return 0;
}
