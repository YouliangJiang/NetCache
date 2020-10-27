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
#include <pthread.h>

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
#include "key_hash.h"
#include "zipf.h"

/*
 * constants
 */
#define RATE_ADJUST_PER_SEC     5
#define MIN_LOSS_RATE           0.01
#define MAX_LOSS_RATE           0.02
#define PKTS_SEND_LIMIT_MIN_MS  10
#define PKTS_SEND_LIMIT_MAX_MS  30000

#define KEY_SPACE_SIZE          (1000*1000)

/*
 * custom types
 */

struct latency_statistics {
    uint64_t max;
    uint64_t num;
    uint64_t total;
    uint64_t overflow;
    uint64_t bin[BIN_SIZE];
} __rte_cache_aligned;

/*
 * global variables
 */

// key-value workload generation
uint32_t zipf_alpha = 0;
uint32_t write_ratio = 0;
uint64_t pkts_send_limit_ms = 0;
uint32_t backend_ip[NODE_NUM];
uint64_t kv_value[VALUE_SIZE];

// generate packets with trace
uint64_t *kv_trace_master;
uint32_t kv_trace_length = 0;
uint32_t kv_trace_p = 0;

// statistics
struct latency_statistics latency_stat_c[NC_MAX_LCORES];
struct latency_statistics latency_stat_b[NC_MAX_LCORES];

// adjust client rate
uint32_t adjust_start = 0;
uint64_t last_sent = 0;
uint64_t last_recv = 0;

/*
 * functions for processing
 */

// print latency
static void print_latency(struct latency_statistics * latency_stat) {
    uint64_t max = 0;
    uint64_t num = 0;
    uint64_t total = 0;
    uint64_t overflow = 0;
    uint64_t bin[BIN_SIZE];
    memset(&bin, 0, sizeof(bin));

    uint32_t i, j;
    for (i = 0; i < n_lcores; i++) {
        if(max < latency_stat[i].max) {
            max = latency_stat[i].max;
        }
        num += latency_stat[i].num;
        total += latency_stat[i].total;
        overflow += latency_stat[i].overflow;
        for (j = 0; j < BIN_SIZE; j++) {
            bin[j] += latency_stat[i].bin[j];
            latency_stat[i].bin[j] = 0;
        }
        latency_stat[i].max = 0;
        latency_stat[i].num = 0;
        latency_stat[i].total = 0;
        latency_stat[i].overflow = 0;
    }

    double average_latency = 0;
    if (num > 0) {
        average_latency = total / (double)num;
    }
    printf("\tcount: %"PRIu64"\t"
        "average latency: %.1f us\t"
        "max latency: %"PRIu64" us\t"
        "overflow: %"PRIu64"\n",
        num, average_latency, max, overflow);
}

// generate request pkt based on trace
static void generate_request_pkt_trace(uint32_t lcore_id, struct rte_mbuf *mbuf,
    uint32_t cur_kv_trace_p, uint64_t* kv_trace, uint32_t kv_trace_idx) {
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    assert(mbuf != NULL);

    // init packet header
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    struct ipv4_hdr *ip = (struct ipv4_hdr *)((uint8_t*) eth
        + sizeof(struct ether_hdr));
    struct udp_hdr *udp = (struct udp_hdr *)((uint8_t*) ip
        + sizeof(struct ipv4_hdr));
    rte_memcpy(eth, header_template, sizeof(header_template));
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->pkt_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->next = NULL;
    mbuf->nb_segs = 1;
    mbuf->ol_flags = 0;

    MessageHeader* message_header = (MessageHeader*)
        ((uint8_t*)eth + sizeof(header_template));

    // type
    if(unlikely(rte_rand() % 100 < write_ratio)) {
        message_header->type = (uint8_t) TYPE_PUT_REQUEST;

        uint8_t* value = (uint8_t *) message_header + sizeof(MessageHeader);
        rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);

        ip->total_length += VALUE_SIZE * 8;
        udp->dgram_len += VALUE_SIZE * 8;
        mbuf->data_len += VALUE_SIZE * 8;
        mbuf->pkt_len += VALUE_SIZE * 8;
    } else {
        message_header->type = (uint8_t) TYPE_GET_REQUEST;
    }

    // key
    uint64_t key = 0;
    if (zipf_alpha == 0
        || rte_rand() % 100 > kv_trace_p) {
            key = rte_rand() % KEY_SPACE_SIZE;
    } else {
        key = kv_trace[kv_trace_idx];
        kv_trace_idx++;
        if (unlikely(kv_trace_idx == kv_trace_length)) {
            kv_trace_idx = 0;
        }
    }
    //uint64_t key_hash = StringToHash(&key, 8) % KEY_SPACE_SIZE;
    uint64_t key_hash = rte_rand() % KEY_SPACE_SIZE;


    /*
    uint64_t key = StringToHash(&zipf_key, 8) % KEY_SPACE_SIZE;*/

    message_header->key = rte_cpu_to_be_64(key_hash);
    message_header->keyy = rte_cpu_to_be_64(0);

    // update dst ip
    ip->dst_addr = backend_ip[key % NODE_NUM];

    // seq: record generation time for latency measurement
    message_header->seq = rte_rdtsc();
}

// update send limit
static void update_send_limit(void) {
    uint32_t i;

    uint64_t cur_sent = 0;
    uint64_t cur_recv = 0;
    for (i = 0; i < n_lcores; i++) {
        cur_sent += tput_stat[i].tx;
        cur_recv += tput_stat[i].rx;
    }

    uint64_t net_sent = cur_sent - last_sent;
    uint64_t net_recv = cur_recv - last_recv;
    last_sent = cur_sent;
    last_recv = cur_recv;
    double loss_rate = 0;
    if (net_sent > net_recv) {
        loss_rate = (net_sent - net_recv) / (double) net_sent;
    }

    uint64_t cur_pkts_send_ms = pkts_send_limit_ms;//net_sent * RATE_ADJUST_PER_SEC / 1000;
    if (loss_rate < MIN_LOSS_RATE) { // increase
        if (unlikely(adjust_start == 0)) {
            pkts_send_limit_ms = cur_pkts_send_ms * 2;
        } else {
            pkts_send_limit_ms = cur_pkts_send_ms * 1.1;
        }
        /*if (net_sent * RATE_ADJUST_PER_SEC / 1000 * 1.2
            > pkts_send_limit_ms) {
            if (unlikely(adjust_start == 0)) {
                pkts_send_limit_ms = pkts_send_limit_ms * 2;
            } else {
                pkts_send_limit_ms = pkts_send_limit_ms * 1.1;
            }
        }*/
    } else if (loss_rate > MAX_LOSS_RATE) { // decrease
        if (unlikely(adjust_start == 0)) {
            pkts_send_limit_ms = cur_pkts_send_ms / 2;
            adjust_start = 1;
        } else {
            pkts_send_limit_ms = cur_pkts_send_ms * 0.9;
        }
    }

    if (unlikely(pkts_send_limit_ms < PKTS_SEND_LIMIT_MIN_MS)) {
        pkts_send_limit_ms = PKTS_SEND_LIMIT_MIN_MS;
    }

    if (unlikely(pkts_send_limit_ms > PKTS_SEND_LIMIT_MAX_MS)) {
        pkts_send_limit_ms = PKTS_SEND_LIMIT_MAX_MS;
    }

    /*printf("\tnet_sent: %"PRIu64"\t"
        "net_recv: %"PRIu64"\t"
        "pkts_send_limit_ms: %"PRIu64"\t"
        "loss_rate: %.3f\t"
        "adjust_start: %"PRIu32"\n",
        net_sent, net_recv,
        pkts_send_limit_ms, loss_rate, adjust_start);*/
}

static void compute_latency(struct latency_statistics *latency_stat,
    uint64_t latency) {
    latency_stat->num++;
    latency_stat->total += latency;
    if(latency_stat->max < latency) {
        latency_stat->max = latency;
    }
    if(latency < BIN_MAX) {
        latency_stat->bin[latency/BIN_RANGE]++;
    } else {
        latency_stat->overflow++;
    }
}

// process packet client
static void process_packet_client(uint32_t lcore_id, struct rte_mbuf *mbuf) {
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    MessageHeader* message_header = (MessageHeader*)
        ((uint8_t *) eth + sizeof(header_template));

    uint64_t latency = timediff_in_us(rte_rdtsc(), message_header->seq);
    if (message_header->type == TYPE_GET_RESPONSE_C) {
        tput_stat[lconf->vid].rx_c += 1;
        compute_latency(&latency_stat_c[lconf->vid], latency);
    } else /*if (message_header->type == TYPE_GET_RESPONSE_B)*/ {
        compute_latency(&latency_stat_b[lconf->vid], latency);
    } /*else {
        rte_exit(EXIT_FAILURE, "unsupported response packet type\n");
    }*/
    rte_pktmbuf_free(mbuf);
}

// TX loop
static void nc_client_tx_loop(uint32_t lcore_id) {
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering TX loop on lcore %u\n",
        (long long)time(NULL),
        lcore_id);

    struct rte_mbuf *mbuf;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;

    uint64_t ms_tsc = rte_get_tsc_hz() / 1000;
    uint64_t next_ms_tsc = cur_tsc + ms_tsc;
    uint64_t drain_tsc = (rte_get_tsc_hz() + US_PER_S - 1) / US_PER_S * NC_DRAIN_US;
    uint64_t next_drain_tsc = cur_tsc + drain_tsc;
    uint64_t pkts_send_ms = 0;
    uint32_t cur_kv_trace_p = 0;
    uint32_t kv_trace_idx = 0;
    uint64_t* kv_trace = malloc(TRACE_LENGTH * sizeof(uint64_t));
    rte_memcpy(kv_trace, kv_trace_master, TRACE_LENGTH * sizeof(uint64_t));

    while (1) {
        // read current time
        cur_tsc = rte_rdtsc();

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
            generate_request_pkt_trace(lcore_id, mbuf,
                cur_kv_trace_p, kv_trace, kv_trace_idx);
            cur_kv_trace_p = (cur_kv_trace_p + 1) % 100;
            //print_packet(mbuf);
            enqueue_pkt(lcore_id, mbuf);
            pkts_send_ms++;
        }
    }
}

// RX loop
static void nc_client_rx_loop(uint32_t lcore_id) {
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering RX loop on lcore %u\n",
        (long long)time(NULL),
        lcore_id);

    struct rte_mbuf *mbuf;
    struct rte_mbuf *mbuf_burst[NC_MAX_BURST_SIZE];
    uint32_t i, j, nb_rx;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;
    uint64_t adjust_tsc = rte_get_tsc_hz() / RATE_ADJUST_PER_SEC;
    uint64_t next_adjust_tsc = cur_tsc + adjust_tsc;

    while (1) {
        // read current time
        cur_tsc = rte_rdtsc();

        // print stats at master lcore
        if (update_tsc > 0) {
            if (unlikely(cur_tsc > next_update_tsc)) {
                print_per_core_throughput();
                printf("latency\n");
                print_latency(latency_stat_c);
                print_latency(latency_stat_b);
                next_update_tsc += update_tsc;
            }
        }

        // adjust send limit at master lcore
        if (unlikely(cur_tsc > next_adjust_tsc)) {
            //update_send_limit();
            next_adjust_tsc += adjust_tsc;
        }

        // RX
        for (i = 0; i < lconf->n_rx_queue; i++) {
            nb_rx = rte_eth_rx_burst(lconf->port, lconf->rx_queue_list[i], 
                   mbuf_burst, NC_MAX_BURST_SIZE);
            tput_stat[lconf->vid].rx += nb_rx;
            for (j = 0; j < nb_rx; j++) {
                mbuf = mbuf_burst[j];
                rte_prefetch0(rte_pktmbuf_mtod(mbuf, void *));
                //print_packet(mbuf);
                process_packet_client(lcore_id, mbuf);
            }
        }
    }
}

// main processing loop for client
static int32_t nc_client_loop(__attribute__((unused)) void *arg) {
    uint32_t lcore_id = rte_lcore_id();
    if (lcore_id == 0) {
        nc_client_rx_loop(lcore_id);
    } else {
        nc_client_tx_loop(lcore_id);
    }
    return 0;
}

// initialization
static void custom_init_trace(void) {
    int i;
    char buffer[100];

    // initialize per-lcore stats
    memset(&tput_stat, 0, sizeof(tput_stat));
    memset(&latency_stat_c, 0, sizeof(latency_stat_c));
    memset(&latency_stat_b, 0, sizeof(latency_stat_b));

    // initialize trace
    kv_trace_master = malloc(TRACE_LENGTH * sizeof(uint64_t));
    if (zipf_alpha > 0) {
        sprintf(buffer, "trace_zipf_%"PRIu32"_key_1M_len_10M", zipf_alpha);
        FILE *file = fopen(buffer, "r");
        if (file) {
            int key;
            float p;

            // scan trace
            for (i = 0; i < TRACE_LENGTH; i++) {
                if(fscanf(file, "%d", &key) != EOF) {
                    kv_trace_master[i] = key;
                } else {
                    rte_exit(EXIT_FAILURE, "trace file ends unexpectedly\n");
                }
            }

            // scan probablity
            if(fscanf(file, "%d %f", &key, &p) == EOF) {
                rte_exit(EXIT_FAILURE, "trace file ends unexpectedly\n");
            }
            fclose(file);
        } else {
            rte_exit(EXIT_FAILURE, "trace file does not exist\n");
        }
    }
    kv_trace_length = TRACE_LENGTH;

    // initialize backend ip
    for (i = 0; i < NODE_NUM; i++) {
        sprintf(buffer, "192.168.12.%d", i);
        inet_pton(AF_INET, buffer, &backend_ip[i]);
    }
    
    printf("target rate: %"PRIu64
        "\ttrace length: %"PRIu32
        "\ttrace probability: %"PRIu32
        "\n",
        pkts_send_limit_ms, kv_trace_length, kv_trace_p);
    printf("finish initialization\n");
    printf("==============================\n");
}

/*
 * functions for parsing arguments
 */

static void nc_parse_args_help(void) {
    printf("client [EAL options] --\n"
        "  -p port mask (>0)\n"
        "  -w write ratio ([0, 100])\n"
        "  -z zipf alpha (e.g., 0, 90, 95, 99)\n"
        "  -k kv trace probability ([0, 100])\n"
        "  -m pkts send limit ms (e.g., 10000)\n");
}

static int nc_parse_args(int argc, char **argv) {
    int opt, num;
    while ((opt = getopt(argc, argv, "p:w:z:k:m:")) != -1) {
        switch (opt) {
        case 'p':
            num = atoi(optarg);
            if (num > 0) {
                enabled_port_mask = num;
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
        case 'z':
            num = atoi(optarg);
            if (num >= 0 && num < 100) {
                zipf_alpha = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        case 'k':
            num = atoi(optarg);
            if (num >= 0 && num <= 100) {
                kv_trace_p = num;
            } else {
                nc_parse_args_help();
                return -1;
            }
            break;
        case 'm':
            num = atoi(optarg);
            if (num >= 0 && num <= 100000) {
                pkts_send_limit_ms = num;
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
    printf("parsed arguments: port mask: %"PRIu32
        ", write ratio: %"PRIu32
        ", zipf alpha: %"PRIu32
        ", kv trace probability: %"PRIu32
        ", pkts send limit ms: %"PRIu64
        "\n",
        enabled_port_mask, write_ratio, zipf_alpha,
        kv_trace_p, pkts_send_limit_ms);
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
    custom_init_trace();

    // launch main loop in every lcore
    rte_eal_mp_remote_launch(nc_client_loop, NULL, CALL_MASTER);
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        if (rte_eal_wait_lcore(lcore_id) < 0) {
            ret = -1;
            break;
        }
    }

    return 0;
}
