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
#include <sys/socket.h>

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
#include "tommyhashdyn.h"

#define KEY_SPACE_SIZE          (1000*1000)
#define NODE_NUM                1

/*
 * key-value store
 */

uint64_t cluster_counter[NODE_NUM];
uint64_t cluster_pkts_send_ms[NODE_NUM];
uint64_t pkts_send_limit_ms;
uint64_t kv_value[VALUE_SIZE];

/*
 * key-value store
 */

struct tommy_object {
    uint64_t key;
    uint64_t value[VALUE_SIZE];
    tommy_node node;
};

int tommy_hashtable_compare(const void* void_arg, const void* void_obj)
{
    const uint64_t* arg = (const uint64_t*)void_arg;
    const struct tommy_object* obj = (const struct tommy_object*)void_obj;

    if (*arg == obj->key) {
        return 0;
    }

    return 1;
}

tommy_hashdyn kv_store;

/*
 * backend write state
 */

struct write_object {
    uint64_t key;
    struct rte_mbuf *mbuf;
    tommy_node node;
};

int tommy_hashtable_compare_write(const void* void_arg, const void* void_obj)
{
    const uint64_t* arg = (const uint64_t*)void_arg;
    const struct write_object* obj = (const struct write_object*)void_obj;

    if (*arg == obj->key) {
        return 0;
    }

    return 1;
}

tommy_hashdyn write_state;

/*
 * functions for processing
 */

// print latency
static void print_cluster(void) {
    uint32_t i, j, idx;
    idx = 0;
    for (i = 0; i < 15; i++) {
        for (j = 0; j < 10; j++) {
            if (idx >= NODE_NUM) {
                break;
            }
            printf("%"PRIu32":%"PRIu64"\t", idx, cluster_counter[idx]);
            idx++;
        }
        printf("\n");
        if (idx >= NODE_NUM) {
            break;
        }
    }
    for (i = 0; i < NODE_NUM; i++) {
        cluster_counter[i] = 0;
    }
}

// process packet at backend
static void process_packet(uint32_t lcore_id, struct rte_mbuf *mbuf) {
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    struct ipv4_hdr *ip = (struct ipv4_hdr *)((uint8_t*) eth
        + sizeof(struct ether_hdr));
    struct udp_hdr *udp = (struct udp_hdr *)((uint8_t*) ip
        + sizeof(struct ipv4_hdr));
    MessageHeader* message_header = (MessageHeader*) ((uint8_t *) eth
        + sizeof(header_template));
    uint8_t* value = (uint8_t *) eth
        + sizeof(header_template) + sizeof(MessageHeader);

    /*if (unlikely(message_header->type != TYPE_GET_REQUEST)) {
        printf("========== not supported packet type %"PRIu8"==========\n",
            message_header->type);
        rte_pktmbuf_free(mbuf);
        return;
    }*/

    // check send limit
    uint32_t backend_id = rte_be_to_cpu_64(message_header->key) % NODE_NUM;
    cluster_counter[backend_id]++;
    if (cluster_pkts_send_ms[backend_id] >= pkts_send_limit_ms) {
        rte_pktmbuf_free(mbuf);
        return;
    } else {
        cluster_pkts_send_ms[backend_id]++;
    }

    uint64_t key = rte_be_to_cpu_64(message_header->key);
    if (likely(message_header->type == TYPE_GET_REQUEST)) {
        //printf("TYPE_GET_REQUEST\n");
        message_header->type = (uint8_t) TYPE_GET_RESPONSE_B;
        struct tommy_object* obj = tommy_hashdyn_search(&kv_store,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        if (obj) {
            rte_memcpy((uint8_t *) value, (uint8_t *) (obj->value),
                VALUE_SIZE * 8);
        } else {
            printf("========== key %"PRIu64" not in store ==========\n",
                key);
        }
        ip->total_length += VALUE_SIZE * 8;
        udp->dgram_len += VALUE_SIZE * 8;
        mbuf->data_len += VALUE_SIZE * 8;
        mbuf->pkt_len += VALUE_SIZE * 8;
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_PUT_REQUEST) {
        printf("TYPE_PUT_REQUEST\n");
        message_header->type = (uint8_t) TYPE_PUT_RESPONSE_B;
        struct tommy_object* obj = tommy_hashdyn_search(&kv_store,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        if (obj) {
            rte_memcpy((uint8_t *)(obj->value), (uint8_t *)value,
                VALUE_SIZE * 8);
        }
        enqueue_pkt(lcore_id, mbuf);
    } else {
        printf("========== not supported packet type %"PRIu8"==========\n",
            message_header->type);
        rte_pktmbuf_free(mbuf);
        return;
    }

    // swap eth address
    struct ether_addr tmp_ether_addr;
    ether_addr_copy(&eth->s_addr, &tmp_ether_addr);
    ether_addr_copy(&eth->d_addr, &eth->s_addr);
    ether_addr_copy(&tmp_ether_addr, &eth->d_addr);

    // swap ip address
    uint32_t tmp_ip_addr = ip->src_addr;
    ip->src_addr = ip->dst_addr;
    ip->dst_addr = tmp_ip_addr;

    // swap udp port
    uint16_t tmp_udp_port = udp->src_port;
    udp->src_port = udp->dst_port;
    udp->dst_port = tmp_udp_port;

    // handle message
    /*message_header->type = (uint8_t) TYPE_GET_RESPONSE_B;
    rte_memcpy((uint8_t *) value, (uint8_t *) (kv_value),
        VALUE_SIZE * 8);
    
    // update metadata
    ip->total_length += VALUE_SIZE * 8;
    udp->dgram_len += VALUE_SIZE * 8;
    mbuf->data_len += VALUE_SIZE * 8;
    mbuf->pkt_len += VALUE_SIZE * 8

    // send packet out
    enqueue_pkt(lcore_id, mbuf);*/
}


// cluster loop
static int32_t nc_loop(__attribute__((unused)) void *arg) {
    uint32_t lcore_id = rte_lcore_id();
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering main loop on lcore %u mode cluster\n",
        (long long)time(NULL), lcore_id);

    struct rte_mbuf *mbuf;
    struct rte_mbuf *mbuf_burst[NC_MAX_BURST_SIZE];
    uint32_t i, j, nb_rx;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;
    uint64_t ms_tsc = rte_get_tsc_hz() / 1000 * 10;
    pkts_send_limit_ms *= 10;
    uint64_t next_ms_tsc = cur_tsc + ms_tsc;
    uint64_t drain_tsc = (rte_get_tsc_hz() + US_PER_S - 1)
        / US_PER_S * NC_DRAIN_US;
    uint64_t next_drain_tsc = cur_tsc + drain_tsc;

    while (1) {
        // read current time
        cur_tsc = rte_rdtsc();

        // print stats at master lcore
        if (update_tsc > 0) {
            if (unlikely(cur_tsc > next_update_tsc)) {
                if (lcore_id == rte_get_master_lcore()) {
                    print_per_core_throughput();
                    print_cluster();
                }
                next_update_tsc += update_tsc;
            }
        }

        // clean packet counters for each ms
        if (unlikely(cur_tsc > next_ms_tsc)) {
            for (i = 0; i < NODE_NUM; i++) {
                cluster_pkts_send_ms[i] = 0;
            }
            next_ms_tsc += ms_tsc;
        }

        // TX: send packets, drain TX queue
        if (unlikely(cur_tsc > next_drain_tsc)) {
            send_pkt_burst(lcore_id);
            next_drain_tsc += drain_tsc;
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
                process_packet(lcore_id, mbuf);
            }
        }
    }
    return 0;
}

// initialization
static void custom_init(void) {
    // initialize per-lcore stats
    memset(&tput_stat, 0, sizeof(tput_stat));
    memset(&kv_value, 0, sizeof(kv_value));
    memset(&cluster_counter, 0, sizeof(cluster_counter));
    memset(&cluster_pkts_send_ms, 0, sizeof(cluster_pkts_send_ms));

    uint64_t key;
    /*tommy_hashdyn_init(&write_state);
    for (key = 0; key < CACHE_SIZE; key++) {
        uint64_t key_hash = StringToHash(&key, 8) % KEY_SPACE_SIZE;

        struct write_object* obj = malloc(sizeof(struct write_object));
        obj->key = key_hash;
        obj->mbuf = NULL;
        if (tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write,
            &obj->key, tommy_inthash_u64(obj->key)) == NULL) {
            tommy_hashdyn_insert(&write_state, &obj->node, obj,
                tommy_inthash_u64(obj->key));
        }
    }
    printf("size: %d\n", tommy_hashdyn_count(&write_state));*/
    
    tommy_hashdyn_init(&kv_store);
    for (key = 0; key < KEY_SPACE_SIZE; key++) {
        struct tommy_object* obj = malloc(sizeof(struct tommy_object));
        obj->key = key;
        memset(&(obj->value), 0, sizeof(obj->value));
        obj->value[0] = key + 1;
        tommy_hashdyn_insert(&kv_store, &obj->node, obj,
            tommy_inthash_u64(obj->key));
    }
    printf("size: %d\n", tommy_hashdyn_count(&kv_store));


    printf("finish initialization\n");
    printf("==============================\n");
}

/*
 * functions for parsing arguments
 */

static void nc_parse_args_help(void) {
    printf("nc_cluster [EAL options] --\n"
        "  -p port mask (>0)\n"
        "  -m pkts send limit ms (e.g., 10000)\n");
}

static int nc_parse_args(int argc, char **argv) {
    int opt, num;
    double fnum;
    while ((opt = getopt(argc, argv, "p:m:")) != -1) {
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
        ", pkts send limit ms: %"PRIu64
        "\n",
        enabled_port_mask, pkts_send_limit_ms);
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
