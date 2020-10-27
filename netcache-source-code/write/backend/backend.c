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

/*
 * key-value store
 */

uint64_t cluster_counter[NODE_NUM];
uint64_t cluster_counter_effective[NODE_NUM];
uint64_t cluster_pkts_send_ms[NODE_NUM];
uint64_t pkts_send_limit_ms;
uint64_t kv_value[VALUE_SIZE];

/*
 * backend write state
 */

struct write_object {
    uint64_t key;
    //struct rte_mbuf *mbuf;
    uint64_t seq;
    uint32_t flag;
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
uint64_t write_timeout;

/*
 * functions for processing
 */

// print latency
static void print_cluster(void) {
    uint32_t i, j, idx;
    idx = 0;
    /*for (i = 0; i < 15; i++) {
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
    }*/

    printf("cluster\n");
    for (i = 0; i < NODE_NUM; i++) {
        printf("%"PRIu32"\t%"PRIu64"\t%"PRIu64"\n", i,
            cluster_counter_effective[i],
            cluster_counter[i]);
    }

    memset(&cluster_counter, 0, sizeof(cluster_counter));
    memset(&cluster_counter_effective, 0, sizeof(cluster_counter_effective));
}

// copy mbuf
static struct rte_mbuf * copy_type_write_cached(struct rte_mbuf *mbuf) {
    struct rte_mbuf *mbuf_clone = rte_pktmbuf_alloc(pktmbuf_pool);
    if (mbuf_clone == NULL) {
        return NULL;
    }

    struct ether_hdr* eth =
        rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    struct ether_hdr* eth_clone =
        rte_pktmbuf_mtod(mbuf_clone, struct ether_hdr *);
    rte_memcpy(eth_clone, eth,
        sizeof(header_template) + sizeof(MessageHeader) + VALUE_SIZE * 8);
    mbuf_clone->data_len = mbuf->data_len;
    mbuf_clone->pkt_len = mbuf->pkt_len;
    mbuf_clone->next = NULL;
    mbuf_clone->nb_segs = 1;
    mbuf_clone->ol_flags = 0;

    return mbuf_clone;
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
    //print_packet(mbuf);
    //rte_exit(EXIT_FAILURE, "invalid netcache arguments\n");

    // check send limit
    uint64_t key = rte_be_to_cpu_64(message_header->key);
    uint32_t backend_id = key % NODE_NUM;
    if (cluster_pkts_send_ms[backend_id] >= pkts_send_limit_ms) {
        rte_pktmbuf_free(mbuf);
        return;
    } else {
        cluster_pkts_send_ms[backend_id]++;
    }

    if (message_header->type != TYPE_WRITE_CACHED) {
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
    }

    // handle different types
    cluster_counter[backend_id]++;
    if (likely(message_header->type == TYPE_GET_REQUEST)) {
        cluster_counter_effective[backend_id]++;
        message_header->type = (uint8_t) TYPE_GET_RESPONSE_B;
        ip->total_length += VALUE_SIZE * 8;
        udp->dgram_len += VALUE_SIZE * 8;
        mbuf->data_len += VALUE_SIZE * 8;
        mbuf->pkt_len += VALUE_SIZE * 8;
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_PUT_REQUEST) {
        cluster_counter_effective[backend_id]++;
        message_header->type = (uint8_t) TYPE_PUT_RESPONSE_B;
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_WRITE_CACHED) {
        struct write_object* write_obj = tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj == NULL) {
            printf("========== not exist cached key %"PRIu64"\n", key);
            //print_packet(mbuf);
            rte_pktmbuf_free(mbuf);
            return;
        }

        if (write_obj->flag == 0) { // no pending write
            // store this write
            write_obj->seq = message_header->seq;
            write_obj->flag = 1;

            // update cache
            struct rte_mbuf *update_mbuf = copy_type_write_cached(mbuf);
            if (update_mbuf == NULL) { // return, if cannot clone the packet
                rte_pktmbuf_free(mbuf);
                return;
            }
            struct ether_hdr* update_eth = rte_pktmbuf_mtod(update_mbuf,
                struct ether_hdr *);
            MessageHeader* update_message_header = (MessageHeader*)
                ((uint8_t *) update_eth + sizeof(header_template));
            update_message_header->type = (uint8_t) TYPE_CACHE_UPDATE;
            enqueue_pkt(lcore_id, update_mbuf);

            // do this write and reply to receiver
            cluster_counter_effective[backend_id]++;
            
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

            message_header->type = (uint8_t) TYPE_PUT_RESPONSE_B;
            enqueue_pkt(lcore_id, mbuf);
        } else {
            // pending write timeout, redo
            if (message_header->seq - write_obj->seq
                > write_timeout) {
                message_header->type = (uint8_t) TYPE_CACHE_UPDATE;
                message_header->seq = write_obj->seq;
                enqueue_pkt(lcore_id, mbuf);
            } else { // drop this write
                rte_pktmbuf_free(mbuf);
            }
        }
    } else if (message_header->type == TYPE_CACHE_UPDATE_RESPONSE) {
        struct write_object* write_obj = tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj->flag == 1 && write_obj->seq == message_header->seq) {
            write_obj->flag == 0;
        }
        rte_pktmbuf_free(mbuf);
    } else {
        /*printf("========== not supported packet type %"PRIu8"==========\n",
            message_header->type);*/
        rte_pktmbuf_free(mbuf);
    }


    /*else if (message_header->type == TYPE_WRITE_CACHED) {
        //print_packet(mbuf);
        //rte_exit(EXIT_FAILURE, "invalid EAL arguments\n");
        struct write_object* write_obj = tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj == NULL) {
            printf("========== not exist cached key %"PRIu64"\n", key);
            //print_packet(mbuf);
            rte_pktmbuf_free(mbuf);
            //rte_exit(EXIT_FAILURE, "uncached key\n");
            return;
        }

        if (write_obj->mbuf == NULL) {
            // if no pending write, store this write
            struct rte_mbuf *mbuf_clone = copy_type_write_cached(mbuf);
            if (mbuf_clone == NULL) { // return, if cannot clone the packet
                write_obj->mbuf = mbuf;
                return;
            }
            write_obj->mbuf = mbuf_clone;
        } else {
            struct ether_hdr* eth_clone = rte_pktmbuf_mtod(write_obj->mbuf,
                struct ether_hdr *);
            MessageHeader* message_header_clone = (MessageHeader*)
                ((uint8_t *) eth_clone + sizeof(header_template));
            message_header->seq = message_header_clone->seq;
        }

        message_header->type = (uint8_t) TYPE_CACHE_UPDATE;
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_CACHE_UPDATE_RESPONSE) {
        struct write_object* write_obj = tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj->mbuf != NULL) {
            struct ether_hdr* eth_clone = rte_pktmbuf_mtod(write_obj->mbuf,
                struct ether_hdr *);
            MessageHeader* message_header_clone = (MessageHeader*)
                ((uint8_t *) eth_clone + sizeof(header_template));
            if (message_header_clone->seq == message_header->seq) {
                rte_pktmbuf_free(write_obj->mbuf);
                write_obj->mbuf = NULL;
                message_header->type = (uint8_t) TYPE_PUT_RESPONSE_C;
                cluster_counter_effective[backend_id]++;
                enqueue_pkt(lcore_id, mbuf);
            } else {
                rte_pktmbuf_free(mbuf);
            }
        } else {
            rte_pktmbuf_free(mbuf);
        }
    } else {
        printf("========== not supported packet type %"PRIu8"==========\n",
            message_header->type);
        rte_pktmbuf_free(mbuf);
    }*/

    /*if (message_header->type == TYPE_WRITE_CACHED) {
        struct write_object* write_obj = tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj == NULL) {
            printf("========== not exist cached key %"PRIu64"\n", key);
            //print_packet(mbuf);
            rte_pktmbuf_free(mbuf);
            return;
        }

        if (write_obj->mbuf == NULL) { // no pending write
            // store this write
            struct rte_mbuf *queue_mbuf = copy_type_write_cached(mbuf);
            if (queue_mbuf == NULL) {
                rte_pktmbuf_free(mbuf);
                return;
            }
            write_obj->mbuf = queue_mbuf;

            // update cache
            struct rte_mbuf *update_mbuf = copy_type_write_cached(mbuf);
            if (update_mbuf == NULL) { // return, if cannot clone the packet
                rte_pktmbuf_free(mbuf);
                return;
            }
            struct ether_hdr* update_eth = rte_pktmbuf_mtod(update_mbuf,
                struct ether_hdr *);
            MessageHeader* update_message_header = (MessageHeader*)
                ((uint8_t *) update_eth + sizeof(header_template));
            update_message_header->type = (uint8_t) TYPE_CACHE_UPDATE;
            enqueue_pkt(lcore_id, update_mbuf);

            // do this write and reply to receiver
            cluster_counter_effective[backend_id]++;
            
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

            message_header->type = (uint8_t) TYPE_PUT_RESPONSE_B;
            enqueue_pkt(lcore_id, mbuf);
        } else {

            struct ether_hdr* queue_eth = rte_pktmbuf_mtod(write_obj->mbuf,
                struct ether_hdr *);
            MessageHeader* queue_message_header = (MessageHeader*)
                ((uint8_t *) queue_eth + sizeof(header_template));

            // pending write timeout, redo
            if (message_header->seq - queue_message_header->seq
                > write_timeout) {
                struct rte_mbuf *update_mbuf
                    = copy_type_write_cached(write_obj->mbuf);
                struct ether_hdr* update_eth = rte_pktmbuf_mtod(update_mbuf,
                    struct ether_hdr *);
                MessageHeader* update_message_header = (MessageHeader*)
                    ((uint8_t *) update_eth + sizeof(header_template));
                update_message_header->type = (uint8_t) TYPE_CACHE_UPDATE;
                enqueue_pkt(lcore_id, update_mbuf);
            }
            
            // drop this write
            rte_pktmbuf_free(mbuf);
        }
    } else if (message_header->type == TYPE_CACHE_UPDATE_RESPONSE) {
        struct write_object* write_obj = tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj->mbuf != NULL) {
            rte_pktmbuf_free(write_obj->mbuf);
        }
        rte_pktmbuf_free(mbuf);
    } else {
        printf("========== not supported packet type %"PRIu8"==========\n",
            message_header->type);
        rte_pktmbuf_free(mbuf);
    }*/

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
    memset(&cluster_counter_effective, 0, sizeof(cluster_counter_effective));
    memset(&cluster_pkts_send_ms, 0, sizeof(cluster_pkts_send_ms));

    uint64_t key;
    tommy_hashdyn_init(&write_state);
    for (key = 0; key < CACHE_SIZE; key++) {
        uint64_t key_hash = StringToHash(&key, 8) % KEY_SPACE_SIZE;

        struct write_object* obj = malloc(sizeof(struct write_object));
        obj->key = key_hash;
        //obj->mbuf = NULL;
        obj->seq = 0;
        obj->flag = 0;
        if (tommy_hashdyn_search(&write_state,
            tommy_hashtable_compare_write,
            &obj->key, tommy_inthash_u64(obj->key)) == NULL) {
            tommy_hashdyn_insert(&write_state, &obj->node, obj,
                tommy_inthash_u64(obj->key));
        }
    }
    printf("size: %d\n", tommy_hashdyn_count(&write_state));

    write_timeout = rte_get_tsc_hz();

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
