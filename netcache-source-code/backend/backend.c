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
#include "key_hash.h"
#include "tommyhashdyn.h"

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

uint32_t backend_id = 0;
tommy_hashdyn kv_store[NC_MAX_LCORES];
uint64_t kv_value[VALUE_SIZE];

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

tommy_hashdyn write_state[NC_MAX_LCORES];

/*
 * functions for processing
 */

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
        sizeof(header_template) + sizeof(MessageHeader) + 1 + VALUE_SIZE * 8);
    mbuf_clone->data_len = mbuf->data_len;
    mbuf_clone->pkt_len = mbuf->pkt_len;
    mbuf_clone->next = NULL;
    mbuf_clone->nb_segs = 1;
    mbuf_clone->ol_flags = 0;

    return mbuf_clone;
}

// process packet at backend
static void process_packet_backend(uint32_t lcore_id, struct rte_mbuf *mbuf) {
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    struct ipv4_hdr *ip = (struct ipv4_hdr *)((uint8_t*) eth
        + sizeof(struct ether_hdr));
    struct udp_hdr *udp = (struct udp_hdr *)((uint8_t*) ip
        + sizeof(struct ipv4_hdr));
    MessageHeader* message_header = (MessageHeader*) ((uint8_t *) eth
        + sizeof(header_template));
    uint8_t* value = (uint8_t *) eth
        + sizeof(header_template) + sizeof(MessageHeader);

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
    tommy_hashdyn *kvs = &kv_store[0];
    tommy_hashdyn *ws = &write_state[0];
    uint64_t key = rte_be_to_cpu_64(message_header->key);
    if (likely(message_header->type == TYPE_GET_REQUEST)) {
        //printf("TYPE_GET_REQUEST\n");
        message_header->type = (uint8_t) TYPE_GET_RESPONSE_B;
        /*struct tommy_object* obj = tommy_hashdyn_search(kvs,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        /*if (obj) {
            rte_memcpy((uint8_t *) value, (uint8_t *) (obj->value),
                VALUE_SIZE * 8);
        } else {
            rte_memcpy((uint8_t *) value, (uint8_t *) (kv_value),
                VALUE_SIZE * 8);
            //printf("========== key %"PRIu64" not in store ==========\n",
                key);
        }*/
        ip->total_length += VALUE_SIZE * 8;
        udp->dgram_len += VALUE_SIZE * 8;
        mbuf->data_len += VALUE_SIZE * 8;
        mbuf->pkt_len += VALUE_SIZE * 8;
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_PUT_REQUEST) {
        //printf("TYPE_PUT_REQUEST\n");
        message_header->type = (uint8_t) TYPE_PUT_RESPONSE_B;
        /*struct tommy_object* obj = tommy_hashdyn_search(kvs,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        if (obj) {
            rte_memcpy((uint8_t *)(obj->value), (uint8_t *)value,
                VALUE_SIZE * 8);
        }*/
        
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_WRITE_CACHED) {
        //printf("TYPE_WRITE_CACHED\n");
        struct tommy_object* obj = tommy_hashdyn_search(kvs,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        if (obj) {
            rte_memcpy((uint8_t *)(obj->value), (uint8_t *)value,
                VALUE_SIZE * 8);
        }

        struct write_object* write_obj = tommy_hashdyn_search(ws,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj == NULL) {
            //printf("========== not exist cached key %"PRIu64"\n", key);
            //print_packet(mbuf);
            rte_pktmbuf_free(mbuf);
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
        uint8_t* bitmap1 = (uint8_t *) eth
            + sizeof(header_template) + sizeof(MessageHeader);
        uint8_t* value1 = bitmap1 + 1;
        uint8_t* bitmap2 = value1 + VALUE_SIZE / 2 * 8;
        uint8_t* value2 = bitmap2 + 1;

        *bitmap1 = 0x0f;
        rte_memcpy(value1, (uint8_t *) (kv_value), VALUE_SIZE / 2 * 8);
        *bitmap2 = 0x0f;
        rte_memcpy(value2, (uint8_t *) (kv_value), VALUE_SIZE / 2 * 8);
        ip->total_length += 1;
        udp->dgram_len += 1;
        mbuf->data_len += 1;
        mbuf->pkt_len += 1;
        enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_CACHE_UPDATE_RESPONSE) {
        //printf("TYPE_CACHE_UPDATE_RESPONSE\n");

        struct write_object* write_obj = tommy_hashdyn_search(ws,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj->mbuf != NULL) {
            struct ether_hdr* eth_clone = rte_pktmbuf_mtod(write_obj->mbuf,
                struct ether_hdr *);
            MessageHeader* message_header_clone = (MessageHeader*)
                ((uint8_t *) eth_clone + sizeof(header_template));
            if (message_header_clone->seq == message_header->seq) {
                message_header->type = (uint8_t) TYPE_PUT_RESPONSE_C;
                rte_pktmbuf_free(write_obj->mbuf);
                write_obj->mbuf = NULL;
                enqueue_pkt(lcore_id, mbuf);
            }
        }
        rte_pktmbuf_free(mbuf);
    } else {
        printf("========== not supported packet type %"PRIu8"==========\n",
            message_header->type);
        rte_pktmbuf_free(mbuf);
    }
}


// backend loop
static int32_t nc_backend_loop(__attribute__((unused)) void *arg) {
    uint32_t lcore_id = rte_lcore_id();
    struct lcore_configuration *lconf = &lcore_conf[lcore_id];
    printf("%lld entering main loop on lcore %u mode RX\n",
        (long long)time(NULL), lcore_id);

    struct rte_mbuf *mbuf;
    struct rte_mbuf *mbuf_burst[NC_MAX_BURST_SIZE];
    uint32_t i, j, nb_rx;

    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz(); // in second
    uint64_t next_update_tsc = cur_tsc + update_tsc;
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
                }
                next_update_tsc += update_tsc;
            }
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
                process_packet_backend(lcore_id, mbuf);
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

     // initialize hash table
    uint32_t i;
    uint64_t key;
    char buffer[100];
    for (i = 0; i < n_lcores; i++) {

        tommy_hashdyn_init(&kv_store[i]);
        for (key = 0; key < PER_NODE_KEY_SPACE_SIZE / 10; key++) {
            struct tommy_object* obj = malloc(sizeof(struct tommy_object));
            obj->key = key * NODE_NUM + backend_id;
            memset(&(obj->value), 0, sizeof(obj->value));
            obj->value[0] = key + 1;
            tommy_hashdyn_insert(&kv_store[i], &obj->node, obj,
                tommy_inthash_u64(obj->key));
        }

        tommy_hashdyn_init(&write_state[i]);
        for (key = 0; key < CACHE_SIZE; key++) {
            sprintf(buffer, "%"PRIu64, key);
            uint64_t key_hash = StringToHash(buffer, strlen(buffer))
                % KEY_SPACE_SIZE;

            struct write_object* obj = malloc(sizeof(struct write_object));
            obj->key = key_hash;
            obj->mbuf = NULL;
            if (tommy_hashdyn_search(&write_state[i],
                tommy_hashtable_compare_write,
                &obj->key, tommy_inthash_u64(obj->key)) == NULL) {
                tommy_hashdyn_insert(&write_state[i], &obj->node, obj,
                    tommy_inthash_u64(obj->key));
            }
        }
        printf("size: %d\n", tommy_hashdyn_count(&write_state[i]));
    }

    printf("finish initialization\n");
    printf("==============================\n");
}

/*
 * functions for parsing arguments
 */

static void nc_parse_args_help(void) {
    printf("nc_client [EAL options] --\n"
        "  -p port mask (>0)\n"
        "  -n backend id ([0, 127]\n");
}

static int nc_parse_args(int argc, char **argv) {
    int opt, num;
    double fnum;
    while ((opt = getopt(argc, argv, "p:n:")) != -1) {
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
        case 'n':
            num = atoi(optarg);
            if (num >= 0 && num <= 127) {
                backend_id = num;
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
        ", backend id: %"PRIu32
        "\n",
        enabled_port_mask, backend_id);
    return 1;
}

/*
 * functions for testing write
 */

static void benchmark(void) {
    uint64_t size = 2*1000*1000;
    //struct tommy_object objects[1000*100];
    uint64_t batch = 10*1000*1000;
    uint64_t i;
    uint64_t update_value[VALUE_SIZE];
    memset(&update_value, 0, sizeof(update_value));

    // init
    tommy_hashdyn kvs;
    tommy_hashdyn_init(&kvs);
    for(i = 0; i < size; i++) {
        struct tommy_object* obj = malloc(sizeof(struct tommy_object));
        //rte_malloc(NULL, sizeof(struct tommy_object), RTE_CACHE_LINE_SIZE);
        obj->key = i;
        memset(&(obj->value), 0, sizeof(obj->value));
        obj->value[0] = i + 1;
        tommy_hashdyn_insert(&kvs, &obj->node, obj,
            tommy_inthash_u64(obj->key));
    }

    while(1) {

    // read
    uint64_t cur_tsc = rte_rdtsc();
    uint64_t update_tsc = rte_get_tsc_hz();
    for (i = 0; i < batch; i++) {
        struct tommy_object* obj = tommy_hashdyn_search(&kvs,
            tommy_hashtable_compare, &i, tommy_inthash_u64(i));
        /*if (obj) {
            printf("value %"PRIu64"\n", obj->value[0]);
        }*/
    }
    uint64_t elapse = rte_rdtsc() - cur_tsc;
    printf("read  time %"PRIu64" %"PRIu64" %"PRIu64"\n", elapse, update_tsc, elapse /
        update_tsc);

    // write
    cur_tsc = rte_rdtsc();
    for (i = 0; i < batch; i++) {
        struct tommy_object* obj = tommy_hashdyn_search(&kvs,
            tommy_hashtable_compare, &i, tommy_inthash_u64(i));
        if (obj) {
            //printf("value %"PRIu64"\n", obj->value[0]);
            rte_memcpy((uint8_t *)(obj->value), (uint8_t *)update_value,
                VALUE_SIZE * 8);
        }
    }
    elapse = rte_rdtsc() - cur_tsc;
    printf("write time %"PRIu64" %"PRIu64" %"PRIu64"\n", elapse, update_tsc, elapse /
        update_tsc);
}
}

static void process_packet_test(struct rte_mbuf *mbuf) {
    struct ether_hdr* eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    struct ipv4_hdr *ip = (struct ipv4_hdr *)((uint8_t*) eth
        + sizeof(struct ether_hdr));
    struct udp_hdr *udp = (struct udp_hdr *)((uint8_t*) ip
        + sizeof(struct ipv4_hdr));
    MessageHeader* message_header = (MessageHeader*) ((uint8_t *) eth
        + sizeof(header_template));
    uint8_t* value = (uint8_t *) eth
        + sizeof(header_template) + sizeof(MessageHeader);

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

    // handle different types
    tommy_hashdyn *kvs = &kv_store[0];
    tommy_hashdyn *ws = &write_state[0];
    uint64_t key = rte_be_to_cpu_64(message_header->key);
    if (likely(message_header->type == TYPE_GET_REQUEST)) {
        printf("TYPE_GET_REQUEST\n");
        message_header->type = (uint8_t) TYPE_GET_RESPONSE_B;
        /*struct tommy_object* obj = tommy_hashdyn_search(kvs,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        /*if (obj) {
            rte_memcpy((uint8_t *) value, (uint8_t *) (obj->value),
                VALUE_SIZE * 8);
        } else {
            rte_memcpy((uint8_t *) value, (uint8_t *) (kv_value),
                VALUE_SIZE * 8);
            //printf("========== key %"PRIu64" not in store ==========\n",
                key);
        }*/
        ip->total_length += VALUE_SIZE * 8;
        udp->dgram_len += VALUE_SIZE * 8;
        mbuf->data_len += VALUE_SIZE * 8;
        mbuf->pkt_len += VALUE_SIZE * 8;
        // enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_PUT_REQUEST) {
        printf("TYPE_PUT_REQUEST\n");
        message_header->type = (uint8_t) TYPE_PUT_RESPONSE_B;
        /*struct tommy_object* obj = tommy_hashdyn_search(kvs,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        if (obj) {
            rte_memcpy((uint8_t *)(obj->value), (uint8_t *)value,
                VALUE_SIZE * 8);
        }*/
        
        //enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_WRITE_CACHED) {
        printf("TYPE_WRITE_CACHED\n");
        struct tommy_object* obj = tommy_hashdyn_search(kvs,
            tommy_hashtable_compare, &key, tommy_inthash_u64(key));
        if (obj) {
            rte_memcpy((uint8_t *)(obj->value), (uint8_t *)value,
                VALUE_SIZE * 8);
        }

        struct write_object* write_obj = tommy_hashdyn_search(ws,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
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
        uint8_t* bitmap1 = (uint8_t *) eth
            + sizeof(header_template) + sizeof(MessageHeader);
        uint8_t* value1 = bitmap1 + 1;
        uint8_t* bitmap2 = value1 + VALUE_SIZE / 2 * 8;
        uint8_t* value2 = bitmap2 + 1;

        *bitmap1 = 0x0f;
        rte_memcpy(value1, (uint8_t *) (kv_value), VALUE_SIZE / 2 * 8);
        *bitmap2 = 0x0f;
        rte_memcpy(value2, (uint8_t *) (kv_value), VALUE_SIZE / 2 * 8);
        ip->total_length += 1;
        udp->dgram_len += 1;
        mbuf->data_len += 1;
        mbuf->pkt_len += 1;
        //enqueue_pkt(lcore_id, mbuf);
    } else if (message_header->type == TYPE_CACHE_UPDATE_RESPONSE) {
        printf("TYPE_CACHE_UPDATE_RESPONSE\n");

        struct write_object* write_obj = tommy_hashdyn_search(ws,
            tommy_hashtable_compare_write, &key, tommy_inthash_u64(key));
        if (write_obj->mbuf != NULL) {
            struct ether_hdr* eth_clone = rte_pktmbuf_mtod(write_obj->mbuf,
                struct ether_hdr *);
            MessageHeader* message_header_clone = (MessageHeader*)
                ((uint8_t *) eth_clone + sizeof(header_template));
            if (message_header_clone->seq == message_header->seq) {
                message_header->type = (uint8_t) TYPE_PUT_RESPONSE_C;
                rte_pktmbuf_free(write_obj->mbuf);
                write_obj->mbuf = NULL;
                //enqueue_pkt(lcore_id, mbuf);
            }
        }
        //rte_pktmbuf_free(mbuf);
    } else {
        //rte_pktmbuf_free(mbuf);
    }
}

static void test(void) {
    n_lcores = 1;
    init_header_template();
    custom_init();

    struct rte_mbuf *mbuf = NULL;
    struct ether_hdr* eth = NULL;
    MessageHeader* message_header = NULL;
    uint64_t tmp_seq = 0;

    pktmbuf_pool = rte_mempool_create(
        "mbuf_pool",
        NC_NB_MBUF,
        NC_MBUF_SIZE,
        NC_MBUF_CACHE_SIZE,
        sizeof(struct rte_pktmbuf_pool_private),
        rte_pktmbuf_pool_init, NULL,
        rte_pktmbuf_init, NULL,
        rte_socket_id(),
        0);
    mbuf = rte_pktmbuf_alloc(pktmbuf_pool);
    mbuf->next = NULL;
    mbuf->nb_segs = 1;
    mbuf->ol_flags = 0;
    eth = rte_pktmbuf_mtod(mbuf, struct ether_hdr *);
    rte_memcpy(eth, header_template, sizeof(header_template));

    printf("=============== get ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader);
    mbuf->pkt_len = mbuf->data_len;
    message_header = (MessageHeader*) ((uint8_t*)eth + sizeof(header_template));
    message_header->type = (uint8_t) TYPE_GET_REQUEST;
    message_header->key = rte_cpu_to_be_64(10);
    message_header->seq = rte_rdtsc();
    tmp_seq = message_header->seq;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== put ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_PUT_REQUEST;
    uint8_t* value = (uint8_t *) message_header + sizeof(MessageHeader);
    rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);
    *value = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== put for cached key ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 1;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_WRITE_CACHED;
    char buffer[100];
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    uint64_t key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    uint8_t* bitmap = (uint8_t *) message_header + sizeof(MessageHeader);
    *bitmap = 0xff;
    value = bitmap + 1;
    rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);
    *value = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== put for cached key ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 1;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_WRITE_CACHED;
    message_header->seq = rte_rdtsc();
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    bitmap = (uint8_t *) message_header + sizeof(MessageHeader);
    *bitmap = 0xff;
    value = bitmap + 1;
    rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);
    *value = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== put for cached key ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 1;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_WRITE_CACHED;
    message_header->seq = rte_rdtsc();
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    bitmap = (uint8_t *) message_header + sizeof(MessageHeader);
    *bitmap = 0xff;
    value = bitmap + 1;
    rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);
    *value = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== write to cache ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 2;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_CACHE_UPDATE_RESPONSE;
    message_header->seq = rte_rdtsc();
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    uint8_t* bitmap1 = (uint8_t *) eth
            + sizeof(header_template) + sizeof(MessageHeader);
    uint8_t* value1 = bitmap1 + 1;
    uint8_t* bitmap2 = value1 + VALUE_SIZE / 2 * 8;
    uint8_t* value2 = bitmap2 + 1;
    *bitmap1 = 0x0f;
    rte_memcpy(value1, (uint8_t *) kv_value, VALUE_SIZE / 2 * 8);
    *value1 = 1;
    *bitmap2 = 0x0f;
    rte_memcpy(value2, (uint8_t *) kv_value, VALUE_SIZE / 2 * 8);
    *value2 = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== write to cache ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 2;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_CACHE_UPDATE_RESPONSE;
    message_header->seq = tmp_seq;
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    bitmap1 = (uint8_t *) eth
            + sizeof(header_template) + sizeof(MessageHeader);
    value1 = bitmap1 + 1;
    bitmap2 = value1 + VALUE_SIZE / 2 * 8;
    value2 = bitmap2 + 1;
    *bitmap1 = 0x0f;
    rte_memcpy(value1, (uint8_t *) kv_value, VALUE_SIZE / 2 * 8);
    *value1 = 1;
    *bitmap2 = 0x0f;
    rte_memcpy(value2, (uint8_t *) kv_value, VALUE_SIZE / 2 * 8);
    *value2 = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== put for cached key ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 1;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_WRITE_CACHED;
    message_header->seq = rte_rdtsc();
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    bitmap = (uint8_t *) message_header + sizeof(MessageHeader);
    *bitmap = 0xff;
    value = bitmap + 1;
    rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);
    *value = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    printf("=============== put for cached key ===============\n");
    mbuf->data_len = sizeof(header_template) + sizeof(MessageHeader)
        + VALUE_SIZE * 8 + 1;
    mbuf->pkt_len = mbuf->data_len;
    message_header->type = (uint8_t) TYPE_WRITE_CACHED;
    message_header->seq = rte_rdtsc();
    sprintf(buffer, "%"PRIu64, (uint64_t) 10);
    key_hash = StringToHash(buffer, strlen(buffer)) % KEY_SPACE_SIZE;
    message_header->key = rte_cpu_to_be_64(key_hash);
    bitmap = (uint8_t *) message_header + sizeof(MessageHeader);
    *bitmap = 0xff;
    value = bitmap + 1;
    rte_memcpy(value, (uint8_t *) kv_value, VALUE_SIZE * 8);
    *value = 1;
    print_packet(mbuf);
    process_packet_test(mbuf);
    print_packet(mbuf);

    rte_exit(EXIT_FAILURE, "test finish\n");
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
benchmark();
    // init
    nc_init();
    custom_init();

    // launch main loop in every lcore
    rte_eal_mp_remote_launch(nc_backend_loop, NULL, CALL_MASTER);
    RTE_LCORE_FOREACH_SLAVE(lcore_id) {
        if (rte_eal_wait_lcore(lcore_id) < 0) {
            ret = -1;
            break;
        }
    }

    return 0;
}
