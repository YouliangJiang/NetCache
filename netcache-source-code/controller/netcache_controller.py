class NetCacheController(object):
    def __init__(self, file_name):
        self.cache_size = 10
        self.hot_keys = []
        with open(file_name, "r") as f:
            line = f.readline()
            for line in f:
                self.hot_keys.append(int(line))

    def run_static(self):
        cache_size = raw_input()
        self.cache_size = int(cache_size)
        for i in range(self.cache_size):
            key = self.hot_keys[i]
            cache_port = ports[1]
            key_idx = i
            val_idx = i
            valstr = str(key+1)
            stages = [1 for j in range(8)]
            self.nc_api.insert_key_value_str(
                dev_tgt, key, cache_port, key_idx, val_idx, valstr, stages)
            self.conn_mgr.complete_operations(self.sess_hdl)

#********************************************************************
# main
#********************************************************************
if __name__ == "__main__":
    controller = NetCacheController("hot_key_hash_100K")
    controller.run_static()
