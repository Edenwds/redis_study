import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.util.JedisClusterCRC16;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RedisClusterManager2 {


    private static final Logger logger = LoggerFactory.getLogger(RedisClusterManager2.class);
    private final JedisCluster cluster;
    private final TreeMap<Long, String> slotToHost;
    private final ReentrantReadWriteLock rwl;
    private final Lock r;
    private final Lock w;
    private volatile boolean renewSlotToHost;

    public RedisClusterManager2(String hosts) {
        String[] addrs = hosts.split(",");
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (String addr : addrs) {
            HostAndPort hostAndPort = getHP(addr);
            hostAndPorts.add(hostAndPort);
        }
        //连接池配置暂时固定
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(100);
        config.setMaxTotal(600);

        this.cluster=new JedisCluster(hostAndPorts, config);
        this.slotToHost = new TreeMap<>();
        this.rwl = new ReentrantReadWriteLock();
        this.r = this.rwl.readLock();
        this.w = this.rwl.writeLock();
        init();

    }
    private HostAndPort getHP(String addr) {
        String[] addrs = addr.split(":");
        HostAndPort hostAndPort = new HostAndPort(addrs[0], Integer.parseInt(addrs[1]));
        return hostAndPort;
    }

    private void init() {
        this.w.lock();
        Map<String, JedisPool> map = cluster.getClusterNodes();
        Iterator iterator = map.values().iterator();
        Jedis jedis = null;
        try {
            this.reset();
            while (true) {
                if (!iterator.hasNext()) {
                    return;
                }
                JedisPool jedisPool = (JedisPool) iterator.next();
                jedis = jedisPool.getResource();
                initMap(jedis);
                break;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
            this.w.unlock();
        }

    }

    private void reset() {
        this.w.lock();

        this.slotToHost.clear();

        this.w.unlock();

    }


    private void initMap(Jedis jedis) {
        List<Object> slots = jedis.clusterSlots();
        try {
            for (Object slotInfo : slots) {
                List slotInfoList = (List) slotInfo;
                if (slotInfoList.size() > 2) {
                    Long start = (Long) slotInfoList.get(0);
                    Long end = (Long) slotInfoList.get(1);
                    List<Object> hostInfos = (List<Object>) slotInfoList.get(2);
                    String host = SafeEncoder.encode((byte[])((byte[])hostInfos.get(0))) + ":" + ((Long)hostInfos.get(1)).intValue();
                    slotToHost.putIfAbsent(start, host);
                    slotToHost.putIfAbsent(end, host);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public Map<Jedis, List<String>> getJedisToKey(List<String> keys) {
        Map<Jedis, List<String>> res = new HashMap<>();
        try {
            Map<String, Jedis> hostToJedis = new HashMap<>();
            Map<String, JedisPool> hostToPool = cluster.getClusterNodes();
            int retry = 0;
            for (String key : keys) {
                int slot = JedisClusterCRC16.getSlot(key);
                Map.Entry<Long, String> hostMap = getSlotHost(Long.valueOf(slot));
                if (hostMap != null) {
                    String host = hostMap.getValue();
                    Jedis jedis = hostToJedis.get(host);
                    if (jedis == null) {
                        JedisPool jedisPool = hostToPool.get(host);
                        if (jedisPool == null) {
                            if (retry > 1) {
                                break;
                            }
                            this.rediscovery();
                            retry++;
                            continue;
                        }
                        jedis = jedisPool.getResource();
                        hostToJedis.put(host, jedis);
                    }

                    List<String> keysOfJedis = res.get(jedis);
                    if (keysOfJedis == null) {
                        keysOfJedis = new ArrayList<>();
                        res.put(jedis, keysOfJedis);
                    }
                    keysOfJedis.add(key);
                }
            }
        } catch (Exception e) {
            logger.error("assign key to redis failed", e);
        }

        return res;
    }

    private void rediscovery() {
        if (!this.renewSlotToHost) {
            try {
                this.w.lock();
                if (!this.renewSlotToHost) {
                    this.renewSlotToHost = true;
                    try {
                        this.init();
                    } catch (Exception e) {
                        throw e;
                    } finally {
                        renewSlotToHost = false;
                    }
                }
            } catch (Exception e) {
                throw e;
            } finally{
                this.w.unlock();
            }
        }
    }

    private Map.Entry<Long, String> getSlotHost(Long slot) {
        this.r.lock();
        try {
            return  slotToHost.ceilingEntry(slot);
        } finally {
            this.r.unlock();
        }
    }

    public List<String> mget(List<String> keys) {
        List<String> res = new ArrayList<>();
        Map<Jedis, List<String>> jkMap = getJedisToKey(keys);
        assert jkMap != null;

        Iterator iterator = jkMap.entrySet().iterator();
        try {
            Map<String, Response<String>> responseMap = new HashMap<>();
            while (iterator.hasNext()) {
                Map.Entry<Jedis, List<String>> jedisListEntry = (Map.Entry<Jedis, List<String>>) iterator.next();
                Jedis curJedis = jedisListEntry.getKey();
                List<String> keyList = jedisListEntry.getValue();
                Pipeline curPiple = curJedis.pipelined();
                for (String key : keyList) {
                    Response<String> response = curPiple.get(key);
                    responseMap.put(key, response);
                }
                curPiple.close();
                curJedis.close();
            }

            for (String key : keys) {
                res.add(responseMap.get(key).get());
            }
        } catch(Exception e){

            logger.error("get failed ", e);
        }
        return res;
    }

    public Map<String, String> mgetWithKey(List<String> keys) {
        Map<Jedis, List<String>> jkMap = getJedisToKey(keys);
        assert jkMap != null;

        Map<String, String> res = new HashMap<>();
        Iterator iterator = jkMap.entrySet().iterator();
        try {
            Map<String, Response<String>> responseMap = new HashMap<>();
            while (iterator.hasNext()) {
                Map.Entry<Jedis, List<String>> jedisListEntry = (Map.Entry<Jedis, List<String>>) iterator.next();
                Jedis jedis = jedisListEntry.getKey();
                List<String> keyList = jedisListEntry.getValue();
                Pipeline pipeline = jedis.pipelined();
                for (String key : keyList) {
                    Response<String> response = pipeline.get(key);
                    responseMap.put(key, response);
                }
                pipeline.close();
                jedis.close();
            }
            Iterator it = responseMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, Response<String>> entry = (Map.Entry<String, Response<String>>) it.next();
                res.put(entry.getKey(), entry.getValue().get());
            }
        } catch(Exception e){

        }

        return res;
    }
}
