import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;


public class RedisClusterManager {

    public static final String NODES_ADDR = "";

    private static final JedisCluster cluster;

    private static final Field CONNECTION_HANDLER;

    private static final Field CACHE_INFO;

    private static final JedisSlotBasedConnectionHandler connectionHandler;

    private static final JedisClusterInfoCache clusterInfoCache;

    static {
        String[] addrs = NODES_ADDR.split(",");
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (String addr : addrs) {
            HostAndPort hostAndPort = getHP(addr);
            hostAndPorts.add(hostAndPort);
        }

        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxIdle(100);
        config.setMaxTotal(600);
        cluster=new JedisCluster(hostAndPorts, config);

        CONNECTION_HANDLER = getField(BinaryJedisCluster.class, "connectionHandler");
        CACHE_INFO = getField(JedisClusterConnectionHandler.class, "cache");

        connectionHandler = getValue(cluster, CONNECTION_HANDLER);
        clusterInfoCache = getValue(connectionHandler, CACHE_INFO);

    }

    private static <T> T getValue(Object object, Field field) {
        try {
            return (T) field.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Field getField(Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("cannot find or access field '" + fieldName + "' from " + clazz.getName());
        }

    }

    private static HostAndPort getHP(String addr) {
        String[] addrs = addr.split(":");
        HostAndPort hostAndPort = new HostAndPort(addrs[0], Integer.parseInt(addrs[1]));
        return hostAndPort;
    }


    public static JedisCluster getCluster() {
        return cluster;
    }


    public static List<String> mget(List<String> keys) throws Exception {
        List<String> res = new ArrayList<>();
        if (keys == null || keys.size() == 1) {
            return res;
        }

        if (keys.size() == 1) {
            return Arrays.asList(cluster.get(keys.get(0)));
        }
        Map<JedisPool, List<String>> keysToPoolMap = new HashMap<>();
        for (String key : keys) {
            int slot = JedisClusterCRC16.getSlot(key);
            JedisPool jedisPool = clusterInfoCache.getSlotPool(slot);
            if (keysToPoolMap.containsKey(jedisPool)) {
                keysToPoolMap.get(jedisPool).add(key);
            } else {
                List<String> keyList = new ArrayList<>();
                keysToPoolMap.put(jedisPool, keyList);
                keyList.add(key);
            }
        }

        JedisPool currentJedisPool = null;
        List<String> keyList = null;
        Pipeline currentPiple = null;

        try {
            for (Map.Entry<JedisPool, List<String>> entry : keysToPoolMap.entrySet()) {
                currentJedisPool = entry.getKey();
                keyList = entry.getValue();
                currentPiple = currentJedisPool.getResource().pipelined();
                for (String key : keyList) {
                    currentPiple.get(key);
                }
                List<Object> returnAll = currentPiple.syncAndReturnAll();
                List<String> returnString = returnAll.stream().map(Object::toString).collect(Collectors.toList());
                res.addAll(returnString);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (currentPiple != null)
                currentPiple.close();
        }


        return res;
    }
}
