package redis.sentinel;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolAbstract;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 14:59 2019/3/8
 */
public class JedisSentinelMasterSlavePool extends JedisPoolAbstract {
    protected GenericObjectPoolConfig poolConfig;
    protected int connectionTimeout;
    protected int soTimeout;
    protected String password;
    protected int database;
    protected String clientName;
    protected Set<MasterListener> masterListeners;
    protected Logger log;
    private volatile HostAndPort currentHostMaster;
    private volatile JedisFactory2 factory;
    private volatile Map<HostAndPort, GenericObjectPool<Jedis>> slavePools;
    private volatile List<HostAndPort> slavesAddr;
    private final Set<String> sentinels;
    private final Object initPoolLock;
    private final Object changeSlavePoolLock;
    private final ThreadLocal<GenericObjectPool<Jedis>> objectPoolThreadLocal = new ThreadLocal<>();

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig) {
        this(masterName, sentinels, poolConfig, 2000, (String)null, 0);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), 2000, (String)null, 0);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, String password) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), 2000, password);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, String password) {
        this(masterName, sentinels, poolConfig, timeout, password, 0);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout) {
        this(masterName, sentinels, poolConfig, timeout, (String)null, 0);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, String password) {
        this(masterName, sentinels, poolConfig, 2000, password);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, String password, int database) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, String password, int database, String clientName) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database, clientName);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int timeout, int soTimeout, String password, int database) {
        this(masterName, sentinels, poolConfig, timeout, soTimeout, password, database, (String)null);
    }

    public JedisSentinelMasterSlavePool(String masterName, Set<String> sentinels, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, int database, String clientName) {
        this.connectionTimeout = 2000;
        this.soTimeout = 2000;
        this.database = 0;
        this.masterListeners = new HashSet();
        this.log = LoggerFactory.getLogger(JedisSentinelMasterSlavePool.class);
        this.initPoolLock = new Object();
        this.changeSlavePoolLock = new Object();
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.sentinels = sentinels;
        HostAndPort master = this.initSentinels(sentinels, masterName);
        this.initPool(master, this.slavesAddr);
    }

    public void destroy() {
        Iterator var1 = this.masterListeners.iterator();

        while(var1.hasNext()) {
            JedisSentinelMasterSlavePool.MasterListener m = (JedisSentinelMasterSlavePool.MasterListener)var1.next();
            m.shutdown();
        }

        super.destroy();
        //关闭从机连接池
        this.destroySlavePool(this.slavePools);
    }

    private void destroySlavePool(Map<HostAndPort, GenericObjectPool<Jedis>> slavePools) {
        for (GenericObjectPool pool : slavePools.values()) {
            pool.close();
        }
    }

    public HostAndPort getCurrentHostMaster() {
        return this.currentHostMaster;
    }

    private void initPool(HostAndPort master) {
        Object var2 = this.initPoolLock;
        synchronized(this.initPoolLock) {
            if (!master.equals(this.currentHostMaster)) {
                this.currentHostMaster = master;
                if (this.factory == null) {
                    this.factory = new JedisFactory2(master.getHost(), master.getPort(), this.connectionTimeout, this.soTimeout, this.password, this.database, this.clientName);
                    this.initPool(this.poolConfig, this.factory);
                } else {
                    this.factory.setHostAndPort(this.currentHostMaster);
                    this.internalPool.clear();
                }

                this.log.info("Rcreated JedisPool to master at " + master);
            }

        }
    }

    //重载 initPool方法  添加从机连接池初始化
    private void initPool(HostAndPort master, List<HostAndPort> slaves) {
        Object var2 = this.initPoolLock;
        synchronized(this.initPoolLock) {
            if (!master.equals(this.currentHostMaster)) {
                this.currentHostMaster = master;
                this.slavesAddr = slaves;
                if (this.factory == null) {
                    this.factory = new JedisFactory2(master.getHost(), master.getPort(), this.connectionTimeout, this.soTimeout, this.password, this.database, this.clientName);
                    this.initPool(this.poolConfig, this.factory);
                    //初始化从机连接池
                    this.initSlavePool(slaves);
                } else {
                    this.factory.setHostAndPort(this.currentHostMaster);
                    this.internalPool.clear();
                }

                this.log.info("Created JedisPool to master at " + master);
            }

        }
    }

    //创建从机连接池
    private void initSlavePool(List<HostAndPort> slaves) {
        Map<HostAndPort, GenericObjectPool<Jedis>> slavePools = new HashMap<>();
        for (HostAndPort slave : slaves) {
            GenericObjectPool<Jedis> slavePool = new GenericObjectPool<Jedis>(new JedisFactory2(slave.getHost(), slave.getPort(), this.connectionTimeout, this.soTimeout, this.password, this.database, this.clientName), this.poolConfig);
            this.log.info("Found Redis slave at {}, created a Slave JedisPool", slave);
            slavePools.put(slave, slavePool);
        }
        this.slavePools = slavePools;
    }

    private HostAndPort initSentinels(Set<String> sentinels, String masterName) {
        HostAndPort master = null;
        boolean sentinelAvailable = false;
        this.log.info("Trying to find master from available Sentinels...");
        Iterator var5 = sentinels.iterator();

        String sentinel;
        HostAndPort hap;
        while(var5.hasNext()) {
            sentinel = (String)var5.next();
            hap = HostAndPort.parseString(sentinel);
            this.log.debug("Connecting to Sentinel {}", hap);
            Jedis jedis = null;

            try {
                jedis = new Jedis(hap);
                List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);
                sentinelAvailable = true;
                //获取从机地址
                List<Map<String, String>> slaveAddr = jedis.sentinelSlaves(masterName);
                if (masterAddr != null && masterAddr.size() == 2 && slaveAddr != null && slaveAddr.size() > 0) {
                    master = this.toHostAndPort(masterAddr);
                    this.log.info("Found Redis master at {}", master);
                    //初始化从机地址
                    this.slavesAddr = this.slaveAddrToHostAndPort(slaveAddr);
                    break;
                }

                this.log.warn("Can not get master addr, master name: {}. Sentinel: {}", masterName, hap);
            } catch (JedisException var13) {
                this.log.warn("Cannot get master address from sentinel running @ {}. Reason: {}. Trying next one.", hap, var13.toString());
            } finally {
                if (jedis != null) {
                    jedis.close();
                }

            }
        }

        if (master == null) {
            if (sentinelAvailable) {
                throw new JedisException("Can connect to sentinel, but " + masterName + " seems to be not monitored...");
            } else {
                throw new JedisConnectionException("All sentinels down, cannot determine where is " + masterName + " master is running...");
            }
        } else {
            this.log.info("Redis master running at " + master + ", starting Sentinel listeners...");
            var5 = sentinels.iterator();

            while(var5.hasNext()) {
                sentinel = (String)var5.next();
                hap = HostAndPort.parseString(sentinel);
                JedisSentinelMasterSlavePool.MasterListener masterListener = new JedisSentinelMasterSlavePool.MasterListener(masterName, hap.getHost(), hap.getPort());
                masterListener.setDaemon(true);
                this.masterListeners.add(masterListener);
                masterListener.start();
            }

            return master;
        }
    }

    //转化从机地址
    private List<HostAndPort> slaveAddrToHostAndPort(List<Map<String, String>> slaveAddr) {
        List<HostAndPort> slaveHPList = new ArrayList<HostAndPort>();
        for (Map<String, String> salve : slaveAddr) {
            String ip = salve.get("ip");
            if ("127.0.0.1".equals(ip)) {
                continue;
            }
            String port = salve.get("port");
            String flags = salve.get("flags");
            if (!flags.contains("disconnected") && !flags.contains("s_down")) {
                HostAndPort hostAndPort = toHostAndPort(ip, port);
                slaveHPList.add(hostAndPort);

            }
        }

        return slaveHPList;
    }

    private HostAndPort toHostAndPort(String ip, String port) {
        return new HostAndPort(ip, Integer.parseInt(port));
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = (String)getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt((String)getMasterAddrByNameResult.get(1));
        return new HostAndPort(host, port);
    }

    //获取主机连接
    public Jedis getResource() {
        while(true) {
            Jedis jedis = (Jedis)super.getResource();
            jedis.setDataSource(this);
            HostAndPort master = this.currentHostMaster;
            HostAndPort connection = new HostAndPort(jedis.getClient().getHost(), jedis.getClient().getPort());
            if (master.equals(connection)) {
                this.log.info("master address is {}", master);
                return jedis;
            }

            this.returnBrokenResource(jedis);
        }
    }

    public Jedis getSlaveResource() {
        try{
            if (this.slavePools != null && this.slavePools.size() > 0) {
                Random random = new Random();
                HostAndPort slaveHP = this.slavesAddr.get(random.nextInt(slavePools.size()));
                GenericObjectPool<Jedis> pool = this.slavePools.get(slaveHP);
                this.log.info("Get a slave pool, the address is {}", slaveHP);
                objectPoolThreadLocal.set(pool);
                return pool.borrowObject();
            }
        }catch(Exception e){
            this.log.debug("Could not get a resource form slave pools");
        }
        return this.getResource();
    }

    //将从机资源归还
    public void closeSlaveJedis(Jedis jedis) {
        GenericObjectPool<Jedis> pool = objectPoolThreadLocal.get();
        //是否为从机的连接
        if (pool != null) {
            pool.returnObject(jedis);
        } else {
            jedis.close();
        }
    }

    protected void returnBrokenResource(Jedis resource) {
        if (resource != null) {
            this.returnBrokenResourceObject(resource);
        }

    }

    //返回主机连接
    protected void returnResource(Jedis resource) {
        if (resource != null) {
            resource.resetState();
            this.returnResourceObject(resource);
        }

    }

    protected class MasterListener extends Thread {
        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis;
        protected volatile Jedis j;
        protected AtomicBoolean running;

        protected MasterListener() {
            this.subscribeRetryWaitTimeMillis = 5000L;
            this.running = new AtomicBoolean(false);
        }

        public MasterListener(String masterName, String host, int port) {
            super(String.format("MasterListener-%s-[%s:%d]", masterName, host, port));
            this.subscribeRetryWaitTimeMillis = 5000L;
            this.running = new AtomicBoolean(false);
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port, long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        public void run() {
            this.running.set(true);
            while(this.running.get()) {
                this.j = new Jedis(this.host, this.port);

                try {
                    if (!this.running.get()) {
                        break;
                    }

                    List<String> masterAddr = this.j.sentinelGetMasterAddrByName(this.masterName);
                    //获取从机地址
                    List<Map<String, String>> slavesAddr = this.j.sentinelSlaves(this.masterName);
                    if (masterAddr != null && masterAddr.size() == 2) {
                        JedisSentinelMasterSlavePool.this.initPool(JedisSentinelMasterSlavePool.this.toHostAndPort(masterAddr), JedisSentinelMasterSlavePool.this.slaveAddrToHostAndPort(slavesAddr));
                    } else {
                        JedisSentinelMasterSlavePool.this.log.warn("Can not get master addr, master name: {}. Sentinel: {}：{}.", new Object[]{this.masterName, this.host, this.port});
                    }

                    //重写监听机制  当发现主机切换时，重新初始化主机的连接池。当发现新从机上线(作为旧主机故障恢复,重新上线成为从机),添加新从机到从机连接池
                    //还有从机的主观下线时 需要将其删除
                    this.j.subscribe(new JedisPubSub() {
                        public void onMessage(String channel, String message) {
                            JedisSentinelMasterSlavePool.this.log.debug("Sentinel {}:{}, channel is {} == published: {}.", new Object[]{JedisSentinelMasterSlavePool.MasterListener.this.host, JedisSentinelMasterSlavePool.MasterListener.this.port, channel, message});

                            String[] switchMasterMsg = message.split(" ");

                            if (switchMasterMsg.length > 3 && channel.equals("+switch-master")) {
                                if (JedisSentinelMasterSlavePool.MasterListener.this.masterName.equals(switchMasterMsg[0])) {
                                    JedisSentinelMasterSlavePool.this.log.debug("Listening messgae on +switch-master for master name {}, the new master address is {} : {}", switchMasterMsg[0], switchMasterMsg[3], switchMasterMsg[4]);
                                    JedisSentinelMasterSlavePool.this.initPool(JedisSentinelMasterSlavePool.this.toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4])));
                                }
                            }
                            if (switchMasterMsg.length > 5 && JedisSentinelMasterSlavePool.MasterListener.this.masterName.equals(switchMasterMsg[5])) {
                                if (channel.equals("+slave")) {
                                    JedisSentinelMasterSlavePool.this.log.debug("Listening messgae on +slave for master name {}, the new slave address is {} : {}", switchMasterMsg[5], switchMasterMsg[2], switchMasterMsg[3]);
                                    JedisSentinelMasterSlavePool.this.addSlavePool(switchMasterMsg);
                                }

                                if (channel.equals("+sdown")) {
                                    if ("slave".equals(switchMasterMsg[0])) {
                                        JedisSentinelMasterSlavePool.this.log.debug("Listening messgae on +sdown for master name {}, the slave is now in Subjectively Down state, remove the slave, the address is {} : {}", switchMasterMsg[5], switchMasterMsg[2], switchMasterMsg[3]);
                                        JedisSentinelMasterSlavePool.this.removeSlavePool(switchMasterMsg);
                                    }
                                }

                                if (channel.equals("-sdown")) {
                                    if ("slave".equals(switchMasterMsg[0])) {
                                        JedisSentinelMasterSlavePool.this.log.debug("Listening messgae on -sdown for master name {}, the slave is no logger in Subjectively Down state, readd the slave, the address is {} : {}", switchMasterMsg[5], switchMasterMsg[2], switchMasterMsg[3]);
                                        JedisSentinelMasterSlavePool.this.addSlavePool(switchMasterMsg);
                                    }
                                }

                            }

                        }
                    }, new String[]{"+switch-master", "+slave", "+sdown", "-sdown"});
                } catch (JedisException var8) {
                    if (this.running.get()) {
                        JedisSentinelMasterSlavePool.this.log.error("Lost connection to Sentinel at {}:{}. Sleeping 5000ms and retrying.", new Object[]{this.host, this.port, var8});

                        try {
                            Thread.sleep(this.subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException var7) {
                            JedisSentinelMasterSlavePool.this.log.error("Sleep interrupted: ", var7);
                        }
                    } else {
                        JedisSentinelMasterSlavePool.this.log.debug("Unsubscribing from Sentinel at {}:{}", this.host, this.port);
                    }
                } finally {
                    this.j.close();
                }
            }

        }

        public void shutdown() {
            try {
                JedisSentinelMasterSlavePool.this.log.debug("Shutting down listener on {}:{}", this.host, this.port);
                this.running.set(false);
                if (this.j != null) {
                    this.j.disconnect();
                }
            } catch (Exception var2) {
                JedisSentinelMasterSlavePool.this.log.error("Caught exception while shutting down: ", var2);
            }

        }
    }

    //当监听到从机处于主观下线状态时 将该实例从从机连接池删除
    private void removeSlavePool(String[] switchMasterMsg) {
        synchronized (this.changeSlavePoolLock) {
            String ip = switchMasterMsg[2];
            String port = switchMasterMsg[3];
            HostAndPort oldSlave = this.toHostAndPort(ip, port);
            if (this.slavesAddr != null && slavesAddr.contains(oldSlave)) {
                this.slavesAddr.remove(oldSlave);
                this.slavePools.remove(oldSlave);
                this.log.debug("Remove the slave that is in Subjectively Down state ", oldSlave);
            }

        }
    }

    //当监听到+slave消息新从机上线时, 添加从机连接池
    private void addSlavePool(String[] switchMasterMsg) {
        synchronized(this.changeSlavePoolLock) {
            String ip = switchMasterMsg[2];
            String port = switchMasterMsg[3];
            HostAndPort newSlave = this.toHostAndPort(ip, port);
            if (this.slavesAddr != null && !slavesAddr.contains(newSlave)) {
                this.slavesAddr.add(newSlave);
                GenericObjectPool<Jedis> slavePool = new GenericObjectPool<>(new JedisFactory2(newSlave.getHost(), newSlave.getPort(), this.connectionTimeout, this.soTimeout, this.password, this.database, this.clientName), this.poolConfig);
                this.slavePools.put(newSlave, slavePool);
                this.log.debug("Added a new slave JedisPool " + newSlave);
            } else {
                this.log.debug("The slave already existed");
            }
        }
    }

}
