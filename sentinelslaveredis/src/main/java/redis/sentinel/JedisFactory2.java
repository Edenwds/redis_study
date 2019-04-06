package redis.sentinel;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.JedisURIHelper;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 15:18 2019/3/8
 */
public class JedisFactory2 implements PooledObjectFactory<Jedis> {
    private final AtomicReference<HostAndPort> hostAndPort;
    private final int connectionTimeout;
    private final int soTimeout;
    private final String password;
    private final int database;
    private final String clientName;
    private final boolean ssl;
    private final SSLSocketFactory sslSocketFactory;
    private final SSLParameters sslParameters;
    private final HostnameVerifier hostnameVerifier;

    JedisFactory2(String host, int port, int connectionTimeout, int soTimeout, String password, int database, String clientName) {
        this(host, port, connectionTimeout, soTimeout, password, database, clientName, false, (SSLSocketFactory)null, (SSLParameters)null, (HostnameVerifier)null);
    }

    JedisFactory2(String host, int port, int connectionTimeout, int soTimeout, String password, int database, String clientName, boolean ssl, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        this.hostAndPort = new AtomicReference();
        this.hostAndPort.set(new HostAndPort(host, port));
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
    }

    JedisFactory2(URI uri, int connectionTimeout, int soTimeout, String clientName) {
        this(uri, connectionTimeout, soTimeout, clientName, (SSLSocketFactory)null, (SSLParameters)null, (HostnameVerifier)null);
    }

    JedisFactory2(URI uri, int connectionTimeout, int soTimeout, String clientName, SSLSocketFactory sslSocketFactory, SSLParameters sslParameters, HostnameVerifier hostnameVerifier) {
        this.hostAndPort = new AtomicReference();
        if (!JedisURIHelper.isValid(uri)) {
            throw new InvalidURIException(String.format("Cannot open Redis connection due invalid URI. %s", uri.toString()));
        } else {
            this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
            this.connectionTimeout = connectionTimeout;
            this.soTimeout = soTimeout;
            this.password = JedisURIHelper.getPassword(uri);
            this.database = JedisURIHelper.getDBIndex(uri);
            this.clientName = clientName;
            this.ssl = JedisURIHelper.isRedisSSLScheme(uri);
            this.sslSocketFactory = sslSocketFactory;
            this.sslParameters = sslParameters;
            this.hostnameVerifier = hostnameVerifier;
        }
    }

    public void setHostAndPort(HostAndPort hostAndPort) {
        this.hostAndPort.set(hostAndPort);
    }

    public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception {
        BinaryJedis jedis = (BinaryJedis)pooledJedis.getObject();
        if (jedis.getDB() != this.database) {
            jedis.select(this.database);
        }

    }

    public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception {
        BinaryJedis jedis = (BinaryJedis)pooledJedis.getObject();
        if (jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception var4) {
                    ;
                }

                jedis.disconnect();
            } catch (Exception var5) {
                ;
            }
        }

    }

    public PooledObject<Jedis> makeObject() throws Exception {
        HostAndPort hostAndPort = (HostAndPort)this.hostAndPort.get();
        Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), this.connectionTimeout, this.soTimeout, this.ssl, this.sslSocketFactory, this.sslParameters, this.hostnameVerifier);

        try {
            jedis.connect();
            if (this.password != null) {
                jedis.auth(this.password);
            }

            if (this.database != 0) {
                jedis.select(this.database);
            }

            if (this.clientName != null) {
                jedis.clientSetname(this.clientName);
            }
        } catch (JedisException var4) {
            jedis.close();
            throw var4;
        }

        return new DefaultPooledObject(jedis);
    }

    public void passivateObject(PooledObject<Jedis> pooledJedis) throws Exception {
    }

    public boolean validateObject(PooledObject<Jedis> pooledJedis) {
        BinaryJedis jedis = (BinaryJedis)pooledJedis.getObject();

        try {
            HostAndPort hostAndPort = (HostAndPort)this.hostAndPort.get();
            String connectionHost = jedis.getClient().getHost();
            int connectionPort = jedis.getClient().getPort();
            return hostAndPort.getHost().equals(connectionHost) && hostAndPort.getPort() == connectionPort && jedis.isConnected() && jedis.ping().equals("PONG");
        } catch (Exception var6) {
            return false;
        }
    }
}
