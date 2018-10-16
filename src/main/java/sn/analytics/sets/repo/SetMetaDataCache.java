package sn.analytics.sets.repo;

import com.google.common.base.Strings;
import org.redisson.Redisson;
import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RLocalCachedMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import sn.analytics.sets.SetConfigUtils;
import sn.analytics.sets.TypeHint;
import sn.analytics.sets.exception.InvalidArgsException;
import sn.analytics.sets.exception.SetExistsException;
import sn.analytics.sets.exception.SetNotFoundException;
import sn.analytics.sets.type.SetMeta;

import java.util.concurrent.TimeUnit;

/**
 * Cache for set meta
 * Also,keep stats for sets in use
 * Created by sumanth
 */
public class SetMetaDataCache {
    private Logger logger = LoggerFactory.getLogger(SetMetaDataCache.class);
    private static SetMetaDataCache ourInstance = new SetMetaDataCache();

    public static SetMetaDataCache getInstance() {
        return ourInstance;
    }

    private SetMetaDataCache() {
    }

    RedissonClient redisClient = null;
    boolean isInit = false;
    JedisPool jedisPool;
    RLocalCachedMap<String, SetMeta> metaMap;

    public synchronized void init(final String redisUrl) {
        if (isInit) return;
        Config redisConfig = new Config();
        redisConfig.useSingleServer().setAddress("redis://" + redisUrl).setConnectTimeout(10);
        redisClient = Redisson.create(redisConfig);
        LocalCachedMapOptions options = LocalCachedMapOptions.defaults()
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
                        // If cache size is 0 then local cache is unbounded.
                .cacheSize(10000)
                .invalidationPolicy(LocalCachedMapOptions.InvalidationPolicy.ON_CHANGE)
                        //this is a small cache
                .timeToLive(30 * 365, TimeUnit.DAYS)
                .maxIdle(30 * 60 * 1000, TimeUnit.SECONDS);
        metaMap = redisClient.getLocalCachedMap("setmeta", options);

        //required only for one call incr

        jedisPool = new JedisPool("redis://" + redisUrl);

        isInit = true;
        System.out.println("Initalized set meta data cache");
    }

    public void updateSetStats(String name, String itemId) {
        //increment counter
        try (Jedis jedis = jedisPool.getResource()) {
            //keep simple counter for expected elements
            //can make this into a key builder
            long setCount = jedis.incr(SetConfigUtils.COUNTS_PREFIX + ":" + name);
            //System.out.println(setCount);
            //uniques set is useful for quick,approx union counts
            jedis.pfadd(SetConfigUtils.UNIQUES_PREFIX + ":" + name, itemId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void addSetCache(final SetMeta setMeta) throws SetExistsException, InvalidArgsException {
        if (setMeta == null) throw new InvalidArgsException("Set Meta data cannot be null");
        if (metaMap.containsKey(setMeta.getName())) throw new SetExistsException(setMeta.getName() + "Set Exists ");
        logger.info("Adding to meta data cache " + setMeta.toString());

        metaMap.put(setMeta.getName(), setMeta);
    }

    public SetMeta getSetMetaData(final String name) throws InvalidArgsException, SetNotFoundException {
        if (Strings.isNullOrEmpty(name)) throw new InvalidArgsException("Set name cannot be null");
        if (metaMap.containsKey(name))
            return metaMap.get(name);
        throw new SetNotFoundException(name + " Set not found");
    }

    public TypeHint getSetType(final String name) throws InvalidArgsException, SetNotFoundException {
        SetMeta meta = getSetMetaData(name);
        if (meta != null) {
            if (meta.getTypeHint() != null)
                return meta.getTypeHint();
            return TypeHint.STRING;//default
        }
        //set doesn't exists
        throw new SetNotFoundException(name + " Set not found");

    }

    //this should be almost same as cardinality , since the set is backed by bloom with less FP
    public long fetchSetSize(final String name) {
        try (Jedis jedis = jedisPool.getResource()) {
            String redisCountKey = SetConfigUtils.COUNTS_PREFIX + ":" + name;
            if (jedis.exists(redisCountKey))
                return Long.parseLong(jedis.get(redisCountKey));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0L;
    }

    public long fetchOnUnion(final String setX, final String setY) {
        try (Jedis jedis = jedisPool.getResource()) {
            String[] setArr = new String[2];
            setArr[0] = SetConfigUtils.UNIQUES_PREFIX + ":" + setX;
            setArr[1] = SetConfigUtils.UNIQUES_PREFIX + ":" + setY;

            return jedis.pfcount(setArr);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0L;
    }

    public synchronized void close() {
        redisClient.shutdown();
        jedisPool.close();
    }
}
