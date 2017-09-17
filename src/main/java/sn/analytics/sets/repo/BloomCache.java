package sn.analytics.sets.repo;

import com.google.common.base.Preconditions;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.sets.exception.SetExistsException;

/**
 * Bloom Cache Singleton, interface for all bloom filter backing sets
 * Bloom Filter backed by Redis
 * Created by sumanth
 */
public class BloomCache {
    private static final Logger logger = LoggerFactory.getLogger(BloomCache.class);
    private static BloomCache ourInstance = new BloomCache();

    public static BloomCache getInstance() {
        return ourInstance;
    }

    static final int DEFAULT_EXPECTED_ELEMS = 100000;
    static final double DEFAULT_FP_RATE = 0.001;

    private BloomCache() {
    }

    boolean isInit = false;
    RedissonClient redisClient;


    public synchronized void initRepo(final String redisUrl) {
        if (isInit) return;

        logger.info("Init Bloom Filter repo...");
        Config redisConfig = new Config();
        //url of the form host:port 127.0.0.1:16379
        logger.info("Using Redis {} for Bloom Cache", redisUrl);
        redisConfig.useSingleServer().setAddress("redis://" + redisUrl).setConnectTimeout(1);
        //create only single server
        redisClient = Redisson.create(redisConfig);
        isInit = true;
        logger.info("Completed init of Bloom Filter cache repo");

    }

    public boolean createBloomFilter(final String name) throws SetExistsException {
        if (redisClient.getBloomFilter(name).isExists()) {
            logger.warn("Bloom filter {} exits", name);
            throw new SetExistsException("Bloom Filter backing set already exists");
        }

        boolean isBloomCreated = redisClient.getBloomFilter(name).tryInit(DEFAULT_EXPECTED_ELEMS, DEFAULT_FP_RATE);
        return isBloomCreated;
    }

    public boolean createBloomFilter(final String name,
                                     final long elems, final double fpRate) throws SetExistsException {
        if (redisClient.getBloomFilter(name).isExists()) {
            throw new SetExistsException("Bloom Filter backing set already exists");
        }
        Preconditions.checkArgument(elems > 10000000, "Cannot be for set with 10 millions");
        Preconditions.checkArgument(fpRate < 0.0001, "Cannot have very high precision");

        boolean isBloomCreated = redisClient.getBloomFilter(name).tryInit(elems, fpRate);
        return isBloomCreated;
    }

    /*
        Returns true upon item insertion, else false
     */
    public boolean updateBloomFilter(final String name, final String itemId) {
        if (redisClient.getBloomFilter(name).isExists()) {
            if (redisClient.getBloomFilter(name).contains(itemId)) return false;
            return redisClient.getBloomFilter(name).add(itemId);
        }
        return false;
    }

    public boolean containsItem(final String name, final String itemId) {
        //in case bloom is not present it fails with exception
        if (redisClient.getBloomFilter(name).isExists()) {
            return redisClient.getBloomFilter(name).contains(itemId);
        }
        return false;


    }

    public synchronized void close() {
        logger.info("Closing Bloom cache");
        redisClient.shutdown();
    }


}
