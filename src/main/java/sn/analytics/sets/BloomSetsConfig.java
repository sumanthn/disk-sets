package sn.analytics.sets;

import io.dropwizard.Configuration;

import javax.validation.Valid;

/**
 * Created by sumanth
 */
public class BloomSetsConfig extends Configuration {
    //redis to connect
    @Valid
    private String redisUrl = "localhost:26379";

    public BloomSetsConfig() {
    }

    public String getRedisUrl() {
        return redisUrl;
    }

    public void setRedisUrl(String redisUrl) {
        this.redisUrl = redisUrl;
    }
}
