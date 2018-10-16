package sn.analytics.sets;

import de.thomaskrille.dropwizard_template_config.TemplateConfigBundle;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.sets.api.SetsApi;
import sn.analytics.sets.repo.BloomCache;
import sn.analytics.sets.repo.HbaseManager;
import sn.analytics.sets.repo.SetMetaDataCache;

/**
 * Simple service for sets server
 * Created by sumanth
 */
public class BloomSetsApplication extends Application<BloomSetsConfig> {
    private static final Logger logger = LoggerFactory.getLogger(BloomSetsApplication.class);

    //TODO: Vert.X for async ops
    @Override
    public void run(BloomSetsConfig bloomSetsConfig, Environment environment) throws Exception {
        //init all repo

        try {
            HbaseManager.getInstance().init();

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

        //init various stores & cache
        logger.info("init bloom cache..");
        BloomCache.getInstance().initRepo(bloomSetsConfig.getRedisUrl());

        logger.info("Init set meta data cache");
        SetMetaDataCache.getInstance().init(bloomSetsConfig.getRedisUrl());

        logger.info("completed all initialization...");
        SetsApi setsApi = new SetsApi();
        environment.jersey().register(setsApi);

    }

    @Override
    public String getName() {
        return "Sets Server";
    }

    @Override
    public void initialize(final Bootstrap<BloomSetsConfig> bootstrap) {
        bootstrap.addBundle(new AssetsBundle());
        bootstrap.addBundle(new TemplateConfigBundle());
    }

    public static void main(final String[] args) throws Exception {
        new BloomSetsApplication().run(args);

    }
}
