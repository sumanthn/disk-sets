package sn.analytics.sets.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.sets.exception.SetNotFoundException;
import sn.analytics.sets.repo.BloomCache;
import sn.analytics.sets.repo.SetMetaDataCache;
import sn.analytics.sets.type.SetMeta;

/**
 * Created by sumanth
 */
public class SetDaoService {
    private static final Logger logger = LoggerFactory.getLogger(SetDaoService.class);
    private static SetDaoService ourInstance = new SetDaoService();

    public static SetDaoService getInstance() {
        return ourInstance;
    }

    private SetDaoService() {
    }


    //TODO: introduce transaction & counting bloom filter for removal
    public boolean addElement(final String name, final String itemid) throws Exception {
        SetMeta setMeta = SetMetaDataCache.getInstance().getSetMetaData(name);
        if (setMeta == null) throw new SetNotFoundException(name + " not found");

        if (logger.isDebugEnabled())
            logger.debug("trying to add to set " + name + " " + itemid);

        //check in bloom filter and if required add element
        boolean isPresent = BloomCache.getInstance().containsItem(name, itemid);
        if (isPresent) {
            if (logger.isDebugEnabled())
                logger.debug("item " + itemid + " already exists in set " + name);
            return false;
        } else {
            //System.out.println("item doesn't exists in bloom add it now...");
        }
        //get type hint to make better choice
        //
        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        //add with type hints , useful for processing
        hbaseSetOps.addElement(name, itemid);
        //add to Bloom here, else there is a scenario, added and subsequent additions failure
        //failure to add bloom is not a catastrophe
        BloomCache.getInstance().updateBloomFilter(name, itemid);

        //Update auxillary structures
        SetMetaDataCache.getInstance().updateSetStats(name, itemid);


        return true;
    }

    public boolean checkElementApprox(final String set, final String item) throws Exception {
        //check in bloom and if present return
        return BloomCache.getInstance().containsItem(set, item);

    }


}
