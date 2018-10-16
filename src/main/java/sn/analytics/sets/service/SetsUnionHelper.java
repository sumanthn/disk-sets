package sn.analytics.sets.service;

import sn.analytics.sets.SetOpsUtils;
import sn.analytics.sets.TypeHint;
import sn.analytics.sets.repo.SetMetaDataCache;
import sn.analytics.sets.type.IdSet;

/**
 * Helper to handle Union operations
 * Created by sumanth
 */
public class SetsUnionHelper {


    public static long getUnionCount(final String setX, final String setY) throws Exception {
        long commonElementsCount = 0L;
        SetOpsUtils.checkCompability(setX, setY);
        //quick check from redis
        long setXCount = SetMetaDataCache.getInstance().fetchSetSize(setX);
        long setYCount = SetMetaDataCache.getInstance().fetchSetSize(setY);
        //short circuit no Hbase ops
        if (setXCount == 0 &&
                setYCount == 0)
            return 0L;


        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        //get set 1 & set 2
        IdSet idSetX = hbaseSetOps.getAllElements(setX);
        IdSet idSetY = hbaseSetOps.getAllElements(setY);

        //from here on , no operations should reach Hbase
        if (idSetX.isEmpty()) return idSetY.size();
        if (idSetY.isEmpty()) return idSetX.size();

        for (Object o : idSetX) {
            if (idSetY.contains(o)) {
                commonElementsCount++;
            }
        }
        return ((idSetX.size() + idSetY.size()) - commonElementsCount);

    }

    public static long getApproxUnionCount(final String setX, final String setY) throws Exception {

        //use HyperLogLog to approximate count
        //need not do a compitablity check

        //Union and cardinality also done using Bloom + actual set, this requires atleast
        //one set to be accessed

        SetOpsUtils.checkCompability(setX, setY);
        //quick check from redis
        long setXCount = SetMetaDataCache.getInstance().fetchSetSize(setX);
        long setYCount = SetMetaDataCache.getInstance().fetchSetSize(setY);
        //short circuit no Hbase ops
        if (setXCount == 0 &&
                setYCount == 0)
            return 0L;

        return SetMetaDataCache.getInstance().fetchOnUnion(setX, setY);
    }


    public static IdSet getOnUnion(final String setX, final String setY) throws Exception {

        SetOpsUtils.checkCompability(setX, setY);

        TypeHint typeX = SetMetaDataCache.getInstance().getSetType(setX);
        //TypeHint typeY = SetMetaDataCache.getInstance().getSetType(setNameY);

        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        //get set 1 & set 2
        IdSet idSetX = hbaseSetOps.getAllElements(setX);
        IdSet idSetY = hbaseSetOps.getAllElements(setY);

        //from here on , no operations should reach Hbase
        if (idSetX.isEmpty()) return idSetX;
        if (idSetY.isEmpty()) return idSetY;

        //TODO: switch based on number of elements
        IdSet unionSet = IdSet.idSetBuilder(typeX, false);

        //untyped fellons
        for (Object o : idSetX)
            unionSet.addElement(o);


        for (Object o : idSetY)
            unionSet.addElement(o);

        return unionSet;

    }


}
