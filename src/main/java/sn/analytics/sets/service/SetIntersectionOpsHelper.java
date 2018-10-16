package sn.analytics.sets.service;

import sn.analytics.sets.SetOpsUtils;
import sn.analytics.sets.TypeHint;
import sn.analytics.sets.repo.SetMetaDataCache;
import sn.analytics.sets.type.IdSet;

/**
 * Created by sumanth
 */
public class SetIntersectionOpsHelper {

    //TODO: support intersection via HLL, approx intersection
    public static long getSetIntersectionCount(String setX, String setY) throws Exception {
        long commonElementsCount = 0L;
        SetOpsUtils.checkCompability(setX, setY);
        //quick check from redis
        long setXCount = SetMetaDataCache.getInstance().fetchSetSize(setX);
        long setYCount = SetMetaDataCache.getInstance().fetchSetSize(setY);
        //one of the sets is simply null then no intersection
        if (setXCount == 0 ||
                setYCount == 0)
            return 0L;


        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        //get set 1 & set 2
        IdSet idSetX = hbaseSetOps.getAllElements(setX);
        IdSet idSetY = hbaseSetOps.getAllElements(setY);

        //from here on , no operations should reach Hbase
        if (idSetX.isEmpty()) return 0L;
        if (idSetY.isEmpty()) return 0L;

        for (Object o : idSetX) {
            if (idSetY.contains(o)) {
                //System.out.println("set 2 contains ");
                commonElementsCount++;
            }
        }
        return commonElementsCount;
    }


    //allows only for 2 sets for now
    public static IdSet getSetIntersection(String setX, String setY) throws Exception {
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
        IdSet diffSet = IdSet.idSetBuilder(typeX, false);

        //untyped fellons
        for (Object o : idSetX) {
            if (!idSetY.contains(o)) {
                //System.out.println("set 2 contains ");
                diffSet.addElement(o);
            }
        }
        return diffSet;
    }

}
