package sn.analytics.sets.service;

import sn.analytics.sets.SetOpsUtils;
import sn.analytics.sets.TypeHint;
import sn.analytics.sets.repo.SetMetaDataCache;
import sn.analytics.sets.type.IdSet;

/**
 * Created by sumanth on 16/09/17.
 */
public class SetDifferenceOpsHelper {

    //Set Difference
    public static long getSetDifferenceCount(String setX, String setY) throws Exception {
        long diff = 0L;
        SetOpsUtils.checkCompability(setX, setY);
        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        //get set 1 & set 2
        IdSet idSetX = hbaseSetOps.getAllElements(setX);
        IdSet idSetY = hbaseSetOps.getAllElements(setY);

        //from here on , no operations should reach Hbase
        if (idSetX.isEmpty()) return 0L;
        if (idSetY.isEmpty()) return idSetX.size();

        for (Object o : idSetX) {
            if (!idSetY.contains(o)) {
                //System.out.println("set 2 contains ");
                diff++;
            }
        }
        return diff;
    }


    public static IdSet getSetDifference(String setX, String setY) throws Exception {
        SetOpsUtils.checkCompability(setX, setY);

        TypeHint typeX = SetMetaDataCache.getInstance().getSetType(setX);

        long setXCount = SetMetaDataCache.getInstance().fetchSetSize(setX);
        long setYCount = SetMetaDataCache.getInstance().fetchSetSize(setY);

        //one of the sets is simply null then no intersection
        if (setXCount == 0 ||
                setYCount == 0)
            return IdSet.idSetBuilder(typeX, false);

        HbaseSetOps hbaseSetOps = new HbaseSetOps();
        //get set 1 & set 2
        IdSet idSetX = hbaseSetOps.getAllElements(setX);
        IdSet idSetY = hbaseSetOps.getAllElements(setY);

        //from here on , no operations should reach Hbase
        if (idSetX.isEmpty()) return IdSet.idSetBuilder(typeX, false);
        if (idSetY.isEmpty()) return IdSet.idSetBuilder(typeX, false);

        //TODO: switch based on number of elements
        IdSet commonSet = IdSet.idSetBuilder(typeX, false);

        //untyped fellons
        for (Object o : idSetX) {
            if (idSetY.contains(o)) {
                commonSet.addElement(o);
            }
        }
        return commonSet;
    }

}
