package sn.analytics.sets.service;

import sn.analytics.sets.type.IdSet;

/**
 * Util service methods to access Hbase for all Operations
 * Created by sumanth
 */
public class SetOperations {

    //Set Difference
    public static long getSetIntersectionCount(String setX, String setY) throws Exception {
        return SetIntersectionOpsHelper.getSetIntersectionCount(setX, setY);
    }

    public static IdSet getSetIntersection(String setX, String setY) throws Exception {
        return SetIntersectionOpsHelper.getSetIntersection(setX, setY);
    }

    public static long getSetDifferenceCount(String setX, String setY) throws Exception {
        return SetDifferenceOpsHelper.getSetDifferenceCount(setX, setY);
    }


    public static IdSet getSetDifference(String setX, String setY) throws Exception {
        return SetDifferenceOpsHelper.getSetDifference(setX, setY);
    }

    public static long getCountOnUnion(final String setX, final String setY, boolean approx) throws Exception {
        if (approx) {
            return SetsUnionHelper.getApproxUnionCount(setX, setY);
        }
        return SetsUnionHelper.getUnionCount(setX, setY);
    }

    public static IdSet getOnUnion(final String setX, final String setY) throws Exception {
        return SetsUnionHelper.getOnUnion(setX, setY);
    }

}
