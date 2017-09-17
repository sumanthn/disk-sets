package sn.analytics.sets;

import sn.analytics.sets.exception.InvalidArgsException;
import sn.analytics.sets.exception.SetNotFoundException;
import sn.analytics.sets.exception.SetsCompitableException;
import sn.analytics.sets.repo.SetMetaDataCache;

/**
 * Created by sumanth
 */
public class SetOpsUtils {

    public static boolean checkCompability(final String setX, final String setY) throws InvalidArgsException, SetNotFoundException, SetsCompitableException {
        TypeHint typeX = SetMetaDataCache.getInstance().getSetType(setX);
        TypeHint typeY = SetMetaDataCache.getInstance().getSetType(setY);
        if (typeX != typeY)
            throw new SetsCompitableException("Set " + setX + "(" + typeX + ")"
                    + " and " + setY + "(" + typeX + ")" + " not Compitable");
        //can allow Type X is sub type of TypeY
        return true;

    }
}
