package sn.analytics.sets.type;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import sn.analytics.sets.SetConfigUtils;

import java.util.Iterator;

/**
 * Created by sumanth
 */
public class LongIdSet implements IdSet<Long> {

    private LongOpenHashSet dataSet = new LongOpenHashSet(SetConfigUtils.DEFAULT_EXPECTED_ELEMS, 0.5f);

    @Override
    public void addElement(Long elem) {
        dataSet.add(elem.longValue());
    }

    @Override
    public boolean contains(Long elem) {
        return dataSet.contains(elem.longValue());
    }

    @Override
    public boolean isEmpty() {
        return dataSet.isEmpty();
    }

    @Override
    public int size() {
        return dataSet.size();
    }

    @Override
    public Iterator<Long> iterator() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return dataSet.iterator().hasNext();

    }

    public LongOpenHashSet getDataSet() {
        return dataSet;
    }

    @Override
    public Long next() {

        //little tricky with boxing :p
        return dataSet.iterator().next();
    }
}
