package sn.analytics.sets.type;

import java.util.Iterator;

/**
 * Sets using Chronicle Map
 * Created by sumanth
 */
public class LongOffHeapSet implements IdSet<Long> {

    @Override
    public void addElement(Long elem) {

    }

    @Override
    public boolean contains(Long elem) {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Iterator<Long> iterator() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Long next() {
        return null;
    }
}
