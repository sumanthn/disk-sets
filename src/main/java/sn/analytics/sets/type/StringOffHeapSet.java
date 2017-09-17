package sn.analytics.sets.type;

import java.util.Iterator;

/**
 * String based offheap sets using Chronicle map
 * Created by sumanth
 */
public class StringOffHeapSet implements IdSet<String> {
    @Override
    public void addElement(String elem) {

    }

    @Override
    public boolean contains(String elem) {
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
    public Iterator<String> iterator() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public String next() {
        return null;
    }
}
