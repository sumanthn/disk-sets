package sn.analytics.sets.type;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Iterator;

/**
 * String set using FastUtil
 * Created by sumanth
 */
public class StringSet implements IdSet<String> {
    //can use OpenBigHashSet too
    private ObjectOpenHashSet<String> dataSet = new ObjectOpenHashSet<>();


    public StringSet() {
        dataSet = new ObjectOpenHashSet<>(1000, 0.10f);
    }


    @Override
    public void addElement(String elem) {
        dataSet.add(elem);
    }

    @Override
    public boolean contains(String elem) {

        return dataSet.contains(elem);
    }

    @Override
    public boolean isEmpty() {
        return dataSet.isEmpty();
    }

    @Override
    public Iterator<String> iterator() {
        return dataSet.iterator();
    }


    //can hit 2 B one day , use OpenBigHashSet and long
    @Override
    public int size() {
        return dataSet.size();
    }

    @Override
    public boolean hasNext() {
        return dataSet.iterator().hasNext();
    }

    @Override
    public String next() {
        return dataSet.iterator().next();
    }

    public ObjectOpenHashSet<String> getDataSet() {
        return dataSet;
    }
}
