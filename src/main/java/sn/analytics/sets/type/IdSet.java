package sn.analytics.sets.type;

import sn.analytics.sets.TypeHint;

import java.util.Iterator;

/**
 * A set interface
 * Created by sumanth
 */
public interface IdSet<T> extends Iterator<T>, Iterable<T> {


    void addElement(T elem);

    boolean contains(T elem);

    boolean isEmpty();

    int size();


    //simple factory to get right set
    static IdSet idSetBuilder(final TypeHint typeHint, boolean offHeap) {
        if (offHeap) {


            switch (typeHint) {
                case STRING:
                    return new StringOffHeapSet();
                case INT:
                case LONG:
                    return new LongOffHeapSet();

            }
        } else {
            //on heap sets
            switch (typeHint) {

                case STRING:
                    return new StringSet();
                case INT:

                case LONG:
                    return new LongIdSet();
            }
        }
        return null;
    }

}
