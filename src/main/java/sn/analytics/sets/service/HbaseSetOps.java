package sn.analytics.sets.service;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import sn.analytics.sets.TypeHint;
import sn.analytics.sets.exception.SetNotFoundException;
import sn.analytics.sets.exception.SetOpsException;
import sn.analytics.sets.repo.HbaseManager;
import sn.analytics.sets.repo.SetMetaDataCache;
import sn.analytics.sets.type.IdSet;
import sn.analytics.sets.type.SetMeta;

import java.io.IOException;

/**
 * Service for all set interactions with Hbase
 * Created by sumanth
 */
public class HbaseSetOps {


    /**
     * returns if element is in set
     * the entire table is not accessed, use column filter
     *
     * @param setName
     * @param item
     * @return
     * @throws Exception
     */
    public boolean contains(String setName, String item) throws Exception {
        Table set = HbaseManager.getInstance().getTable(setName);
        TypeHint typeHint = SetMetaDataCache.getInstance().getSetType(setName);
        boolean elementFound = false;
        try {

            Get get = new Get(setName.getBytes());
            get.setMaxVersions(1);
            get.setMaxVersions(1);
            get.setConsistency(Consistency.TIMELINE);

            Result rs = null;

            try {
                rs = set.get(get);
                byte[] colName = null;
                switch (typeHint) {
                    case INT:
                        colName = Ints.toByteArray(Integer.valueOf(item));
                        break;
                    case LONG:
                        colName = Longs.toByteArray(Long.valueOf(item));
                        break;
                    case STRING:
                        colName = item.getBytes();
                        break;
                }

                //check in column directly
                elementFound = rs.containsColumn(HbaseManager.DEFAULT_COL_FAMILY, colName);
                System.out.println(elementFound + "  for " + item);
            } catch (IOException e) {
                e.printStackTrace();
                throw new Exception(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new SetOpsException("Exception in checking set containment " + setName + " for " + item);
        } finally {
            if (set != null)
                set.close();
        }
        return elementFound;
    }

    public void addElement(String name, String itemId) throws Exception {
        SetMeta setMeta = SetMetaDataCache.getInstance().getSetMetaData(name);
        if (setMeta == null) throw new SetNotFoundException(name + " not found");

        TypeHint typeHint = setMeta.getTypeHint();
        addElement(name, itemId, typeHint);

    }

    public void addElement(String name, String itemId, final TypeHint typeHint) throws Exception {
        Table set = HbaseManager.getInstance().getTable(name);
        try {
            //here row key is the name of the table
            //there is one big row , not too many row keys
            //TODO: split data items into multiple row keys based on a threshold
            //for example , keep a HLL to count number of items added to a a set
            //if item has expanded a threshold just add to new row key
            Put putItem = new Put(name.getBytes());
            putItem.setDurability(Durability.ASYNC_WAL);
            switch (typeHint) {
                case STRING:
                    putItem.addImmutable(HbaseManager.DEFAULT_COL_FAMILY,
                            itemId.getBytes(),
                            System.currentTimeMillis(),
                            Longs.toByteArray(System.currentTimeMillis()));
                    break;

                case INT:
                    putItem.addImmutable(HbaseManager.DEFAULT_COL_FAMILY,
                            Ints.toByteArray(Integer.valueOf(itemId)),
                            System.currentTimeMillis(),
                            Longs.toByteArray(System.currentTimeMillis()));
                    break;
                case LONG:
                    putItem.addImmutable(HbaseManager.DEFAULT_COL_FAMILY,
                            Longs.toByteArray(Long.valueOf(itemId)),
                            System.currentTimeMillis(),
                            Longs.toByteArray(System.currentTimeMillis()));

                    break;
            }
            set.put(putItem);


        } catch (Exception e) {

            e.printStackTrace();
            throw new SetOpsException("Exception in adding element to set " + name, e);
        } finally {

            if (set != null) {
                //close table
                set.close();
            }
        }


    }


    /**
     * get count of a set
     *
     * @param setName
     * @return
     * @throws Exception
     */
    public long getCount(String setName) throws Exception {
        long count = 0;
        Table set = HbaseManager.getInstance().getTable(setName);

        //TODO: assume only one row key exists
        Get get = new Get(setName.getBytes());
        get.setMaxVersions(1);
        get.setMaxVersions(1);
        get.setConsistency(Consistency.TIMELINE);
        get.isCheckExistenceOnly();
        Result rs = null;

        try {
            rs = set.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        if (rs != null) {

            if (rs.listCells() != null) {
                for (Cell cell : rs.listCells()) {
                    count++;
                }
            }
        }
        return count;
    }

    public IdSet<String> getAllElements(String setName) throws Exception {
        TypeHint typeHint = SetMetaDataCache.getInstance().getSetType(setName);
        return getAllElements(setName, typeHint);
    }

    public IdSet<String> getAllElements(String setName, final TypeHint typeHint) throws Exception {

        //TODO: load expected elemnts , from stats from set
        //ObjectOpenHashSet<String> dataSet = new ObjectOpenHashSet<String>(1000,0.25f);
        //currently are all on heap, switch as required
        IdSet dataSet = IdSet.idSetBuilder(typeHint, false);

        Table set = HbaseManager.getInstance().getTable(setName);

        //TODO: assume only one row key exists
        Get get = new Get(setName.getBytes());
        get.setMaxVersions(1);
        get.setConsistency(Consistency.TIMELINE);


        Result rs = null;


        try {
            rs = set.get(get);

        } catch (IOException e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        if (rs != null) {


            if (rs.listCells() != null) {
                for (Cell cell : rs.listCells()) {


                    if (cell != null) {
                        //long val = Bytes.toLong(CellUtil.cloneQualifier(cell));
                        switch (typeHint) {

                            case STRING:
                                dataSet.addElement(Bytes.toString(CellUtil.cloneQualifier(cell)));
                                break;

                            case INT:
                                dataSet.addElement(Bytes.toInt(CellUtil.cloneQualifier(cell)));
                                break;

                            case LONG:
                                dataSet.addElement(Bytes.toLong(CellUtil.cloneQualifier(cell)));
                                break;
                        }

                    }
                }
            }
        }

        return dataSet;
    }


}
