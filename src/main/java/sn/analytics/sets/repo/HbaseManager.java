package sn.analytics.sets.repo;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sn.analytics.sets.exception.SetExistsException;
import sn.analytics.sets.exception.SetInitException;
import sn.analytics.sets.exception.TableNotFoundException;

import java.io.IOException;

/**
 * Created by sumanth
 */
public class HbaseManager {
    private static final Logger logger = LoggerFactory.getLogger(HbaseManager.class);
    private static HbaseManager ourInstance = new HbaseManager();

    public static HbaseManager getInstance() {
        return ourInstance;
    }

    private HbaseManager() {
    }

    public static final byte[] DEFAULT_COL_FAMILY = "a".getBytes();
    private Connection connection = null;
    boolean isInit = false;

    public Table getTable(final String name) throws TableNotFoundException, Exception {

        boolean tableExists = HbaseManager.getInstance().tableExists(name);

        if (tableExists) {
            try {
                Table hTable = connection.getTable(TableName.valueOf(name));
                return hTable;
            } catch (IOException e) {

                e.printStackTrace();
                throw new Exception("Unable to fetch table handler for " + name, e);
            }
        } else {
            throw new TableNotFoundException("HBase Table doesn't exist");
        }
    }

    //TODO: can take config file location as arg
    public synchronized void init() {
        if (isInit) return;
        logger.info("Initialize Hbase Connection..");
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("connection to hbase is initialized");
        isInit = true;
    }


    public boolean tableExists(final String name) {
        try {
            boolean exists = connection.getAdmin().tableExists(TableName.valueOf(name));
            return exists;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;

    }

    public void createTable(final String name) throws SetInitException, SetExistsException {

        logger.info("create table {}", name);
        if (tableExists(name)) {
            logger.warn("Table {} already exists");
            throw new SetExistsException("Table " + name + " already exists");
        }

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(name));

        HColumnDescriptor col = new HColumnDescriptor(DEFAULT_COL_FAMILY);
        col.setMaxVersions(1);
        col.setMinVersions(1);
        col.setCompressionType(Compression.Algorithm.GZ);
        //all simple strings
        col.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        col.setBlockCacheEnabled(false);
        descriptor.addFamily(col);

        descriptor.setDurability(Durability.ASYNC_WAL);
        descriptor.setNormalizationEnabled(true);
        descriptor.setCompactionEnabled(true);

        try {
            connection.getAdmin().createTable(descriptor);
            logger.info("Created table {}", name);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SetInitException("Exception in create Hbase Table " + name, e);
        }
    }


}
