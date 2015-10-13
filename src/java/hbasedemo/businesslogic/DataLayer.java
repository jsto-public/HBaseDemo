package hbasedemo.businesslogic;

import java.io.*;
import java.text.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.commons.lang.time.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class DataLayer {

    static Configuration _config = null;

    // Get configuration to connect to HBase cluster
    public static Configuration GetHBaseConfig() throws IOException {

        if (_config == null) {
            _config = HBaseConfiguration.create();
            _config.clear();
            _config.set("hbase.zookeeper.quorum", "ec2-54-212-83-56.us-west-2.compute.amazonaws.com");
            _config.set("hbase.zookeeper.property.clientPort", "2181");
            _config.set("hbase.master", "ec2-54-212-83-56.us-west-2.compute.amazonaws.com:60000");
            // HBaseAdmin.checkHBaseAvailable(_config);
        }
        return _config;
    }

    public static void createSchema() throws IOException {

        Configuration config = GetHBaseConfig();

        HBaseAdmin admin = new HBaseAdmin(config);

        if (admin.tableExists("intervaldata")) {
            admin.disableTable("intervaldata");
            admin.deleteTable("intervaldata");
        }

        HTableDescriptor table = new HTableDescriptor("intervaldata");
        HColumnDescriptor columnFamily = new HColumnDescriptor("data");
        columnFamily.setMaxVersions(1);
        table.addFamily(columnFamily);

        admin.createTable(table);
    }

    // Reads interval data from HBase for given meterId
    public static IntervalDataSet readIntervalDataSet(String meterId) throws IOException {
        IntervalDataSet intervalDataSet = new IntervalDataSet();
        intervalDataSet.meterId = meterId;
        Configuration config = GetHBaseConfig();               
        HTable table = new HTable(config, "intervaldata");

        String prefix = meterId + "-";
        
        List<IntervalData> intervalDataList = new ArrayList<IntervalData>();
           
        Scan scan = new Scan();
        scan.setCaching(10000);
        scan.setCacheBlocks(true);
        scan.addColumn(Bytes.toBytes("data"), null);
        Filter filter = new PrefixFilter(Bytes.toBytes(prefix));        
        scan.setFilter(filter);
        
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        
        ResultScanner resultScanner = table.getScanner(scan);
        int iRows = 0;
        for (Result result : resultScanner) {    
            iRows ++;                
            String key = Bytes.toString(result.getRow());
            String epochTimeStr = key.substring(prefix.length());            
            IntervalData intervalData = new IntervalData();
            intervalData.epochTime = Long.parseLong(epochTimeStr);
            intervalData.value = Bytes.toFloat(result.getValue(Bytes.toBytes("data"), null));
            intervalDataList.add(intervalData);
        }   
        resultScanner.close();
        table.close();
        intervalDataSet.intervalData = intervalDataList.toArray(new IntervalData[intervalDataList.size()]);

        stopWatch.stop();       
        long time = stopWatch.getTime();
   
        System.out.println("Scanned " + iRows + " rows in " + time + " ms");
        
        return intervalDataSet;
    }
   
    // Writes a single interval data set to HBase. (Calls multi-dataset function to do the write - this
    // wrapper is to be a convenience to callers.)
    public static void writeIntervalDataSet(IntervalDataSet intervalDataSet) throws IOException {
        // Turn into a single-element list and call the multi-dataset function to write
        List<IntervalDataSet> intervalDataSets = new ArrayList<IntervalDataSet>();
        intervalDataSets.add(intervalDataSet);        
        writeIntervalDataSets(intervalDataSets);
    }

    // Writes multiple data sets to HBase
    public static void writeIntervalDataSets(List<IntervalDataSet> intervalDataSets) throws IOException {
        Configuration config = GetHBaseConfig();               
        HTable table = new HTable(config, "intervaldata");
        
        List<Put> putList = new ArrayList<Put>();
        for (IntervalDataSet intervalDataSet : intervalDataSets) {
            appendPutsForIntervalDataSet(putList, intervalDataSet);
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        table.put(putList);
        table.flushCommits();
        table.close();
        stopWatch.stop();       
        long time = stopWatch.getTime();
        double timePerRow = (double) time / (double) putList.size();
        double timePerDataSet = (double) time / (double) intervalDataSets.size();
        System.out.println(String.format("Write time: %d ms for %d rows, %d data sets. %.2f ms per row, %.0f ms per data set",
                time, putList.size(), intervalDataSets.size(), timePerRow, timePerDataSet));
    }

    // helper function to write data
    static void appendPutsForIntervalDataSet(List<Put> putList, IntervalDataSet intervalDataSet) {
        for (IntervalData intervalData : intervalDataSet.intervalData) {
            String rowkey = intervalDataSet.meterId + "-" + intervalData.epochTime;
            Put put = new Put(toBytes(rowkey));
            put.add(toBytes("data"), null, toBytes(intervalData.value));
            putList.add(put);
        }
    }
    
    // Generates test data
    public static IntervalDataSet generateIntervalDataSet(String meterId) {
        IntervalDataSet intervalDataSet = new IntervalDataSet();
        intervalDataSet.meterId = meterId;
        IntervalData[] intervalData = new IntervalData[8760];
        intervalDataSet.intervalData = intervalData;
        String str = "01/01/2014 00:00:00";
        SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        Date date = null;
                       
        try {
            date = df.parse(str);
        } catch (ParseException e) {
        }
        long epochTime = date.getTime();

        for (int i = 0; i < intervalData.length; i++) {
            intervalData[i] = new IntervalData();
            intervalData[i].epochTime = epochTime;
            double scale = (Math.random() > 0.9 ? 5.0 : 2.0);
            intervalData[i].value = (float) (Math.random() * scale);
            epochTime += 60;
        }

        return intervalDataSet;
    }
}
