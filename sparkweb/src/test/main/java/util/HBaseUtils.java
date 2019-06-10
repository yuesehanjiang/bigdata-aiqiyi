package util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.*;

/**
 * HBase 操作工具类
 */
public class HBaseUtils {
    HBaseAdmin admin = null;
    Configuration configration=null;

    /**
     * 私有构造方法
     */
    private HBaseUtils() {
        configration = new Configuration();
        configration.set("hbase.zookeeper.quorum", "had101:2181");
        configration.set("hbase.rootdir", "hdfs://had101/hbase");
        try {
            admin = new HBaseAdmin(configration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance=null;

    /**
     * 获取单实例对象 *@return
     */
    public static synchronized HBaseUtils getInstance() {
        if (null == instance) {
            instance = new HBaseUtils();

            return instance;
        }
        return instance;
    }

    /**
     * 根据表明获取到 Htable 实例 *@paramtableName *@return
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configration, tableName);
        } catch (Exception e) {
            e.printStackTrace();
        } return table;
    }

    /**
     * 添加一条记录到 Hbase 表 703012832 核 200T8000 * *@paramtableNameHbase 表名 *@paramrowkey Hbase 表的 rowkey *@paramcf Hbase 表的 columnfamily *@paramcolumn Hbase 表的列 *@paramvalue 写入 Hbase 表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value) {
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名输入条件获取 Hbase 的记录数
     */
    public Map<String, String> query(String tableName, String condition)throws IOException {
        Map<String, String> map = new HashMap <>();
        HTable table = getTable(tableName);
        String cf = "info";
        String qualifier = "click_count";
        Scan scan = new Scan();
        Filter filter = new PrefixFilter(Bytes.toBytes(condition));
        scan.setFilter(filter);


      ResultScanner rs = table.getScanner(scan);
        for (Result result:rs) {
            String row = Bytes.toString(result.getRow());

            String clickCount = Bytes.toString(result.getValue(cf.getBytes(), qualifier.getBytes()));
            String  clickCount1 = String.valueOf(clickCount);
            map.put(row, clickCount);
        }
        return map;
    }

    public static void main(String[] args)throws IOException {
        // HBaseUtils.getInstance().getAllRows("student");
        HBaseUtils.getInstance().query("student","");











    }




    /**
     * 得到所有的数据
     * @param tableName
     * @throws IOException
     */
    public  void getAllRows(String tableName) throws IOException{
        configration = new Configuration();
        configration.set("hbase.zookeeper.quorum", "had101:2181");
        configration.set("hbase.rootdir", "hdfs://had101/hbase");
        HTable hTable = new HTable(configration, tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

    }







}