import java.io.BufferedReader;
import java.io.File;  
import java.io.FileReader;  
import java.io.IOException; 
import java.util.*;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*; 
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
public class my_hbase {
    public static void main(String[] args) throws IOException, Exception {
        System.out.println("Hello HBase");
	if (args.length > 2) {
	    String tableName = args[0];
	    Configuration conf = HBaseConfiguration.create();
	    Connection conn = ConnectionFactory.createConnection(conf);
	    Table hTable = conn.getTable(TableName.valueOf(tableName));

	    // Scan for all time range
	    ScanAllValues(hTable);

	    String rowKey = args[1];
	    Get g = new Get(Bytes.toBytes(rowKey));
	    System.out.println("Current row: " + Bytes.toString(g.getRow()));
	    g = g.setTimeRange(1509605787459L, 1510352640108L + 1L);
	    
	    Result result = hTable.get(g); 
	    Cell[] cells = result.rawCells();
	    System.out.println("No. cells: " + cells.length);
    	    for (int i = 0; i < cells.length; i++) {
		int valueOffset = cells[i].getValueOffset();
		int valueLength = cells[i].getValueLength();
		byte[] value = Arrays.copyOfRange(cells[i].getValueArray(), valueOffset, valueOffset + valueLength);
		System.out.println(i + " Range value: " + Bytes.toString(value));
	    }

	    /*
	    String cfName = args[2];
            String cfQualifier = args[3];
	    byte [] value = result.getValue(Bytes.toBytes(cfName), Bytes.toBytes(cfQualifier));
    	    String res = Bytes.toString(value);

	    System.out.println("Result: " + res);
	    Date d = new Date(1509605787459L);
            System.out.println("test date: " + d.toString());
	    */
	}
    }
    private static void ScanAllValues(Table hTable) throws IOException, Exception {
	Scan scan = new Scan();
	byte[] startRow = scan.getStartRow();
   	ResultScanner scanner = hTable.getScanner(scan);
    	for (Result r = scanner.next(); (r != null); r = scanner.next()) {
            System.out.println("Row key: " + Bytes.toString(r.getRow()));
            List<Cell> cells = r.listCells();
            for (Cell cell : cells) {
                int valueOffset = cell.getValueOffset();
            	int valueLength = cell.getValueLength();
            	byte[] value = Arrays.copyOfRange(cell.getValueArray(), valueOffset, valueOffset + valueLength);
            	System.out.println("Value: " + Bytes.toString(value));
       	    }
        }
    }
}
