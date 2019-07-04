/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
 /*
https://github.com/larsgeorge/hbase-book/tree/master/ch05/src/main/java/admin code reference.

 */
package HbaseSchemaInference.control;

import HbaseSchemaInference.model.PutData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class HbaseOperations {

    private Connection connection;

    public User getUser() throws IOException {
        return User.getCurrent();
    }

    /*
        return a connection or null on erro.
     */
    public Connection connect() {
        if (connection != null) {
            return connection;
        }
        try {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection();
            return connection;
        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /*
        return 
         1 if namespace already exists
         0 on sucess 
        -1 on erro
        
     */
    public short createNamespace(String name) {
        Admin admin = null;
        Connection connection = connect();
        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return -1;

        }

        try {
            admin.getNamespaceDescriptor(name);

            return 1;
        } catch (IOException ex) {
            try {
                NamespaceDescriptor namespace = NamespaceDescriptor.create(name).build();
                admin.createNamespace(namespace);
            } catch (IOException ex1) {
                Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex1);
            }
        }

        return 0;

    }

    /*
        return 
         1 if table already exists
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 error on creation
        
     */
    public short createTable(String namespace, String name, String[] familyName) {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return -1;

        }

        try {
            admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {

            return -2;
        }

        TableName tableName = TableName.valueOf(namespace, name);

        try {
            admin.getTableDescriptor(tableName);

            return 1;
        } catch (IOException ex) {
        }

        HTableDescriptor desc = new HTableDescriptor(tableName);

        for (String family : familyName) {
            HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(family));
            desc.addFamily(coldef);
        }
        try {
            admin.createTable(desc);
        } catch (IOException ex) {

            return -3;
        }

        return 0;

    }

    /*
        return 
         1 all families already exists
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 table dont exists
        -4 error on modify
        
     */
    public short alterFamilies(String namespace, String table, String[] familyName) {
        Admin admin = null;
        ;
        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {
            Logger.getLogger(HbaseOperations.class.getName()).log(Level.SEVERE, null, ex);

            return -1;

        }

        try {
            admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {

            return -2;
        }

        TableName tableName = TableName.valueOf(namespace, table);
        HTableDescriptor desc = null;
        try {
            desc = admin.getTableDescriptor(tableName);
        } catch (IOException ex) {

            return -3;
        }
        boolean allFamilyExists = true;
        for (String family : familyName) {
            HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes(family));
            if (!desc.hasFamily(Bytes.toBytes(family))) {
                allFamilyExists = false;
                desc.addFamily(coldef);
            }
        }
        if (allFamilyExists) {

            return 1;
        }
        try {
            admin.modifyTable(tableName, desc);
        } catch (IOException ex) {

            return -4;
        }

        return 0;

    }

    /*
        return
        0 on sucess
        -1 table not founded
        -2 put error
    
     */
    public short putData(String namespace, String table, String row, byte[] family, byte[] column, byte[] value) {

        TableName tableName = TableName.valueOf(namespace, table);
        byte[] rowName = Bytes.toBytes(row);
        Put p = new Put(rowName);
        Table tableClass = null;
        try {
            tableClass = connection.getTable(tableName);
        } catch (IOException ex) {

            return -1;
        }
        p.addColumn(family, column, value);
        try {
            tableClass.put(p);
        } catch (IOException ex) {

            return -2;
        }

        return 0;

    }

    /*
        return
        0 on sucess
        -1 table not founded
        -2 put error
    
     */
    public short putArrayOfData(String namespace, String table, String row, ArrayList<PutData> data) {

        TableName tableName = TableName.valueOf(namespace, table);
        byte[] rowName = Bytes.toBytes(row);
        Put p = new Put(rowName);
        Table tableClass = null;
        try {
            tableClass = connection.getTable(tableName);
        } catch (IOException ex) {

            return -1;
        }
        for (PutData entry : data) {
            p.addColumn(entry.getFamily(), entry.getColumn(), entry.getValue());
        }
        try {
            tableClass.put(p);
        } catch (IOException ex) {

            return -2;
        }

        return 0;

    }

    /*
        return 
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 error on disable and delete
        
     */
    public short deleteTable(String namespace, String table) {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return -1;

        }

        try {
            admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {

            return -2;
        }

        TableName tableName = TableName.valueOf(namespace, table);

        try {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException ex) {

            return -3;
        }

        return 0;
    }

    /*
        return 
         0 on sucess 
        -1 on erro
        -2 namespace dont exists
        -3 error on disable and delete
        
     */
    public short deleteNamespace(String namespace) {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return -1;

        }
        NamespaceDescriptor namespaceDescriptor = null;
        try {
            namespaceDescriptor = admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {

            return -2;
        }
        String[] tables = this.getTables(namespace);
        for (String table : tables) {
            deleteTable(namespace, table);
        }

        try {
            admin.deleteNamespace(namespace);
        } catch (IOException ex) {

            return -3;
        }

        return 0;
    }


    /*
        return the table names of a namespace
     */
    public String[] getTables(String namespace) {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return null;

        }
        NamespaceDescriptor namespaceDescriptor;
        try {
            namespaceDescriptor = admin.getNamespaceDescriptor(namespace);

        } catch (IOException ex) {

            return null;
        }

        HTableDescriptor[] tables = new HTableDescriptor[0];
        try {
            tables = admin.listTableDescriptorsByNamespace(namespace);
        } catch (IOException ex) {

            return null;
        }
        String[] tableNames = new String[tables.length];
        for (int i = 0; i < tables.length; i++) {
            tableNames[i] = tables[i].getNameAsString().split(":")[1];

        }

        return tableNames;
    }

    /*
        return the table names of a namespace
     */
    public String[] getFamilies(String namespace, String table) {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return null;

        }

        TableName tableName = TableName.valueOf(namespace, table);
        HTableDescriptor desc = null;
        try {
            desc = admin.getTableDescriptor(tableName);
        } catch (IOException ex) {

            return null;
        }

        HColumnDescriptor[] columnFamilies = desc.getColumnFamilies();
        String[] familyNames = new String[columnFamilies.length];
        for (int j = 0; j < columnFamilies.length; j++) {
            familyNames[j] = columnFamilies[j].getNameAsString();
        }

        return familyNames;
    }

    public String[] getColumns(String namespace, String table, String family) {
        String[] columnNames = new String[0];

        TableName tableName = TableName.valueOf(namespace, table);

        Table table2 = null;
        try {
            table2 = connection.getTable(tableName);
        } catch (IOException ex) {

            return null;
        }
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        try {
            ResultScanner scanner = table2.getScanner(scan);
            for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
                List<Cell> family_cells = result2.listCells();
                String[] tempNames = new String[family_cells.size()];
                int i = 0;
                for (Cell family_cell : result2.listCells()) {
                    byte[] rowArray = family_cell.getRowArray();
                    byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                    tempNames[i] = Bytes.toString(column);
                    i++;
                }
                columnNames = combine(columnNames, tempNames);
            }
        } catch (IOException ex) {

            return null;
        }

        return columnNames;
    }

    public ResultScanner getValuesScan(String namespace, String table, String family, String column) {

        TableName tableName = TableName.valueOf(namespace, table);

        Table table2 = null;
        try {
            table2 = connection.getTable(tableName);
        } catch (IOException ex) {

            return null;
        }
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        ResultScanner scanner = null;
        try {
            scanner = table2.getScanner(scan);

        } catch (IOException ex) {

            return null;
        }

        return scanner;
    }

    public ResultScanner getTableScan(String namespace, String table) {

        TableName tableName = TableName.valueOf(namespace, table);

        Table table2 = null;
        try {
            table2 = connection.getTable(tableName);
        } catch (IOException ex) {

            return null;
        }
        Scan scan = new Scan();
        ResultScanner scanner = null;
        try {
            scanner = table2.getScanner(scan);

        } catch (IOException ex) {

            return null;
        }

        return scanner;
    }

    public short deleteColumn(String namespace, String table, String row, String family, String column) {

        TableName tableName = TableName.valueOf(namespace, table);

        Table table2 = null;
        try {
            table2 = connection.getTable(tableName);
        } catch (IOException ex) {

            return -1;
        }
        Delete delete = new Delete(Bytes.toBytes(row));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        try {
            table2.delete(delete);
        } catch (IOException ex) {
            return -2;
        }
        return 0;
    }

    /*
        https://javarevisited.blogspot.com/2013/02/combine-integer-and-string-array-java-example-tutorial.html
     */
    public String[] combine(String[] a, String[] b) {
        int length = a.length + b.length;
        String[] result = new String[length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    public String[] getNamespaces(boolean clear, String[] management) {
        Admin admin = null;

        try {
            admin = connection.getAdmin();

        } catch (IOException ex) {

            return new String[0];

        }
        NamespaceDescriptor[] namespaceDescriptor;
        String[] namespaces;
        try {
            namespaceDescriptor = admin.listNamespaceDescriptors();
            namespaces = new String[namespaceDescriptor.length];
            int i = 0;
            for (NamespaceDescriptor namespace : namespaceDescriptor) {
                String name = namespace.getName();
                if (!name.contains(management[3]) && !management[0].equals(name)) {
                    namespaces[i] = namespace.getName();
                    i++;
                }
            }
            return namespaces;
        } catch (IOException ex) {

            return new String[0];
        }
    }

    String[] getSchemes(String managementNamespace, String managementTable, String managementFamily, String namespace) {
        String[] namespaceNames = new String[0];

        TableName tableName = TableName.valueOf(managementNamespace, managementTable);

        Table table2 = null;
        try {
            table2 = connection.getTable(tableName);
        } catch (IOException ex) {

            return null;
        }
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(managementFamily));
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(namespace))));
        try {
            ResultScanner scanner = table2.getScanner(scan);
            for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
                List<Cell> family_cells = result2.listCells();
                String[] tempNames = new String[family_cells.size()];
                int i = 0;
                for (Cell family_cell : result2.listCells()) {
                    byte[] rowArray = family_cell.getRowArray();
                    byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                    tempNames[i] = Bytes.toString(column);
                    i++;
                }
                namespaceNames = combine(namespaceNames, tempNames);
            }
        } catch (IOException ex) {

            return null;
        }

        return namespaceNames;
    }

}
