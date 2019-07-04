/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package HbaseSchemaInference.control;

import HbaseSchemaInference.model.RawSchema;
import HbaseSchemaInference.view.MainView;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class App {

    //hbase namespace, man table, man family, new namespace ident
    static String[] manageNamespace = {"hBaseSchemaInference", "rawSchemas", "data", "_rawSchema_"};
    static String inferenceNamespace2 = "tests2", inferenceNamespace = "tests";
    static String inferenceTable = "testTable";
    private MainView view;
    private HbaseOperations ops;

    public static void main(String[] args) {
        new App();
    }

    public App() {
        ops = new HbaseOperations();
        ops.createNamespace(manageNamespace[0]);
        ops.createTable(manageNamespace[0], manageNamespace[1], (new String[]{manageNamespace[2]}));
        view = new MainView(this);
        view.setVisible(true);
        //ops.deleteNamespace(namespace);
        //ops.deleteNamespace("tests_rawSchema_15551"+"11674769");
    }

    public String[] getNamespaces() {
        return ops.getNamespaces(true, manageNamespace);
    }

    public String[] getSchemes(String namespace) {
        return ops.getSchemes(manageNamespace[0], manageNamespace[1], manageNamespace[2], namespace);
    }

    public String getSchema(String namespace) {
        String[] split = namespace.split("_");
        RawSchema rawSchema = new RawSchema(split[0], ops, namespace, this);
        return rawSchema.rawToJSON();
    }

    public void newSchema(String namespace) {
        Date date = new Date();
        String newNamespace = namespace + manageNamespace[3] + date.getTime();
        ops.putData(manageNamespace[0], manageNamespace[1], namespace, Bytes.toBytes(manageNamespace[2]), Bytes.toBytes(newNamespace), Bytes.toBytes(newNamespace));
        RawSchema rawSchema = new RawSchema(namespace, ops, newNamespace, this);
        new Thread() {

            @Override
            public void run() {
                long rawTime = rawSchema.getRawSchema();
                view.newSchema(date, rawTime);
            }
        }.start();

    }

    public void updateStatus(String text) {
        view.updateStatus(text);
    }

    public void deleteScheme(String selected) {
        ops.deleteNamespace(selected);
        String row = selected.split("_")[0];
        ops.deleteColumn(manageNamespace[0], manageNamespace[1],row,manageNamespace[2],selected);
    }

/////////////////////////////////////////////////////////////////////////////////
//Legacy 
    public static void scan_all(String table, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nFound row : " + Bytes.toString(row));
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result2.getMap();

            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                byte[] family = entry.getKey();
                System.out.println("family key : " + Bytes.toString(family));

                for (Entry<byte[], NavigableMap<Long, byte[]>> entry1 : entry.getValue().entrySet()) {
                    byte[] column = entry1.getKey();
                    System.out.println("column key : " + Bytes.toString(column));

                    for (Entry<Long, byte[]> entry2 : entry1.getValue().entrySet()) {
                        byte[] cellValue = entry2.getValue();
                        System.out.println("Cell Value: " + Bytes.toString(cellValue));
                    }
                }
            }
        }
        scanner.close();
    }

    private static void get_tables_and_families(String namespace, Configuration conf) throws ZooKeeperConnectionException, IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        HTableDescriptor[] tabelas = admin.listTableDescriptorsByNamespace(namespace);
        for (int i = 0; i < tabelas.length; i++) {
            String nome_tabela = tabelas[i].getNameAsString();
            System.out.println("Tabela " + nome_tabela);
            HColumnDescriptor[] columnFamilies = tabelas[i].getColumnFamilies();
            for (int j = 0; j < columnFamilies.length; j++) {
                String nome_familia = columnFamilies[j].getNameAsString();
                System.out.println("familia de " + nome_tabela + ": " + nome_familia);
            }
        }
    }

    private static void get_columns_and_values(String table, String family, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nLinha : " + Bytes.toString(row));

            List<Cell> family_cells = result2.listCells();
            for (Cell family_cell : result2.listCells()) {
                byte[] rowArray = family_cell.getRowArray();
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("Coluna : " + Bytes.toString(column));
                byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(), family_cell.getValueOffset() + family_cell.getValueLength());
                int typeByte = value[0];
                if (typeByte == 51) {
                    System.out.println("string");
                } else if (typeByte == 43) // do operation for Integer
                {
                    System.out.println("int");
                } else if (typeByte == 45) {
                    System.out.println("double");
                }
                // do operation for DoubleF
                System.out.println("Valor : " + Bytes.toString(value));

            }
        }
    }

    private static void get_columns(String table, String family, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nLinha : " + Bytes.toString(row));

            List<Cell> family_cells = result2.listCells();
            for (Cell family_cell : result2.listCells()) {
                byte[] rowArray = family_cell.getRowArray();
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("Coluna : " + Bytes.toString(column));
            }
        }
    }

    private static void binaryTests(String namespace, String table, String family, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nLinha : " + Bytes.toString(row));

            List<Cell> family_cells = result2.listCells();
            for (Cell family_cell : result2.listCells()) {
                byte[] rowArray = family_cell.getRowArray();
                byte[] column = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(), family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("\nColuna : " + Bytes.toString(column));
                byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(), family_cell.getValueOffset() + family_cell.getValueLength());
                for (byte b : value) {
                    //System.out.println(String.valueOf(b));
                }
                boolean print = false;
                if (print) {
                    byte[] dunno = Arrays.copyOfRange(rowArray, 0,
                            4);
                    System.out.println("dunno: "
                            + Bytes.toInt(dunno) + " - 0 - 4");

                    byte[] row1 = Arrays.copyOfRange(rowArray, family_cell.getRowOffset(),
                            family_cell.getRowOffset() + family_cell.getRowLength());
                    System.out.println("row1: "
                            + Bytes.toString(row1) + " - " + family_cell.getRowOffset() + " - "
                            + (family_cell.getRowOffset() + family_cell.getRowLength()));

                    byte bt = rowArray[family_cell.getFamilyOffset() - 1];
                    System.out.println(bt + " - " + (family_cell.getFamilyOffset() - 1) + " - " + (family_cell.getFamilyOffset() - 1));

                    byte[] fam = Arrays.copyOfRange(rowArray, family_cell.getFamilyOffset(),
                            family_cell.getFamilyOffset() + family_cell.getFamilyLength());
                    System.out.println("fam : "
                            + Bytes.toString(fam) + " - " + family_cell.getFamilyOffset() + " - "
                            + (family_cell.getFamilyOffset() + family_cell.getFamilyLength()));

                    byte[] qualif = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(),
                            family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                    System.out.println("qualif : "
                            + Bytes.toString(qualif) + " - " + family_cell.getQualifierOffset()
                            + " - " + (family_cell.getQualifierOffset() + family_cell.getQualifierLength()));

                    /*byte[]  tag= Arrays.copyOfRange(rowArray, family_cell.getTagsOffset(), 
                        family_cell.getTagsOffset()+family_cell.getTagsLength());
                System.out.println("tag : " + 
                Bytes.toString(tag) +" - "+family_cell.getTagsOffset()+
                        " - "+(family_cell.getTagsOffset()+family_cell.getTagsLength()));
                     */
                    long typ = family_cell.getTimestamp();
                    byte[] typ1 = Arrays.copyOfRange(rowArray, (family_cell.getQualifierOffset() + family_cell.getQualifierLength()),
                            family_cell.getQualifierOffset() + family_cell.getQualifierLength() + 8);

                    byte[] toBytes = Bytes.toBytes(typ);
                    System.out.println("stamp : " + typ + " - "
                            + (family_cell.getQualifierOffset() + family_cell.getQualifierLength()) + " - "
                            + (family_cell.getQualifierOffset() + family_cell.getQualifierLength() + 8));

                    bt = rowArray[family_cell.getValueOffset() - 1];
                    System.out.println("type: " + bt + " - " + (family_cell.getValueOffset() - 1) + " - " + (family_cell.getValueOffset() - 1));

                }

                byte[] val = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(),
                        family_cell.getValueOffset() + family_cell.getValueLength());
                System.out.println("value : "
                        + Bytes.toString(val) + " - " + family_cell.getValueOffset()
                        + " - " + (family_cell.getValueOffset() + family_cell.getValueLength()) + "\nlen " + family_cell.getValueLength());
                // do operation for DoubleF
                System.out.println("Valor : " + Bytes.toStringBinary(val));

                /* WEBTRASH
                String contentType = null;
                try {
                    contentType = URLConnection.guessContentTypeFromStream(
                            new ByteArrayInputStream(val));
                    System.out.println(contentType);
                } catch (IOException e) {
                    System.out.println("Could not guess content type");
                }

                contentType = new Tika().detect(val);
                System.out.println(contentType);
                 */
                for (int a = 0; a < rowArray.length; a++) {
                    //System.out.println("row : " +                             Bytes.toString(Arrays.copyOfRange(rowArray, a, a+1 ))+" - "+String.valueOf(Arrays.copyOfRange(rowArray, a, a+1 )[0]));
                }
                //System.out.println("row : " +                         Bytes.toString(Arrays.copyOfRange(rowArray, 0, 7 )));
                int a = 1;
                //System.out.println(35-26);
            }
        }
    }

    public static void dataTests(String namespace, String table, String family, Configuration conf) throws IOException {
        boolean bool1 = true, bool2 = false;
        char cha1 = 'a', cha2 = '9';
        short sho1 = 9;
        int in1 = 9, int2 = 57;
        long ln1 = 9;
        float ft1 = (float) 0.5;
        double db1 = 9.9; // long equal = 4621762822593629389
        String str1 = "ç";
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        byte[] row1 = Bytes.toBytes("eduardo");
        Put p = new Put(row1);
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("charypselly"), Bytes.toBytes('ç'));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("str1"), Bytes.toBytes(str1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("bool1"), Bytes.toBytes(bool1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("cha1"), Bytes.toBytes(cha1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("cha2"), Bytes.toBytes(cha2));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("sho1"), Bytes.toBytes(sho1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("in1"), Bytes.toBytes(in1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("in2"), Bytes.toBytes(int2));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("bool2"), Bytes.toBytes(bool2));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("ln1"), Bytes.toBytes(ln1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("ft1"), Bytes.toBytes(ft1));
        p.addColumn(Bytes.toBytes("teste"), Bytes.toBytes("db1"), Bytes.toBytes(db1));
        table2.put(p);
    }

    private static void typeTests(String namespace, String table, String family, Configuration conf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table2 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));

        ResultScanner scanner = table2.getScanner(scan);
        for (Result result2 = scanner.next(); result2 != null; result2 = scanner.next()) {
            byte[] row = result2.getRow();
            System.out.println("\nLinha : " + Bytes.toString(row));

            List<Cell> family_cells = result2.listCells();
            for (Cell family_cell : result2.listCells()) {
                byte[] rowArray = family_cell.getRowArray();
                byte[] fam = Arrays.copyOfRange(rowArray, family_cell.getFamilyOffset(),
                        family_cell.getFamilyOffset() + family_cell.getFamilyLength());
                byte[] qua = Arrays.copyOfRange(rowArray, family_cell.getQualifierOffset(),
                        family_cell.getQualifierOffset() + family_cell.getQualifierLength());
                System.out.println("\ncoluna : " + Bytes.toString(fam) + ":" + Bytes.toString(qua));

                byte[] value = Arrays.copyOfRange(rowArray, family_cell.getValueOffset(),
                        family_cell.getValueOffset() + family_cell.getValueLength());;
                int len = family_cell.getValueLength();
                //String valueOf = Bytes.toStringBinary(value);
                switch (len) {
                    case 1:
                        //bool or byte or string
                        /*
                        strings são representadas em UTF-8  e o método toStringBinary em unicode contudo como para um byte
                        a codificação é a mesma para ambos uma string
                        de um byte nunca contém o identificar de hexadecial "\x". ainda que isso nao garanta que o valor é uma string
                        pois o mesmo pode ser um byte.
                        
                        dentre os valores hexadecimais o FF e 00 são reservados para guardar valores booleanos, sendo 00 false e FF true.
                        
                        se qualquer outro valor hexadecimal for encontrado garantidamente o valor é do tipo byte.
                         */
                        if (isUtf8Valid(value)) {
                            //string or byte
                            System.out.println(Bytes.toString(value));

                        } else if (value[0] == -1 || value[0] == 0) {
                            //boolean or byte
                            System.out.println(Bytes.toBoolean(value));

                        } else {
                            //byte
                            System.out.println(Bytes.toStringBinary(value));
                        }
                        break;
                    case 2:
                        //short or string
                        /*
                         strings são representadas em UTF-8  e o método toStringBinary em unicode contudo como 
                        para dois bytes podem acontecer 3 casos:
                        uma string com dois caracteres de um byte(utf8), nao contem \x
                        uma string com um caractere de dois bytes(utf8), contem dois \x
                        uma entra dom penas um valor \x, garantidamente short
                        ainda que isso nao garanta que o valor é uma string
                        pois o mesmo pode ser um short.
                         */
                        if (isUtf8Valid(value)) {
                            //string or short
                            System.out.println(Bytes.toString(value));
                            System.out.println(Bytes.toShort(value));
                        } else {
                            //short
                            System.out.println(Bytes.toShort(value));
                        }
                        break;
                    case 3:
                        //string
                        if (isUtf8Valid(value)) {
                            System.out.println(Bytes.toString(value));
                        }
                        break;
                    case 4:
                        //char or float or integer or string
                        /*
                        esse é complicado, mas garantidamente pode-se dizer que não existe possibilidade de um 
                        valor ser char ou string ao mesmo tempo pois char tem os dois primeiros bytes \x00 e esse valor 
                        não é aceito como caractere de string
                         */
                        if (isUtf8Valid(value)) {
                            System.out.println(Bytes.toString(value));
                        } else if (value[0] == 0 && value[1] == 0 && (value[2] != 0 || value[3] != 0)) {
                            System.out.println(new String(value, "ISO-8859-1"));
                        }

                        if ((value[0] != -1 && value[0] != 127) || value[1] >= 0) {
                            System.out.println(Bytes.toFloat(value));
                        }

                        System.out.println(Bytes.toInt(value));
                        break;
                    case 5:
                    case 6:
                    case 7:
                        //string
                        if (isUtf8Valid(value)) {
                            System.out.println(Bytes.toString(value));
                        }
                        break;
                    case 8:
                        //double or long or string
                        if (isUtf8Valid(value)) {
                            System.out.println(Bytes.toString(value));
                        }
                        //01111111 11110000
                        if ((value[0] != -1 && value[0] != 127) || (value[1] < -16 || value[1] > -1)) {
                            System.out.println(Bytes.toDouble(value));
                        }

                        System.out.println(Bytes.toLong(value));
                        break;
                    default:
                        //string or blob
                        if (isUtf8Valid(value)) {
                            System.out.println(Bytes.toString(value));
                        } else {
                            System.out.println(Bytes.toString(value));
                        }

                }

            }
        }
    }

    public static boolean isUtf8Valid(byte[] value) {
        int c1Min = 0x1, c1Max = 0x7E;
        int c2Min = 0xC0, c2Max = 0xDF;
        int c3Min = 0xE0, c3Max = 0xEF;
        int c4Min = 0xF0, c4Max = 0xF7;
        int min = 0x80, max = 0xBF;

        for (int i = 0; i < value.length; i++) {
            if (value[i] >= c1Min && value[i] <= c1Max) {
                continue;
            } else if ((i + 1) < value.length && value[i] >= c2Min && value[i] <= c2Max) {

                if (value[i + 1] >= min && value[i + 1] <= max) {
                    i++;
                    continue;
                } else {
                    return false;
                }
            } else if ((i + 2) < value.length && value[i] >= c3Min && value[i] <= c3Max) {

                if (value[i + 1] >= min && value[i + 1] <= max
                        && value[i + 2] >= min && value[i + 2] <= max) {
                    i += 2;
                    continue;
                } else {
                    return false;
                }
            } else if ((i + 3) < value.length && value[i] >= c4Min && value[i] <= c4Max) {

                if (value[i + 1] >= min && value[i + 1] <= max
                        && value[i + 2] >= min && value[i + 2] <= max
                        && value[i + 3] >= min && value[i + 3] <= max) {
                    i += 3;
                    continue;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

}


/*
0-10 dunno

 */
