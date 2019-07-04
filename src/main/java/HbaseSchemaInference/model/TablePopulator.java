/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package HbaseSchemaInference.model;

import HbaseSchemaInference.control.HbaseOperations;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author eduardo
 */
public class TablePopulator {

    String[] types = {"byte", "boolean", "string", "short", "char", "float", "integer", "double", "long", "blob"};
    String namespace, table;
    ArrayList<String> families, rows;

    public TablePopulator(String namespace, String table, int numFamilies, int numRows) {
        this.namespace = namespace;
        this.table = table;
        families = new ArrayList<String>();
        rows = new ArrayList<String>();
        for (int i = 0; i < numFamilies; i++) {
            families.add("f" + i);
        }
        for (int i = 0; i < numRows; i++) {
            rows.add("r" + i);
        }
        HbaseOperations ops = new HbaseOperations();
        ops.createNamespace(namespace);
        String[] fam = families.toArray(new String[families.size()]);  
        short tab = ops.createTable(namespace, table, fam);
        if(tab == 1){
            ops.alterFamilies(namespace, table, fam);
        }
        for (int i = numRows; i > 0; i--) {
            for (int j = 0; j < types.length; j++) {
                String family = families.get(ThreadLocalRandom.current().nextInt(0, families.size()));
                for (int k = 0; k < i; k++) {
                    byte[] value = null;
                    switch (types[j]) {
                        case "byte":
                            value = new byte[]{makeByte()};
                            break;
                        case "boolean":
                            value = new byte[]{makeBoolean()};
                            break;
                        case "string":
                            value = makeString(0);
                            break;
                        case "short":
                            value = makeShort();
                            break;
                        case "char":
                            value = makeChar();
                            break;
                        case "float":
                            value = makeFloat();
                            break;
                        case "integer":
                            value = makeInt();
                            break;
                        case "double":
                            value = makeDouble();
                            break;
                        case "long":
                            value = makeLong();
                            break;
                        case "blob":
                            value = makeBlob(0);
                            break;

                    }
                    ops.putData(namespace, table, rows.get(k), Bytes.toBytes(family), Bytes.toBytes((types[j] + i)), value);
                }
            }
        }
    }

    public String getNamespace() {
        return namespace;
    }

    public String getTable() {
        return table;
    }

    public ArrayList<String> getFamilies() {
        return families;
    }

    public ArrayList<String> getRows() {
        return rows;
    }

    //data generators
    /* datatypes
        *byte
        *boolean
        *string
        *short
        *char
        *float
        *integer
        *double
        *long
        *blob
    
     */
    public byte makeByte() {
        byte output = (byte) ThreadLocalRandom.current().nextInt(0x0, 0xFF + 1);
        return output;
    }

    public byte makeBoolean() {
        byte output = (byte) 0xFF;
        int rand = ThreadLocalRandom.current().nextInt(0, 2);
        if (rand == 0) {
            output = (byte) 0x00;
        }
        return output;
    }

    public byte[] makeString(int lenght) {
        byte[] output = new byte[0];
        int[] cMin = new int[5], cMax = new int[5];
        cMin[0] = 0x80;
        cMax[0] = 0xBF;
        cMin[1] = 0x1;
        cMax[1] = 0x7E;
        cMin[2] = 0xC2;
        cMax[2] = 0xDF;
        cMin[3] = 0xE0;
        cMax[3] = 0xEF;
        cMin[4] = 0xF0;
        cMax[4] = 0xF5;

        if (lenght == 0) {
            lenght = getRandom(1, 3000);
        }
        int size = 0;

        while (size < lenght) {
            int rest = lenght - size;
            int byteLen = 0;

            switch (rest) {
                case 1:
                    byteLen = 1;
                    break;
                case 2:
                    byteLen = ThreadLocalRandom.current().nextInt(1, 3);
                    break;
                case 3:
                    byteLen = ThreadLocalRandom.current().nextInt(1, 4);
                    break;
                default:
                    byteLen = ThreadLocalRandom.current().nextInt(1, 5);

            }
            byte[] atual = new byte[byteLen];
            atual[0] = (byte) ThreadLocalRandom.current().nextInt(cMin[byteLen], cMax[byteLen] + 1);
            for (int i = 1; i < byteLen; i++) {
                atual[i] = (byte) ThreadLocalRandom.current().nextInt(cMin[0], cMax[0] + 1);
            }
            output = combine(output, atual);
            size++;
        }
        return output;
    }

    public byte[] makeShort() {
        byte[] output = new byte[2];
        short rand = (short) ThreadLocalRandom.current().nextInt(0x0, 0xFFFF + 1);
        output = Bytes.toBytes(rand);
        return output;
    }

    public byte[] makeChar() {
        byte[] output = new byte[4];
        char rand = (char) ThreadLocalRandom.current().nextInt(0x1, 0xFF65 + 1);
        output = Bytes.toBytes(rand);
        return output;
    }

    public byte[] makeFloat() {
        byte[] output = new byte[2];
        float rand = ThreadLocalRandom.current().nextFloat() * ThreadLocalRandom.current().nextInt(0x1, 100000) * (-ThreadLocalRandom.current().nextInt(0, 2));
        output = Bytes.toBytes(rand);
        return output;
    }

    public byte[] makeInt() {
        byte[] output = new byte[4];
        int rand = ThreadLocalRandom.current().nextInt();
        output = Bytes.toBytes(rand);
        return output;
    }

    public byte[] makeDouble() {
        byte[] output = new byte[8];
        double rand = ThreadLocalRandom.current().nextDouble(-2 ^ 50, 2 ^ 50);
        output = Bytes.toBytes(rand);
        return output;
    }

    public byte[] makeLong() {
        byte[] output = new byte[8];
        long rand = ThreadLocalRandom.current().nextLong();
        output = Bytes.toBytes(rand);
        return output;
    }

    public byte[] makeBlob(int lenght) {

        if (lenght == 0) {
            lenght = getRandom(10, 100000);
        }

        byte[] output = new byte[lenght];
        for (int i = 1; i < lenght; i++) {
            output[i] = (byte) ThreadLocalRandom.current().nextInt(0x0, 0xFF + 1);
        }

        return output;
    }

    //end data generators
    private int getRandom(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

    /*
        https://javarevisited.blogspot.com/2013/02/combine-integer-and-string-array-java-example-tutorial.html
     */
    public byte[] combine(byte[] a, byte[] b) {
        int length = a.length + b.length;
        byte[] result = new byte[length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}
