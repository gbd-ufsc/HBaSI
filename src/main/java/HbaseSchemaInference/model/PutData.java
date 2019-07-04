/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package HbaseSchemaInference.model;

/**
 *
 * @author eduardo
 */
public class PutData {

    byte[] family, column, value;

    public PutData(byte[] family, byte[] column, byte[] value) {
        this.family = family;
        this.column = column;
        this.value = value;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getColumn() {
        return column;
    }

    public byte[] getValue() {
        return value;
    }
    

}
