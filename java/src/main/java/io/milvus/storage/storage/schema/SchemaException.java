package io.milvus.storage.storage.schema;

public class SchemaException extends Exception {
    public static String ErrPrimaryColumnNotFound = "primary column not found";
    public static String ErrPrimaryColumnType     = "primary column is not int64 or string";
    public static String ErrPrimaryColumnEmpty    = "primary column is empty";
    public static String ErrVersionColumnNotFound = "version column not found";
    public static String ErrVersionColumnType     = "version column is not int64";
    public static String ErrVectorColumnNotFound  = "vector column not found";
    public static String ErrVectorColumnType      = "vector column is not fixed size binary or fixed size list";
    public static String ErrVectorColumnEmpty     = "vector column is empty";

    public SchemaException(String message) {
        super(message);
    }
}


