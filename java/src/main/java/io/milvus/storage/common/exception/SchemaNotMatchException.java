package io.milvus.storage.common.exception;

public class SchemaNotMatchException extends MilvusStorageException {

    public SchemaNotMatchException(String message) {
        super(message);
    }

    public SchemaNotMatchException(String schema, String schemb) {
        super("Schema mismatch a: " + schema + "b: " + schemb);
    }
}
