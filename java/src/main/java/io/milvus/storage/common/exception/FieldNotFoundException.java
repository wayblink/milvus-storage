package io.milvus.storage.common.exception;

public class FieldNotFoundException extends MilvusStorageException {

    public FieldNotFoundException(String field) {
        super("Field not found: " + field);
    }
}
