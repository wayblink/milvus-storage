package io.milvus.storage.common.exception;

public abstract class MilvusStorageException extends Exception{

    public MilvusStorageException(String message) {
        super(message);
    }
}
