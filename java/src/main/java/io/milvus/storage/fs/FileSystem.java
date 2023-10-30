package io.milvus.storage.fs;

import io.milvus.storage.format.parquet.ParquetFileWriter;

import java.io.File;
import java.util.List;

public interface FileSystem {
    File Open(String path);
    void Rename(String src, String dst);
    void Delete(String path);
    void CreateDir(String path);
    List<File> List(String path);
    byte[] Read(String path);
    boolean Exist(String path);
    String Path();
}
