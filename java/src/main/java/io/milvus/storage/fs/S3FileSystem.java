package io.milvus.storage.fs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class S3FileSystem implements FileSystem {
    private String bucket;
    private String region;
    private String accessKey; // ak
    private String secretKey; // sk
    private S3AFileSystem innerFs;

    @Override
    public File Open(String path) {
        return null;
    }

    @Override
    public void Rename(String src, String dst) {

    }

    @Override
    public void Delete(String path) {

    }

    @Override
    public void CreateDir(String path) {

    }

    @Override
    public List<File> List(String path) {
        return null;
    }

    @Override
    public byte[] Read(String path) {
        return new byte[0];
    }

    @Override
    public boolean Exist(String path) {
        return false;
    }

    @Override
    public String Path() {
        return null;
    }
}
