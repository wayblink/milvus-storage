package io.milvus.storage.fs;

import java.io.File;
import java.util.List;

public class LocalFileSystem implements FileSystem{
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
