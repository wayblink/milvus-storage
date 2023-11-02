package io.milvus.storage.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public interface FileSystem {
    File open(String path);
    boolean rename(String src, String dst) throws IOException;
    boolean delete(String path, boolean recursive) throws IOException;
    boolean mkdir(String path) throws IOException;
    FileStatus[] list(String path) throws IOException;
    byte[] read(String path) throws IOException;
    boolean exist(String path) throws IOException;
    // seems useless
    String path();


    URI getUri();
}
