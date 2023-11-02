package io.milvus.storage.fs;

import io.milvus.storage.fs.local.LocalFileSystem;
//import io.milvus.storage.fs.s3.S3FileSystem;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;

public class FsFactory {

    public static FileSystem Create(FsType fsType, URI uri) throws IOException {
        switch (fsType) {
            case InMemory:
                throw new Error("memory fs not supported yet");
            case LocalFS:
                return new LocalFileSystem();
//            case S3:
//                return new S3FileSystem(uri);
            default:
                throw new Error("unknown fs type");
        }
    }
}
