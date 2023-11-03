package io.milvus.storage.fs.s3;

import io.milvus.storage.fs.File;
import io.milvus.storage.fs.FileStatus;
import io.milvus.storage.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class S3FileSystem implements FileSystem {
    // Constant
    public final static String ENDPOINT_OVERRIDE = "endpoint_override";
    public final static String REGION = "region";

    private String scheme;
    private URI uri;
    private String bucket;
    private String rootPath;
    private String accessKey; // ak
    private String secretKey; // sk
    private String endpoint;
    private String region;
    private org.apache.hadoop.fs.FileSystem innerFs;

    // uri should be s3://username:password@bucket/path?endpoint_override=localhost%3A9000
    public S3FileSystem(URI uri) throws IOException {
        this.uri = uri;
        String userInfo = uri.getUserInfo();
        this.accessKey = userInfo.split(":")[0];
        this.secretKey = userInfo.split(":")[1];
        this.bucket = uri.getHost();
        this.rootPath = uri.getPath();
        this.scheme = uri.getScheme();
        Map<String, String> querys = S3Util.splitQuery(uri);
        if (querys.containsKey(ENDPOINT_OVERRIDE)) {
            this.endpoint = querys.get(ENDPOINT_OVERRIDE);
        }
        if (querys.containsKey(REGION)) {
           this.region = querys.get(REGION);
        }
        this.initialize();
    }

    private void initialize() throws IOException {
//        S3AFileSystem s3 = new S3AFileSystem();
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", endpoint);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.ssl.enabled", "true");
        conf.set("fs.s3a.access.key", this.accessKey);
        conf.set("fs.s3a.secret.key", this.secretKey);
        org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem.get(conf);
        this.innerFs = fileSystem;
    }

    @Override
    public File open(String path) {
        return new S3File(path);
    }

    @Override
    public boolean rename(String src, String dst) throws IOException {
        return innerFs.rename(new Path(src), new Path(dst));
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException {
        return innerFs.delete(new Path(path), recursive);
    }

    @Override
    public boolean mkdir(String path) {
        return true;
    }

    @Override
    public FileStatus[] list(String path) throws IOException {
        org.apache.hadoop.fs.FileStatus[] fileStatues = innerFs.listStatus(new Path(path));
        FileStatus[] res = new FileStatus[fileStatues.length];
        for (int i = 0; i < fileStatues.length; i++) {
            res[i] = new S3FileStatus(fileStatues[i], this);
        }
        return res;
    }

    @Override
    public FileStatus getFileStatus(String path) throws IOException {
        org.apache.hadoop.fs.FileStatus fileStatus = innerFs.getFileStatus(new Path(path));
        return new S3FileStatus(fileStatus, this);
    }

    @Override
    public byte[] read(String path) {
        return new byte[0];
    }

    @Override
    public boolean exist(String path) throws IOException {
        return innerFs.exists(new Path(path));
    }

    @Override
    public String path() {
        return this.rootPath;
    }

    @Override
    public URI getUri() {
        return this.uri;
    }
}
