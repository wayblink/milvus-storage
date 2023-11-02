package io.milvus.storage.fs.s3;

import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;

import java.net.URI;
//import java.net.URL;

public class TestS3FileSystem {

    @Test
    public void testURI() throws Exception {
       URI uri = new URI("s3://username:password@bucket/path?endpoint_override=localhost%3A9000");
       String userInfo = uri.getUserInfo();
       String bucket = uri.getHost();
       String path = uri.getPath();
       String queryString = uri.getQuery();
       String scheme = uri.getScheme();
       Map<String, String> querys = S3Util.splitQuery(uri);


       System.out.println(uri);
    }
}
