package io.milvus.storage.fs.local;

import org.junit.Assert;
import org.junit.Test;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

public class TestLocalFileSystem {
    private final static String tempFile = "tempFile";

//    @After
//    public void after() {
//
//    }

    @Test
    public void testWriteFile() throws Exception {
        LocalFileSystem fs = new LocalFileSystem();

        FileOutputStream fos = fs.open(tempFile, true);
        String content = "milvus-storage";
        fos.write(content.getBytes());
        fos.close();
    }

    @Test
    public void testReadFile() throws Exception {
        LocalFileSystem fs = new LocalFileSystem();

        byte[] bytes = fs.read(tempFile);
        String s = new String(bytes, StandardCharsets.UTF_8);
        String content = "milvus-storage";
        Assert.assertEquals(s, content);
    }

    @Test
    public void testMkdirAndDelete() throws Exception {
        LocalFileSystem fs = new LocalFileSystem();
        boolean succeed1 = fs.mkdir("a/b/c");
        Assert.assertEquals(true, succeed1);

        boolean exist = fs.exist("a/b/c");
        Assert.assertEquals(true, exist);

        fs.delete("a", true);
        boolean exist2 = fs.exist("a/b/c");
        Assert.assertEquals(false, exist2);
    }

    @Test
    public void testOpenAndDelete() throws Exception {
        LocalFileSystem fs = new LocalFileSystem();
        fs.open("a/b/c", true);

        boolean exist = fs.exist("a/b/c");
        Assert.assertEquals(true, exist);

        fs.delete("a", true);
        boolean exist2 = fs.exist("a/b/c");
        Assert.assertEquals(false, exist2);
    }


}
