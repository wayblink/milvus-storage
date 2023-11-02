package io.milvus.storage.fs.local;

import com.amazonaws.services.dynamodbv2.xspec.S;
import io.milvus.storage.common.utils.OperatingSystem;
import io.milvus.storage.fs.FileStatus;
import io.milvus.storage.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.*;
import java.net.URI;
import java.nio.file.*;

/**
 * The class {@code LocalFileSystem} is an implementation of the {@link FileSystem} interface for
 * the local file system of the machine where the JVM runs.
 */
public class LocalFileSystem implements FileSystem{
    /** The URI representing the local file system. */
    private static final URI LOCAL_URI =
            OperatingSystem.isWindows() ? URI.create("file:/") : URI.create("file:///");

    private URI uri;

    public LocalFileSystem() {
//        this.uri = uri;
    }

    @Override
    public File open(String path) {
        File src = new File("test");
        src.mkdirs();
        return src;
    }

    public FileOutputStream open(String filePath, boolean overwrite) throws IOException {
        File file = new File(filePath);
        if (exist(filePath) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + filePath);
        }
        String parent = file.getParent();
        if (parent != null && !mkdir(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        return new FileOutputStream(file);
    }

    @Override
    public boolean rename(String src, String dst) throws IOException {
        final File srcFile = new File(src);
        final File dstFile = new File(dst);

        final File dstParent = dstFile.getParentFile();

        // Files.move fails if the destination directory doesn't exist
        // noinspection ResultOfMethodCallIgnored -- we don't care if the directory existed or was
        // created
        dstParent.mkdirs();

        try {
            Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (NoSuchFileException
                | AccessDeniedException
                | DirectoryNotEmptyException
                | SecurityException ex) {
            // catch the errors that are regular "move failed" exceptions and return false
            return false;
        }
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException {

        final File file = new File(path);
        if (file.isFile()) {
            return file.delete();
        } else if ((!recursive) && file.isDirectory()) {
            File[] containedFiles = file.listFiles();
            if (containedFiles == null) {
                throw new IOException(
                        "Directory "
                                + file.toString()
                                + " does not exist or an I/O error occurred");
            } else if (containedFiles.length != 0) {
                throw new IOException("Directory " + file.toString() + " is not empty");
            }
        }

        return delete(file);
    }

    /**
     * Deletes the given file or directory.
     *
     * @param f the file to be deleted
     * @return <code>true</code> if all files were deleted successfully, <code>false</code>
     *     otherwise
     * @throws IOException thrown if an error occurred while deleting the files/directories
     */
    private boolean delete(final File f) throws IOException {

        if (f.isDirectory()) {
            final File[] files = f.listFiles();
            if (files != null) {
                for (File file : files) {
                    final boolean del = delete(file);
                    if (!del) {
                        return false;
                    }
                }
            }
        } else {
            return f.delete();
        }

        // Now directory is empty
        return f.delete();
    }

    @Override
    public URI getUri() {
        return LOCAL_URI;
    }

    @Override
    public boolean mkdir(String path) throws IOException {
        return mkdirsInternal(new File(path));
    }

    private boolean mkdirsInternal(File file) throws IOException {
        if (file.isDirectory()) {
            return true;
        } else if (file.exists() && !file.isDirectory()) {
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
            //            be safe when multiple parallel instances try to create the directory

            // exists and is not a directory -> is a regular file
            throw new IOException("directory already exist " + file.getAbsolutePath());
        } else {
            File parent = file.getParentFile();
            return (parent == null || mkdirsInternal(parent))
                    && (file.mkdir() || file.isDirectory());
        }
    }

    @Override
    public FileStatus[] list(String path) throws IOException {
        File localf = new File(path);
        FileStatus[] results;

        if (!localf.exists()) {
            return null;
        }
        if (localf.isFile()) {
            return new FileStatus[]{new LocalFileStatus(localf, this)};
        }

        final String[] names = localf.list();
        if (names == null) {
            return null;
        }
        results = new FileStatus[names.length];
        for (int i = 0; i < names.length; i++) {
            results[i] = getFileStatus((new File(path, names[i])).getPath());
        }

        return results;
    }

    public FileStatus getFileStatus(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            return new LocalFileStatus(file, this);
        } else {
            throw new IOException("File " + path + " does not exist");
        }
    }

    @Override
    public byte[] read(String path) throws IOException {
        File file = new File(path);
        FileInputStream fis = new FileInputStream(file);
        byte[] b = new byte[(int) file.length()];
        fis.read(b);
        return b;
    }

    @Override
    public boolean exist(String path) {
        File file = new File(path);
        return file.exists();
    }

    @Override
    public String path() {
        return null;
    }
}
