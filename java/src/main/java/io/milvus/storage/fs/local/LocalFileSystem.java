package io.milvus.storage.fs.local;

import io.milvus.storage.common.utils.OperatingSystem;
import io.milvus.storage.fs.FileStatus;
import io.milvus.storage.fs.File;
import io.milvus.storage.fs.FileSystem;

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
    public File open(String path) throws IOException {
        LocalFile file = new LocalFile(path);
        String parent = file.getFile().getParent();
        this.mkdir(parent);
        return file;
    }

    public FileOutputStream open(String path, boolean overwrite) throws IOException {
        LocalFile file = new LocalFile(path);
        if (exist(path) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + path);
        }
        String parent = file.getFile().getParent();
        if (parent != null && !mkdir(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }
        return new FileOutputStream(file.getFile());
    }

    @Override
    public boolean rename(String src, String dst) throws IOException {
        LocalFile srcFile = new LocalFile(src);
        LocalFile dstFile = new LocalFile(dst);

        LocalFile dstParent = new LocalFile(dstFile.getFile().getParentFile());

        if (dstParent.getFile() != null) {
            dstParent.getFile().mkdirs();
        }

        try {
            Files.move(srcFile.getFile().toPath(), dstFile.getFile().toPath(), StandardCopyOption.REPLACE_EXISTING);
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

        LocalFile file = new LocalFile(path);
        if (file.getFile().isFile()) {
            return file.getFile().delete();
        } else if ((!recursive) && file.getFile().isDirectory()) {
            java.io.File[] containedFiles = file.getFile().listFiles();
            if (containedFiles == null) {
                throw new IOException(
                        "Directory " + file.toString() + " does not exist or an I/O error occurred");
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
    private boolean delete(LocalFile f) throws IOException {

        if (f.getFile().isDirectory()) {
            java.io.File[] files = f.getFile().listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    final boolean del = delete(new LocalFile(file.getPath()));
                    if (!del) {
                        return false;
                    }
                }
            }
        } else {
            return f.getFile().delete();
        }

        // Now directory is empty
        return f.getFile().delete();
    }

    @Override
    public URI getUri() {
        return LOCAL_URI;
    }

    @Override
    public boolean mkdir(String path) throws IOException {
        return new LocalFile(path).getFile().mkdirs();
//        return mkdirsInternal(new LocalFile(path));
    }

    private boolean mkdirsInternal(LocalFile file) throws IOException {
        if (file.getFile().isDirectory()) {
            return true;
        } else if (file.getFile().exists() && !file.getFile().isDirectory()) {
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
            //            be safe when multiple parallel instances try to create the directory

            // exists and is not a directory -> is a regular file
            throw new IOException("directory already exist " + file.getFile().getAbsolutePath());
        } else {
            LocalFile parent = new LocalFile(file.getFile().getParentFile());
            return (parent == null || mkdirsInternal(parent))
                    && (file.getFile().mkdir() || file.getFile().isDirectory());
        }
    }

    @Override
    public FileStatus[] list(String path) throws IOException {
        LocalFile localf = new LocalFile(path);
        FileStatus[] results;

        if (!localf.getFile().exists()) {
            return null;
        }
        if (localf.getFile().isFile()) {
            return new FileStatus[]{new LocalFileStatus(localf.getFile(), this)};
        }

        final String[] names = localf.getFile().list();
        if (names == null) {
            return null;
        }
        results = new FileStatus[names.length];
        for (int i = 0; i < names.length; i++) {
            results[i] = getFileStatus((new java.io.File(path, names[i])).getPath());
        }

        return results;
    }

    public FileStatus getFileStatus(String path) throws IOException {
        LocalFile file = new LocalFile(path);
        if (file.getFile().exists()) {
            return new LocalFileStatus(file.getFile(), this);
        } else {
            throw new IOException("File " + path + " does not exist");
        }
    }

    @Override
    public byte[] read(String path) throws IOException {
        LocalFile file = new LocalFile(path);
        FileInputStream fis = new FileInputStream(file.getFile());
        byte[] b = new byte[(int) file.getFile().length()];
        fis.read(b);
        return b;
    }

    @Override
    public boolean exist(String path) {
        LocalFile file = new LocalFile(path);
        return file.getFile().exists();
    }

    @Override
    public String path() {
        return null;
    }
}
