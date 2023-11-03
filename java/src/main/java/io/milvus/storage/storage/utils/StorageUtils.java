package io.milvus.storage.storage.utils;

import io.milvus.storage.common.constant.Constant;
import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;

public class StorageUtils {
    private final static Logger log = LoggerFactory.getLogger(StorageUtils.class);

    public static String GetManifestFilePath(String path, Long version) {
        return StringUtils.join(new String[]{
                path,
                Constant.ManifestDir,
                version.toString() + Constant.ManifestFileSuffix}, File.separator);
    }

    public static String GetManifestTmpFilePath(String path, Long version) {
        return StringUtils.join(new String[]{
                path,
                Constant.ManifestDir,
                version.toString() + Constant.ManifestTempFileSuffix}, File.separator);
    }

    public static String GetManifestDir(String path) {
        return StringUtils.join(new String[]{
                path,
                Constant.ManifestDir}, File.separator);
    }

    public static Long ParseManifestVersionFromFileName(String path) {
        if (!path.endsWith(Constant.ManifestFileSuffix)) {
            return -1L;
        }
        String versionString = path.substring(0, path.length() - Constant.ManifestFileSuffix.length());
        Long versionLong;
        try {
            versionLong = Long.valueOf(versionString);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            log.warn(String.format("parse version from file name error, path: %s", path));
            return -1L;
        }
        return versionLong;
    }

    public static String GetNewParquetFilePath(String path) {
        UUID scalarFileId = UUID.randomUUID();
        return StringUtils.join(new String[]{path, scalarFileId + Constant.ParquetDataFileSuffix}, File.separator);
    }

    public static String GetScalarDataDir(String path) {
        return new File(path, Constant.ScalarDataDir).getPath();
    }

    public static String GetVectorDataDir(String path) {
        return new File(path, Constant.VectorDataDir).getPath();
    }

}
