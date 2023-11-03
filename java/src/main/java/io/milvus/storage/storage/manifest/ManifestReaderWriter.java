package io.milvus.storage.storage.manifest;

import io.milvus.storage.common.constant.Constant;
import io.milvus.storage.fs.File;
import io.milvus.storage.fs.FileStatus;
import io.milvus.storage.fs.FileSystem;
import io.milvus.storage.storage.utils.StorageUtils;
import lombok.AllArgsConstructor;
import manifest_proto.ManifestOuterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@AllArgsConstructor
public class ManifestReaderWriter {
    private final static Logger log = LoggerFactory.getLogger(ManifestReaderWriter.class);
    private FileSystem fs;
    private String rootPath;

    public void Write(Manifest m) throws IOException {
        String tmpManifestFilePath = StorageUtils.GetManifestTmpFilePath(this.rootPath, m.getVersion());
        String manifestFilePath = StorageUtils.GetManifestFilePath(this.rootPath, m.getVersion());

        log.debug("write to tmpManifestFilePath", tmpManifestFilePath);
//        Path tempPath = new Path(tmpManifestFilePath);
//        FSDataOutputStream fos = this.fs.create(tempPath);
//        fos.write(m.ToProtobuf().toByteArray());
//        fos.close();
//        fs.rename(tempPath, new Path(manifestFilePath));

        File file = this.fs.open(tmpManifestFilePath);
        file.write(m.ToProtobuf().toByteArray());
        file.close();
        fs.rename(tmpManifestFilePath, manifestFilePath);

        log.debug("save manifest file success", manifestFilePath);
        return;
    }

    public Manifest Read(Long version) throws Exception {

        FileStatus[] manifests = this.findAllManifest();

        if (manifests == null || manifests.length == 0) {
            return null;
        }
        String maxVersionManifest = null;
        Long maxVersion = -1L;
        for (FileStatus manifest: manifests) {
            Long ver = StorageUtils.ParseManifestVersionFromFileName(manifest.getName());
            if (ver == -1) {
                continue;
            }

            if (version != Constant.LatestManifestVersion) {
                if (ver == version) {
                    return ParseFromFile(this.fs, manifest.getPath());
                }
            } else if (ver > maxVersion) {
                maxVersion = ver;
                maxVersionManifest = manifest.getPath();
            }
        }

        if (maxVersion != -1) {
            return ParseFromFile(this.fs, maxVersionManifest);
        }
        return null;
    }

    public Long MaxVersion() {
        return -1L;
    }

    private Manifest ParseFromFile(FileSystem fs, String path) throws Exception {
//        File file = fs.open(path);
//        FileStatus fileStatus = fs.getFileStatus(path);
//        byte[] bytes = new byte[(int) fileStatus.getLen()];
        byte[] bytes = fs.read(path);
        ManifestOuterClass.Manifest pManifest = ManifestOuterClass.Manifest.parseFrom(bytes);
        return Manifest.FromProtobuf(pManifest);
    }

    private FileStatus[] findAllManifest() throws IOException {
        String manifestDir = StorageUtils.GetManifestDir(this.rootPath);
        if (this.fs.exist(manifestDir)) {
            FileStatus[] files = this.fs.list(manifestDir);
            return files;
        } else {
            return null;
        }
    }

}
