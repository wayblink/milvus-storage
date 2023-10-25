package io.milvus.storage.file.fragment;

import lombok.*;
import manifest_proto.ManifestOuterClass;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Fragment {
    private Long id;
    private List<String> files;

    public static List<String> ToFilesVector(List<Fragment> fragments) {
        List<String> files = new ArrayList<>();
        fragments.forEach(f -> files.addAll(f.files));
        return files;
    }

    public void AddFile(String file) {
        this.files.add(file);
    }

    public static Fragment FromProtobuf(ManifestOuterClass.Fragment fragment) {
        return new Fragment(fragment.getId(), fragment.getFilesList());
    }

    public static ManifestOuterClass.Fragment ToProtobuf(Fragment fragment) {
        return ManifestOuterClass.Fragment.newBuilder()
                .setId(fragment.id)
                .addAllFiles(fragment.getFiles())
                .build();
    }
}

