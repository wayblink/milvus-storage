package io.milvus.storage.fs;

public enum FsType {
    InMemory(0), LocalFS(1), S3(2);    //    调用构造函数来构造枚举项

    private int value = 0;

    private FsType(int value) {    //    必须是private的，否则编译错误
        this.value = value;
    }

    public static FsType valueOf(int value) {    //    手写的从int到enum的转换函数
        switch (value) {
            case 0:
                return InMemory;
            case 1:
                return LocalFS;
            case 2:
                return S3;
            default:
                return null;
        }
    }

    public int value() {
        return this.value;
    }
}
