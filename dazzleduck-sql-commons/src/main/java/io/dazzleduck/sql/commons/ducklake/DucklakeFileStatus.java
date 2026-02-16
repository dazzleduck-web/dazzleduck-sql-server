package io.dazzleduck.sql.commons.ducklake;

import io.dazzleduck.sql.commons.FileStatus;

public record DucklakeFileStatus(String fileName, Long size, Long lastModified, Boolean pathIsRelative, Long tableId, Long mappingId){
    public FileStatus toFileStatus(){
        return new FileStatus(fileName, size, lastModified);
    }

    public FileStatus resolvedFileStatus(String basePath){
        if (pathIsRelative != null && pathIsRelative) {
            var resolvedPath = basePath + "/" + fileName;
            return new FileStatus(resolvedPath, size, lastModified);
        }
        return new FileStatus(fileName, size, lastModified);
    }
}