package ai.basic.x1.usecase;

import ai.basic.x1.adapter.port.dao.FileDAO;
import ai.basic.x1.adapter.port.dao.mybatis.model.File;
import ai.basic.x1.adapter.port.minio.MinioService;
import ai.basic.x1.entity.FileBO;
import ai.basic.x1.entity.RelationFileBO;
import ai.basic.x1.usecase.exception.UsecaseException;
import ai.basic.x1.util.DefaultConverter;
import ai.basic.x1.util.Constants;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ByteUtil;
import cn.hutool.crypto.SecureUtil;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.dao.DuplicateKeyException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.ArrayList;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;

/**
 * @author : fyb
 */
@Slf4j
public class FileUseCase {

    @Autowired
    private FileDAO fileDAO;

    @Autowired
    private MinioService minioService;

    private static final Set<Long> GTDataHashSet = ConcurrentHashMap.newKeySet();

    /**
     * fileId
     *
     * @param id id
     * @return RelationFileBO
     */
    public RelationFileBO findById(Long id) {
        var file = fileDAO.getById(id);
        var lambdaQueryWrapper = Wrappers.lambdaQuery(File.class);
        lambdaQueryWrapper.eq(File::getRelationId, id);
        var relationFiles = fileDAO.list(lambdaQueryWrapper);
        var fileBO = DefaultConverter.convert(file, RelationFileBO.class);
        if (CollectionUtil.isNotEmpty(relationFiles)) {
            var relationFileBOs = DefaultConverter.convert(relationFiles, FileBO.class);
            relationFileBOs.forEach(this::setUrl);
            fileBO.setRelationFiles(relationFileBOs);
        }
        setUrl(fileBO);
        return fileBO;
    }

    
    public List<FileBO> findRelatedFilesByRelationIds(List<Long> relationIds) {
        var files = fileDAO.lambdaQuery().in(File::getRelationId, relationIds).list();
        return DefaultConverter.convert(files, FileBO.class);
    }


    /**
     * file object list
     *
     * @param ids file object ids
     * @return file object list
     */
    public List<RelationFileBO> findByIds(List<Long> ids) {
        var files = fileDAO.listByIds(ids);
        var fileBOs = DefaultConverter.convert(files, RelationFileBO.class);
        var lambdaQueryWrapper = Wrappers.lambdaQuery(File.class);
        lambdaQueryWrapper.in(File::getRelationId, ids);
        var relationFiles = fileDAO.list(lambdaQueryWrapper);
        Objects.requireNonNull(fileBOs).forEach(fileBO -> {
            setUrl(fileBO);
            if (CollectionUtil.isNotEmpty(relationFiles)) {
                var relationFileBOs = DefaultConverter.convert(relationFiles.stream().
                        filter(relationFile -> relationFile.getRelationId().equals(fileBO.getId())).collect(Collectors.toList()), FileBO.class);
                Objects.requireNonNull(relationFileBOs).forEach(this::setUrl);
                fileBO.setRelationFiles(relationFileBOs);
            }
        });
        return fileBOs;
    }

    public void deleteByUrls(List<String> fileUrls) {
        if (CollUtil.isEmpty(fileUrls)) {
            return;
        }
        for (var fileUrl : fileUrls) {
            try {
                // Extract object name from URL (remove endpoint part)
                String objectName = fileUrl;
                if (fileUrl.contains("://")) {
                    // URL format: http://endpoint/bucket/objectName
                    String[] urlParts = fileUrl.split("/", 4);
                    if (urlParts.length >= 4) {
                        objectName = urlParts[3];  // Get objectName part
                    }
                }
                
                log.info("deleting fileUrl objectName: {} ", objectName);
                minioService.removeObject(objectName);
                log.info("Deleted file from MinIO: {}", objectName);
            } catch (Exception e) {
                log.warn("Failed to delete fileUrl from MinIO: {}", fileUrl, e);
            }
        }
    }

    public void deleteByIds(List<Long> fileIds) {
        log.info("fileIds: {}", fileIds);
        if (CollUtil.isEmpty(fileIds)) {
            return;
        }

        var files = fileDAO.listByIds(fileIds); // 실제 파일 정보 조회

        for (var file : files) {
            try {
                // Try original path first
                minioService.removeObject(file.getPath());
                log.info("Deleted file from MinIO: {}", file.getPath());
            } catch (Exception e1) {
                // If original path fails, try translating to new format
                try {
                    String translatedPath = translateOldPathToNew(file.getPath());
                    if (!translatedPath.equals(file.getPath())) {
                        minioService.removeObject(translatedPath);
                        log.info("Deleted file from MinIO using translated path: {}", translatedPath);
                    } else {
                        log.warn("Failed to delete file from MinIO (no translation available): {}", file.getPath(), e1);
                    }
                } catch (Exception e2) {
                    log.warn("Failed to delete file from MinIO (both paths tried): {}", file.getPath(), e2);
                }
            }
        }

        fileDAO.removeByIds(fileIds); // file 테이블 삭제
        log.info("DB에서 file 레코드 {}개 삭제 완료", fileIds.size());
    }
    
    /**
     * Delete entire dataset folder from MinIO
     * This removes all files and empty folders for a dataset
     * 
     * @param datasetName Name of the dataset
     */
    public void deleteDatasetFolder(String datasetName) {
        if (StrUtil.isEmpty(datasetName)) {
            return;
        }
        
        try {
            // Determine the correct prefix based on dataset name
            var datasetPrefix = datasetName.endsWith("_raw") ? datasetName + "/" : datasetName + "/";
            log.info("Deleting dataset folder from MinIO with prefix: {}", datasetPrefix);
            minioService.removeObjectsByPrefix(datasetPrefix);
            log.info("Successfully deleted dataset folder: {}", datasetName);
        } catch (Exception e) {
            log.warn("Failed to delete dataset folder from MinIO: {}", datasetName, e);
        }
    }
    
    /**
     * Translate old path format to new format
     * Old: userId/datasetId/UUID/Scene_01/file.jpg
     * New DB format: datasetName/raw/UUID/Scene_01/file.jpg
     * MinIO format: datasetName/raw/Scene_01/file.jpg
     */
    public String translateOldPathToNew(String dbPath) {
        // log.info("Translating path: {}", dbPath);
        
        // Check if already in MinIO format (no UUID between /raw/ and next folder)
        if (dbPath.contains("/raw/")) {
            // New format: datasetName/raw/UUID/Scene_01/file.jpg
            // Need to remove UUID (3rd segment after /raw/)
            String[] parts = dbPath.split("/");
            
            // Find /raw/ position
            int rawIndex = -1;
            for (int i = 0; i < parts.length; i++) {
                if ("raw".equals(parts[i])) {
                    rawIndex = i;
                    break;
                }
            }
            
            if (rawIndex >= 0 && rawIndex + 1 < parts.length) {
                // Check if next part after /raw/ is UUID (32 chars, hex)
                String potentialUUID = parts[rawIndex + 1];
                if (potentialUUID.length() == 32 && potentialUUID.matches("[a-f0-9]+")) {
                    // This is UUID, remove it
                    StringBuilder result = new StringBuilder();
                    for (int i = 0; i < parts.length; i++) {
                        if (i == rawIndex + 1) {
                            continue;  // Skip UUID
                        }
                        if (i > 0 && !parts[i].isEmpty()) {
                            result.append("/");
                        }
                        if (!parts[i].isEmpty()) {
                            result.append(parts[i]);
                        }
                    }
                    String translated = result.toString();
                    // log.info("Translated path (removed UUID): {}", translated);
                    return translated;
                }
            }
            
            // Already in MinIO format (no UUID)
            // log.info("Path already in MinIO format: {}", dbPath);
            return dbPath;
        }
        
        if (dbPath.contains("/export_packages/")) {
            // Export packages don't have UUID
            log.info("Export packages path, no translation needed: {}", dbPath);
            return dbPath;
        }
        
        // Old format: userId/datasetId/UUID/...rest
        // Extract the part after UUID (skip first 3 parts)
        String[] parts = dbPath.split("/", 4);
        if (parts.length < 4) {
            log.warn("Cannot translate path (not enough parts): {}", dbPath);
            return dbPath;  // Can't translate, return as-is
        }
        
        // This is old format, we can't easily translate without dataset info
        log.warn("Old format path, cannot translate without dataset context: {}", dbPath);
        return dbPath;  // Return as-is
    }

    private void setUrl(FileBO fileBO) {
        try {
            // Translate DB path (with UUID) to MinIO path (without UUID)
            String minioPath = translateOldPathToNew(fileBO.getPath());
            
            fileBO.setInternalUrl(minioService.getInternalUrl(fileBO.getBucketName(), minioPath));
            fileBO.setUrl(minioService.getUrl(fileBO.getBucketName(), minioPath));
        } catch (Exception e) {
            log.error("Get url error", e);
            throw new UsecaseException("Get url error");
        }
    }

    /**
     * batch save file
     *
     * @param fileBOS fileBOs
     * @return fileList
     */
    @Transactional(rollbackFor = Throwable.class)
    public List<FileBO> saveBatchFile(Long userId, List<FileBO> fileBOS) {
        var files = DefaultConverter.convert(fileBOS, File.class);
        List<File> uniqueFiles = new ArrayList<>();
        OffsetDateTime now = OffsetDateTime.now();
        log.info("====================================");
        for (File file : Objects.requireNonNull(files)) {
            String filename = file.getName();
            boolean isRosPerceptionJson = filename.contains(Constants.ROS_PERCEPTION_DATA) &&
                           filename.endsWith(Constants.JSON_SUFFIX.toLowerCase());            
            // log.info("file name, isRosPerceptionJson: {}, {}", filename, isRosPerceptionJson);
            long pathHash = ByteUtil.bytesToLong(SecureUtil.md5().digest(file.getPath()));
            if (isRosPerceptionJson && !GTDataHashSet.add(pathHash)){
                continue;
            }
            file.setPathHash(pathHash);
            file.setCreatedBy(userId);
            file.setCreatedAt(now);
            file.setUpdatedBy(userId);
            file.setUpdatedAt(now);
            uniqueFiles.add(file);
        }
        fileDAO.saveBatch(uniqueFiles);
        log.info("====================================");
        log.info("fileDAO.saveBatch completed");
        var reFileBOs = DefaultConverter.convert(uniqueFiles, FileBO.class);
        reFileBOs.forEach(fileBO -> setUrl(fileBO));
        return reFileBOs;
    }
}
