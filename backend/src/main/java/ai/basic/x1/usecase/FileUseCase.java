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
                log.info("deleting fileUrl: {} ", fileUrl);
                minioService.removeObject(fileUrl);
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
                // log.info("deleting file path: {} ", file.getPath());
                minioService.removeObject(file.getPath());
            } catch (Exception e) {
                log.warn("Failed to delete file from MinIO: {}", file.getPath(), e);
            }
        }

        fileDAO.removeByIds(fileIds); // file 테이블 삭제
        log.info("DB에서 file 레코드 {}개 삭제 완료", fileIds.size());
    }

    private void setUrl(FileBO fileBO) {
        try {
            fileBO.setInternalUrl(minioService.getInternalUrl(fileBO.getBucketName(), fileBO.getPath()));
            fileBO.setUrl(minioService.getUrl(fileBO.getBucketName(), fileBO.getPath()));
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
