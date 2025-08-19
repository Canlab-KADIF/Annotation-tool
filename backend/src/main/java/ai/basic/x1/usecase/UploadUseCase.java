package ai.basic.x1.usecase;

import ai.basic.x1.adapter.port.dao.UploadRecordDAO;
import ai.basic.x1.adapter.port.dao.mybatis.model.UploadRecord;
import ai.basic.x1.entity.UploadRecordBO;
import ai.basic.x1.entity.enums.UploadStatusEnum;
import ai.basic.x1.util.DecompressionFileUtils;
import ai.basic.x1.util.DefaultConverter;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import ai.basic.x1.adapter.port.minio.MinioProp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fyb
 */
public class UploadUseCase {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private UploadRecordDAO uploadRecordDAO;
    @Autowired
    private MinioProp minioProp;

    public UploadRecordBO createUploadRecord(String fileUrl, Long datasetId) {
        var serialNumber = IdUtil.getSnowflakeNextId();
    
        // Step 1: 파라미터 제거된 순수 URL 추출
        String cleanUrl = DecompressionFileUtils.removeUrlParameter(fileUrl);
        log.info("cleanUrl: {}", cleanUrl);
    
        // Step 2: MinIO prefix
        String minioPrefix = minioProp.getEndpoint() + minioProp.getBucketName() + "/";
        // String minioPrefix = String.format("%s/%s/", StringUtils.stripEnd(minioProp.getEndpoint(), "/"), minioProp.getBucketName());

        log.info("minioPrefix: {}", minioPrefix);
    
        // Step 3: objectName 추출
        String objectName;
        if (cleanUrl.startsWith(minioPrefix)) {
            objectName = cleanUrl.substring(minioPrefix.length());
        } else {
            throw new RuntimeException("Invalid fileUrl: cannot extract object name.");
        }
    
        // Step 4: 파일 이름 추출
        String fileName = FileUtil.getName(objectName);
        log.info("objectName: {}", objectName);
        log.info("fileName: {}", fileName);
    
        var uploadRecord = UploadRecord.builder()
                .serialNumber(serialNumber)
                .fileUrl(objectName) // presigned 전체 URL이 아닌 objectName만 저장
                .fileName(fileName)
                .datasetId(datasetId)
                .status(UploadStatusEnum.UNSTARTED)
                .build();
    
        uploadRecordDAO.save(uploadRecord);
        return DefaultConverter.convert(uploadRecord, UploadRecordBO.class);
    }


    // public UploadRecordBO findByDatasetId(Long datasetId) {
    //     var records = uploadRecordDAO.lambdaQuery()
    //         .eq(UploadRecord::getDatasetId, datasetId)
    //         .one();
    //     return record != null ? DefaultConverter.convert(record, UploadRecordBO.class) : null;
    // }
    public List<UploadRecordBO> findByDatasetId(Long datasetId) {
        var lambdaQueryWrapper = new LambdaQueryWrapper<UploadRecord>();
        lambdaQueryWrapper.eq(UploadRecord::getDatasetId, datasetId);
        var uploadRecordList = uploadRecordDAO.list(lambdaQueryWrapper);
        return DefaultConverter.convert(uploadRecordList, UploadRecordBO.class);
    }
    /**
     * Modify upload record status and error information based on ID
     *
     * @param id           upload record ID
     * @param uploadStatus upload status
     * @param errorMessage error information
     */
    public void updateUploadRecordStatus(Long id, UploadStatusEnum uploadStatus, String errorMessage) {
        var uploadRecord = UploadRecord.builder()
                .id(id)
                .status(uploadStatus)
                .errorMessage(errorMessage).build();
        uploadRecordDAO.updateById(uploadRecord);
    }

    /**
     * Query import records according to serial number
     *
     * @param serialNumbers Serial number
     * @return Import record collection
     */
    public List<UploadRecordBO> findBySerialNumbers(List<String> serialNumbers) {
        var lambdaQueryWrapper = new LambdaQueryWrapper<UploadRecord>();
        lambdaQueryWrapper.in(UploadRecord::getSerialNumber, serialNumbers);
        var uploadRecordList = uploadRecordDAO.list(lambdaQueryWrapper);
        return DefaultConverter.convert(uploadRecordList, UploadRecordBO.class);
    }
}
