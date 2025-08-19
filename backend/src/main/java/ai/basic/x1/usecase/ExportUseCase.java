package ai.basic.x1.usecase;

import ai.basic.x1.adapter.dto.ApiResult;
import ai.basic.x1.adapter.port.dao.ExportRecordDAO;
import ai.basic.x1.adapter.port.dao.mybatis.model.ExportRecord;
import ai.basic.x1.adapter.port.minio.MinioProp;
import ai.basic.x1.adapter.port.minio.MinioService;
import ai.basic.x1.entity.*;
import ai.basic.x1.entity.enums.DataFormatEnum;
import ai.basic.x1.entity.enums.ExportStatusEnum;
import ai.basic.x1.usecase.exception.UsecaseCode;
import ai.basic.x1.usecase.exception.UsecaseException;
import ai.basic.x1.util.*;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.TemporalAccessorUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.ZipUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;
import com.alibaba.ttl.TtlRunnable;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Assert;
import io.minio.errors.*;
import kotlin.jvm.functions.Function4;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.*;

import java.io.InputStream;
import java.io.OutputStream;
import cn.hutool.http.HttpRequest;

import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpRequest;

/**
 * @author fyb
 */
@Slf4j
public class ExportUseCase {


    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ExportRecordUseCase exportRecordUsecase;

    @Autowired
    private ExportRecordDAO exportRecordDAO;

    @Autowired
    private MinioService minioService;

    @Autowired
    private MinioProp minioProp;

    @Autowired
    private FileUseCase fileUseCase;

    @Autowired
    protected UploadUseCase uploadUseCase;

    @Autowired
    private DataInfoUseCase dataInfoUseCase;

    @Value("${file.tempPath:/tmp/xtreme1/}")
    private String tempPath;

    private static Integer BATCH_SIZE = 100;

    private static final ExecutorService executorService = ThreadUtil.newExecutor(10);

    /**
     * Create export record
     *
     * @return serial number
     */
    public Long createExportRecord(String fileName, Long dataset_id) {
        var serialNumber = IdUtil.getSnowflakeNextId();
        var exportRecord = ExportRecord.builder()
                .serialNumber(serialNumber)
                .fileName(fileName)
                .datasetId(dataset_id)
                .status(ExportStatusEnum.GENERATING).build();
        exportRecordDAO.saveOrUpdate(exportRecord);
        return serialNumber;
    }


    public <Q extends BaseQueryBO> Long asyncExportDataZip(String fileName, Long serialNumber, Map<Long, String> classMap, Map<Long, String> resultMap,
                                                           Q query, Function<Q, List<Long>> fun, Function4<List<Long>, Q, Map<Long, String>, Map<Long, String>, List<DataExportBO>> processData) {
        var lambdaQueryWrapper = new LambdaQueryWrapper<ExportRecord>();
        lambdaQueryWrapper.in(ExportRecord::getSerialNumber, serialNumber);
        var exportRecord = exportRecordDAO.getOne(lambdaQueryWrapper);
        var srcPath = String.format("%s%s", tempPath, FileUtil.getPrefix(fileName));
        FileUtil.mkdir(srcPath);
        getDataAndUpload(exportRecord, srcPath, classMap, resultMap, query, fun, processData);
        return serialNumber;
    }

    private <Q extends BaseQueryBO> void getDataAndUpload(ExportRecord record, String srcPath, Map<Long, String> classMap, Map<Long, String> resultMap, Q query,
                                                          Function<Q, List<Long>> fun, Function4<List<Long>, Q, Map<Long, String>, Map<Long, String>, List<DataExportBO>> processData) {
        var rootPath = String.format("%s/%s", record.getCreatedBy(),
                TemporalAccessorUtil.format(OffsetDateTime.now(), DatePattern.PURE_DATETIME_PATTERN));
        var exportRecordBOBuilder = ExportRecordBO.builder()
                .id(record.getId())
                .updatedBy(record.getCreatedBy())
                .updatedAt(OffsetDateTime.now());

        var dataIds = fun.apply(query);
        log.info("Export 대상 dataIds: {}", dataIds);
        if (CollUtil.isEmpty(dataIds)) {
            exportRecordBOBuilder.status(ExportStatusEnum.FAILED);
            return;
        }
        AtomicInteger i = new AtomicInteger(0);
        var dataIdList = ListUtil.partition(dataIds, 1000);
        log.info("getDataAndUploads src path: {}", srcPath);
        log.info("dataIdList size: {}", dataIdList.size());
        long startTime = System.currentTimeMillis(); // 시작 시간

        dataIdList.forEach(subDataIds -> {
            log.info("subDataIds: {}", subDataIds);
            // writeFile(subDataIds, srcPath, classMap, resultMap, query, processData);
            writeAnnotationFilesWithOriginalZip(subDataIds, srcPath, classMap, resultMap, query, processData);
            var exportRecordBO = exportRecordBOBuilder
                    .generatedNum(i.get() * BATCH_SIZE + subDataIds.size())
                    .totalNum(dataIds.size())
                    .updatedAt(OffsetDateTime.now())
                    .build();
            exportRecordUsecase.saveOrUpdate(exportRecordBO);
            i.getAndIncrement();
        });

        long endTime = System.currentTimeMillis(); // 종료 시간
        double durationSec = (endTime - startTime) / 1000.0; // 초 단위 변환
        log.info("1. 원본 zip 다운로드 및 annoation 데이터 다운로드 소요시간: {} 초", durationSec);
        
        startTime = System.currentTimeMillis(); // 시작 시간
        var tarPath = srcPath + ".tar";
        File tarFile = null;
        var path = String.format("%s/%s", rootPath, FileUtil.getName(tarPath));

        try{
            log.info("Start archiving...");
            tarFile = TarUtil.tar(new File(srcPath), new File(tarPath)); // 압축 없음
            // TarUtil.tarGz(new File(srcPath), new File(tarGzPath)); // gzip 압축
        }
        catch (Exception e){
            logger.error("Archiving file error", e);
        }

        var fileName = FileUtil.getName(tarFile.getPath());
        endTime = System.currentTimeMillis(); // 종료 시간
        durationSec = (endTime - startTime) / 1000.0; // 초 단위 변환
        log.info("2. 데이터 아카이빙 소요시간: {} 초", durationSec);
        
        startTime = System.currentTimeMillis(); // 시작 시간
        var fileBO = FileBO.builder()
                           .name(fileName)
                           .originalName(fileName)
                           .bucketName(minioProp.getBucketName())
                           .size(tarFile.length())
                           .path(path)
                           .type(FileUtil.getMimeType(tarFile.getPath()))
                           .build();

        try{
            minioService.uploadFile(minioProp.getBucketName(),
                                    path, FileUtil.getInputStream(tarFile), 
                                    FileUtil.getMimeType(path), 
                                    tarFile.length());
            endTime = System.currentTimeMillis(); // 종료 시간
            durationSec = (endTime - startTime) / 1000.0; // 초 단위 변환
            log.info("3. minio 데이터 upload 소요시간: {} 초", durationSec);
            startTime = System.currentTimeMillis(); // 시작 시간
            var resFileBOS = fileUseCase.saveBatchFile(record.getCreatedBy(), Collections.singletonList(fileBO));
            var exportRecordBO = exportRecordBOBuilder
                    .fileId(CollectionUtil.getFirst(resFileBOS).getId())
                    .status(ExportStatusEnum.COMPLETED)
                    .updatedAt(OffsetDateTime.now())
                    .build();
            exportRecordUsecase.saveOrUpdate(exportRecordBO);
        } catch (Exception e) {
            var exportRecordBO = exportRecordBOBuilder
                    .status(ExportStatusEnum.FAILED)
                    .updatedAt(OffsetDateTime.now())
                    .build();
            exportRecordUsecase.saveOrUpdate(exportRecordBO);
            logger.error("Upload file error", e);
        } finally {
            // FileUtil.del(zipFile);
            log.info("tarFile, srcpath: {}, {}", tarFile, srcPath);
            FileUtil.del(tarFile);
            FileUtil.del(srcPath);
        }
        endTime = System.currentTimeMillis(); // 종료 시간
        durationSec = (endTime - startTime) / 1000.0; // 초 단위 변환
        log.info("4. DB에 export record upload 소요 시간: {} 초", durationSec);
    }

    private String replacePresignedUrlForInternalAccess(String url) {
        return url
            .replace("http://localhost:8190/minio", "http://minio:9000")
            .replace("http://127.0.0.1:8190/minio", "http://minio:9000")
            .replace("http://host.docker.internal:8190/minio", "http://minio:9000")
            .replace("http://ketilabel.iptime.org:8080/minio", "http://minio:9000");
    }
    
    
    private <Q extends BaseQueryBO> void writeAnnotationFilesWithOriginalZip(List<Long> dataIds, String zipPathOr, Map<Long, String> classMap, 
                                                                                Map<Long, String> resultMap, Q query, 
                                                                                Function4<List<Long>, Q, Map<Long, String>, 
                                                                                Map<Long, String>, List<DataExportBO>> processData) {
        var dataExportBOList = processData.invoke(dataIds, query, classMap, resultMap);
        var jsonConfig = JSONConfig.create().setIgnoreNullValue(false);
        var datasetId = ((DataInfoQueryBO) query).getDatasetId();
        List<UploadRecordBO> recordsBO = uploadUseCase.findByDatasetId(datasetId);
        log.info("writeAnnotationFilesWithOriginalZip, dataset id: {} ", datasetId);
        log.info("dataExportBOList size: {}", dataExportBOList.size());
        dataExportBOList.forEach(dataExportBO -> {
            var sceneName = dataExportBO.getSceneName();
            var zipPath = StrUtil.isNotEmpty(sceneName) ? String.format("%s/%s", zipPathOr, sceneName) : zipPathOr;
            var dataExportBaseBO = dataExportBO.getData();
            // log.info("sceneName, zipPath: {}, {} ", sceneName, zipPath);
            if (ObjectUtil.isNotNull(dataExportBO.getResult())) {
                for (DataResultExportBO resultBO : dataExportBO.getResult()) {
                    var sourceName = resultBO.getSourceName(); // e.g. "ROS", "MODEL", "GROUND_TRUTH"
                    var resultPath = String.format("%s/%s/%s/%s.json",
                            zipPath,
                            Constants.RESULT,
                            sourceName != null ? sourceName : "UNKNOWN",
                            dataExportBaseBO.getName());
                    FileUtil.writeString(JSONUtil.toJsonStr(resultBO, jsonConfig), resultPath, StandardCharsets.UTF_8);
                }
            }
            // else {
            //     // 결과가 없으면 빈 JSON 만들어주기
            //     var resultPath = String.format("%s/%s/%s/%s.json",
            //             zipPath,
            //             Constants.RESULT,
            //             "UNKNOWN",
            //             dataExportBaseBO.getName());
            //     FileUtil.writeString("{}", resultPath, StandardCharsets.UTF_8);
            // }
        });
    }
    private <Q extends BaseQueryBO> void writeFile(List<Long> dataIds, String zipPathOr, Map<Long, String> classMap, Map<Long, String> resultMap, Q query, Function4<List<Long>, Q, Map<Long, String>, Map<Long, String>, List<DataExportBO>> processData) {
        var dataExportBOList = processData.invoke(dataIds, query, classMap, resultMap);
        var jsonConfig = JSONConfig.create().setIgnoreNullValue(false);
        dataExportBOList.forEach(dataExportBO -> {
            var sceneName = dataExportBO.getSceneName();
            var zipPath = StrUtil.isNotEmpty(sceneName) ? String.format("%s/%s", zipPathOr, sceneName) : zipPathOr;
            var dataExportBaseBO = dataExportBO.getData();
            log.info("===========================================================================");
            log.info("dataExportBaseBO.getType: {}", dataExportBaseBO.getType());
            log.info("[EXPORT DEBUG] getData JSON: {}", JSONUtil.toJsonStr(dataExportBaseBO, jsonConfig));
            log.info("===========================================================================");

            // LIDAR_BASIC 또는 LIDAR_FUSION 타입일 때 .pcd 파일 다운로드
            if ("LIDAR_FUSION".equals(dataExportBaseBO.getType())) {
                var lidarFusionData = DefaultConverter.convert(dataExportBaseBO, LidarFusionDataExportBO.class);
                var lidarPointClouds = lidarFusionData.getLidarPointClouds();
                var cameraImages = lidarFusionData.getCameraImages();
                var cameraConfig = lidarFusionData.getCameraConfig();
                // lidar pcd file
                if (CollUtil.isNotEmpty(lidarPointClouds)) {
                    for (ExportDataLidarPointCloudFileBO pcdFile : lidarPointClouds) {
                        try (InputStream inputStream = HttpRequest.get(
                                replacePresignedUrlForInternalAccess(pcdFile.getUrl())
                                ).execute().bodyStream()){ 
                            String fullPcdPath = String.format("%s/%s/%s/%s", zipPath, Constants.DATA, Constants.LIDAR_POINT_CLOUD, pcdFile.getFilename());
                            log.info("fullPcdPath: {}", fullPcdPath);
                            File file = new File(fullPcdPath);
                            FileUtil.mkdir(file.getParentFile());
                            FileUtil.writeFromStream(inputStream, file);
                        } catch (Exception e) {
                            log.error("Failed to download PCD file: {}", pcdFile.getUrl(), e);
                        }
                    }
                }
                // camera jpeg file
                if (CollUtil.isNotEmpty(cameraImages)) {
                    for (ExportDataImageFileBO imageFile : cameraImages) {
                        try (InputStream inputStream = HttpRequest.get(
                                replacePresignedUrlForInternalAccess(imageFile.getUrl())
                                ).execute().bodyStream()){ 
                            Pattern pattern = Pattern.compile("camera_image_\\d+");
                            Matcher matcher = pattern.matcher(imageFile.getZipPath());
                            String cameraDir;
                            if (matcher.find()) {
                                cameraDir = matcher.group(); // 예: "camera_image_1"
                            } else {
                                cameraDir = Constants.CAMERA_IMAGE;
                            }
                            log.info("cam dir: {} ", cameraDir);
                            String fullImagePath = String.format("%s/%s/%s/%s", zipPath, Constants.DATA, cameraDir, imageFile.getFilename());
                            log.info("full ImagePath: {}", fullImagePath);
                            File file = new File(fullImagePath);
                            FileUtil.mkdir(file.getParentFile());
                            FileUtil.writeFromStream(inputStream, file);
                        } catch (Exception e) {
                            log.error("Failed to download image file: {}", imageFile.getUrl(), e);
                        }
                    }
                }

                // camera calibration file
                if (cameraConfig != null) {
                    // for (ExportDataImageFileBO cameraConfigFile : cameraConfigs) {
                    try (InputStream inputStream = HttpRequest.get(
                            replacePresignedUrlForInternalAccess(cameraConfig.getUrl())
                            ).execute().bodyStream()){
                        String cameraConfigFilePath = String.format("%s/%s/%s/%s", zipPath, Constants.DATA, Constants.CAMERA_CONFIG, cameraConfig.getFilename());
                        log.info("CameraConfigFilePath: {}", cameraConfigFilePath);
                        File file = new File(cameraConfigFilePath);
                        FileUtil.mkdir(file.getParentFile());
                        FileUtil.writeFromStream(inputStream, file);
                    } catch (Exception e) {
                        log.error("Failed to download image file: {}", e);
                    }
                    // }
                }

            }

            if (ObjectUtil.isNotNull(dataExportBO.getResult())) {
                for (DataResultExportBO resultBO : dataExportBO.getResult()) {
                    var sourceName = resultBO.getSourceName(); // e.g. "ROS", "MODEL", "GROUND_TRUTH"
                    var resultPath = String.format("%s/%s/%s/%s.json",
                            zipPath,
                            Constants.RESULT,
                            sourceName != null ? sourceName : "UNKNOWN",
                            dataExportBaseBO.getName());
                    FileUtil.writeString(JSONUtil.toJsonStr(resultBO, jsonConfig), resultPath, StandardCharsets.UTF_8);
                }
            }
        });
    }

    /**
     * Download original file
     */
    private void downLoadRawFile(String destDir, List<? extends ExportDataFileBaseBO> list, String dataName) {
        if (CollUtil.isEmpty(list)) {
            return;
        }
        CountDownLatch countDownLatch = new CountDownLatch(list.size());
        list.forEach(l -> executorService.submit(Objects.requireNonNull(TtlRunnable.get(() -> {
            try {
                var deviceName = l.getDeviceName();
                var internalUrl = l.getInternalUrl();
                var dest = String.format("%s/%s/%s.%s",
                        destDir, deviceName, dataName, FileUtil.getSuffix(l.getFilename()));
                HttpUtil.downloadFile(internalUrl, dest);
            } catch (Throwable throwable) {
                logger.error("downLoad raw file error", throwable);
            } finally {
                countDownLatch.countDown();
            }
        }))));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * get export record by serial numbers
     *
     * @param serialNumbers serial numbers
     * @return export records
     */
    public List<ExportRecordBO> findExportRecordBySerialNumbers(List<String> serialNumbers) {
        Assert.notEmpty(serialNumbers, "serial number cannot be null");
        return exportRecordUsecase.findBySerialNumbers(serialNumbers);
    }

}
