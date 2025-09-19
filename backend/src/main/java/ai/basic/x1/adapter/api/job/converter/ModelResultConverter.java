package ai.basic.x1.adapter.api.job.converter;


import ai.basic.x1.adapter.dto.ApiResult;
import ai.basic.x1.adapter.port.dao.mybatis.model.ModelClass;
import ai.basic.x1.adapter.port.rpc.dto.PointCloudDetectionObject;
import ai.basic.x1.adapter.port.rpc.dto.PointCloudDetectionExtendedObject;
import ai.basic.x1.adapter.port.rpc.dto.PointCloudDetectionRespDTO;
import ai.basic.x1.adapter.port.rpc.dto.PointCloudDetectionExtendedRespDTO;
import ai.basic.x1.entity.ObjectBO;
import ai.basic.x1.entity.PointBO;
import ai.basic.x1.entity.PointCloudDetectionObjectBO;
import ai.basic.x1.entity.PointCloudDetectionParamBO;
import ai.basic.x1.usecase.exception.UsecaseCode;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author andy
 */
public class ModelResultConverter {
    private static final Logger log = LoggerFactory.getLogger(ModelResultConverter.class);

    public static PointCloudDetectionObjectBO preModelResultConverter(ApiResult<List<PointCloudDetectionRespDTO>> preModelRespDTOApiResult,
                                                                      PointCloudDetectionParamBO preModelParamBO, Map<String, ModelClass> modelClassMap) {
        PointCloudDetectionObjectBO.PointCloudDetectionObjectBOBuilder<?, ?> builder = PointCloudDetectionObjectBO.builder();
        if (preModelRespDTOApiResult.getCode() == UsecaseCode.OK) {
            builder.dataId(preModelRespDTOApiResult.getData().get(0).getId());
            for (PointCloudDetectionRespDTO preModelRespDTO : preModelRespDTOApiResult.getData()) {
                builder.message(preModelRespDTO.getMessage())
                        .code(preModelRespDTO.getCode());
                if (UsecaseCode.OK.getCode().equals(preModelRespDTO.getCode())) {
                    var objects = toObjectBOs(preModelRespDTO.getObjects(), preModelParamBO, modelClassMap);
                    builder.objects(objects);
                    builder.confidence(preModelRespDTO.getConfidence());
                    if(CollUtil.isNotEmpty(objects) && ObjectUtil.isNull(preModelRespDTO.getConfidence())){
                        var dataConfidence =objects.stream().mapToDouble(object->object.getConfidence().doubleValue()).summaryStatistics();
                        builder.confidence(BigDecimal.valueOf(dataConfidence.getAverage()));
                    }
                } else {
                    builder.code(preModelRespDTO.getCode()).message(preModelRespDTO.getMessage());
                }
            }
        } else {
            builder.code(UsecaseCode.ERROR.getCode()).message(preModelRespDTOApiResult.getMessage());
        }

        return builder.build();
    }
    public static List<PointCloudDetectionObjectBO> preModelBatchResultConverter(ApiResult<List<PointCloudDetectionExtendedRespDTO>> preModelRespDTOApiResult,
                                                                                PointCloudDetectionParamBO preModelParamBO,
                                                                                Map<String, ModelClass> modelClassMap) {
        
        // log.info("preModelBatchResultConverter input preModelRespDTOApiResult: {}", preModelRespDTOApiResult);
        List<PointCloudDetectionObjectBO> resultList = new ArrayList<>();
        if (preModelRespDTOApiResult.getCode() != UsecaseCode.OK || CollUtil.isEmpty(preModelRespDTOApiResult.getData())) {
            log.info("preModelBatchResultConverter no data or error code");
            return resultList;
        }

        for (PointCloudDetectionExtendedRespDTO extendedRespDTO : preModelRespDTOApiResult.getData()) {
            PointCloudDetectionObjectBO.PointCloudDetectionObjectBOBuilder<?, ?> builder = PointCloudDetectionObjectBO.builder();
            builder.dataId(extendedRespDTO.getId())
                .message(extendedRespDTO.getMessage())
                .code(extendedRespDTO.getCode());

            if (UsecaseCode.OK.getCode().equals(extendedRespDTO.getCode()) && CollUtil.isNotEmpty(extendedRespDTO.getObjects())) {
                // log.info("extendedRespDTO.getObjects(): {}", extendedRespDTO.getObjects());
                var objects = toExtendedObjectBOs(extendedRespDTO.getObjects(), preModelParamBO, modelClassMap);
                // log.info("objects after toExtendedObjectBOs: {}", objects);
                builder.objects(objects)
                    .confidence(extendedRespDTO.getConfidence());

                if (CollUtil.isNotEmpty(objects) && ObjectUtil.isNull(extendedRespDTO.getConfidence())) {
                    var dataConfidence = objects.stream().mapToDouble(object -> object.getConfidence().doubleValue()).summaryStatistics();
                    builder.confidence(BigDecimal.valueOf(dataConfidence.getAverage()));
                }
            }

            resultList.add(builder.build());
        }
        // log.info("preModelBatchResultConverter resultList: {}", resultList);
        return resultList;
    }

    public static List<ObjectBO> toExtendedObjectBOs(
            List<PointCloudDetectionExtendedObject> extendedObjects,
            PointCloudDetectionParamBO preModelParamBO,
            Map<String, ModelClass> modelClassMap) {

        List<ObjectBO> list = new ArrayList<>(CollUtil.isNotEmpty(extendedObjects) ? extendedObjects.size() : 0);
        for (PointCloudDetectionExtendedObject extendedObject : extendedObjects) {
            ObjectBO objectBO = buildExtendedObjectBO(extendedObject, preModelParamBO, modelClassMap);
            if (ObjectUtil.isNotNull(objectBO)) {
                list.add(objectBO);
            }
        }
        return list;
    }

    private static ObjectBO buildExtendedObjectBO(
            PointCloudDetectionExtendedObject extendedObject,
            PointCloudDetectionParamBO preModelParamBO,
            Map<String, ModelClass> modelClassMap) {

        if (extendedObject.getContour() == null) {
            return null;
        }

        // GT_ 접두사 제거
        String rawModelClass = extendedObject.getModelClass();
        String modelClassKey = null;

        if (rawModelClass != null) {
            switch (rawModelClass) {
                case "GT_PED":
                    modelClassKey = "PEDESTRIAN";
                    break;
                default:
                    modelClassKey = rawModelClass.replaceFirst("^GT_", "");
                    break;
            }
        }

        String modelClassName = null;
        if (StrUtil.isNotEmpty(modelClassKey)) {
            ModelClass mc = modelClassMap.get(modelClassKey.trim());
            if (mc != null) {
                modelClassName = mc.getName();
                // log.info("raw model class: {}, Mapping model class key: {}, found ModelClass: {}", rawModelClass, modelClassKey, modelClassName);
            }
        }

        ObjectBO objectBO = ObjectBO.builder()
                .confidence(extendedObject.getModelConfidence())
                .type("3D_BOX")
                .id(extendedObject.getId())
                .trackId(extendedObject.getTrackId())
                .trackName(extendedObject.getTrackName())
                .pointN(extendedObject.getContour().getPointN())
                .modelClass(modelClassName)
                .center3D(buildCenter3D(extendedObject))
                .rotation3D(buildRotation3D(extendedObject))
                .size3D(buildSize3D(extendedObject))
                .classId(extendedObject.getClassId())
                .build();

        if (ObjectUtil.isNull(preModelParamBO) || matchExtendedResult(extendedObject, preModelParamBO)) {
            return objectBO;
        }

        return null;
    }

    private static boolean matchExtendedResult(PointCloudDetectionExtendedObject extendedObject, PointCloudDetectionParamBO preModelParamBO) {
        var selectedClasses = new HashSet<>(preModelParamBO.getClasses());
        var selectedUpperClasses = selectedClasses.stream().map(String::toUpperCase).collect(Collectors.toList());

        // boolean matchClassResult = ((CollUtil.isNotEmpty(selectedUpperClasses)
        //         && selectedUpperClasses.contains(extendedObject.getModelClass().toUpperCase()))
        //         || CollUtil.isEmpty(selectedUpperClasses));

        boolean matchMinConfidence = ((ObjectUtil.isNotNull(preModelParamBO.getMinConfidence())
                && preModelParamBO.getMinConfidence().compareTo(extendedObject.getModelConfidence()) <= 0)
                || ObjectUtil.isNull(preModelParamBO.getMinConfidence()));

        boolean matchMaxConfidence = ((ObjectUtil.isNotNull(preModelParamBO.getMaxConfidence())
                && preModelParamBO.getMaxConfidence().compareTo(extendedObject.getModelConfidence()) >= 0)
                || ObjectUtil.isNull(preModelParamBO.getMaxConfidence()));

        // return matchClassResult && matchMinConfidence && matchMaxConfidence;
        return matchMinConfidence && matchMaxConfidence;
    }

    private static PointBO buildCenter3D(PointCloudDetectionExtendedObject extendedObject) {
        var center = extendedObject.getContour().getCenter3D();
        return PointBO.builder()
                .x(center.getX())
                .y(center.getY())
                .z(center.getZ())
                .build();
    }

    private static PointBO buildRotation3D(PointCloudDetectionExtendedObject extendedObject) {
        var rotation = extendedObject.getContour().getRotation3D();
        return PointBO.builder()
                .x(rotation.getX())
                .y(rotation.getY())
                .z(rotation.getZ())
                .build();
    }

    private static PointBO buildSize3D(PointCloudDetectionExtendedObject extendedObject) {
        var size = extendedObject.getContour().getSize3D();
        return PointBO.builder()
                .x(size.getX())
                .y(size.getY())
                .z(size.getZ())
                .build();
    }
    public static List<ObjectBO> toObjectBOs(List<PointCloudDetectionObject> labelInfos, PointCloudDetectionParamBO preModelParamBO, Map<String, ModelClass> modelClassMap) {
        List<ObjectBO> list = new ArrayList<>(CollUtil.isNotEmpty(labelInfos) ? labelInfos.size() : 0);
        for (PointCloudDetectionObject labelInfo : labelInfos) {
            ObjectBO objectBO = buildObjectBO(labelInfo, preModelParamBO, modelClassMap);
            if (ObjectUtil.isNotNull(objectBO)) {
                list.add(objectBO);
            }
        }
        return list;
    }

    private static ObjectBO buildObjectBO(PointCloudDetectionObject labelInfo, PointCloudDetectionParamBO preModelParamBO, Map<String, ModelClass> modelClassMap) {
        ObjectBO objectBO = ObjectBO.builder().confidence(labelInfo.getConfidence())
                .type("3D_BOX")
                .modelClass(StrUtil.isNotEmpty(labelInfo.getLabel()) ?
                        ObjectUtil.isNotNull(modelClassMap.get(labelInfo.getLabel())) ? modelClassMap.get(labelInfo.getLabel()).getName() : null : null)
                .center3D(buildCenter3D(labelInfo))
                .rotation3D(buildRotation3D(labelInfo))
                .size3D(buildSize3D(labelInfo)).build();
        if (ObjectUtil.isNull(preModelParamBO)) {
            return objectBO;
        } else if (matchResult(labelInfo, preModelParamBO)) {
            return objectBO;
        }
        return null;
    }

    private static boolean matchResult(PointCloudDetectionObject labelInfo, PointCloudDetectionParamBO preModelParamBO) {
        var selectedClasses = new HashSet<>(preModelParamBO.getClasses());
        var selectedUpperClasses = selectedClasses.stream().map(c -> c.toUpperCase()).collect(Collectors.toList());

        boolean matchClassResult = ((CollUtil.isNotEmpty(selectedUpperClasses)
                && selectedUpperClasses.contains(labelInfo.getLabel().toUpperCase()))
                || CollUtil.isEmpty(selectedUpperClasses));

        boolean matchMinConfidence = ((ObjectUtil.isNotNull(preModelParamBO.getMinConfidence())
                && preModelParamBO.getMinConfidence().compareTo(labelInfo.getConfidence()) <= 0)
                || ObjectUtil.isNull(preModelParamBO.getMinConfidence()));

        boolean matchMaxConfidence = ((ObjectUtil.isNotNull(preModelParamBO.getMaxConfidence())
                && preModelParamBO.getMaxConfidence().compareTo(labelInfo.getConfidence()) >= 0)
                || ObjectUtil.isNull(preModelParamBO.getMaxConfidence()));

        return matchClassResult && matchMinConfidence && matchMaxConfidence;
    }

    private static PointBO buildCenter3D(PointCloudDetectionObject labelInfo) {
        return PointBO.builder().x(labelInfo.getX()).y(labelInfo.getY()).z(labelInfo.getZ()).build();
    }

    private static PointBO buildRotation3D(PointCloudDetectionObject labelInfo) {
        return PointBO.builder().x(labelInfo.getRotX()).y(labelInfo.getRotY()).z(labelInfo.getRotZ()).build();
    }

    private static PointBO buildSize3D(PointCloudDetectionObject labelInfo) {
        return PointBO.builder().x(labelInfo.getDx()).y(labelInfo.getDy()).z(labelInfo.getDz()).build();
    }
}
