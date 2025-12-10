package ai.basic.x1.adapter.port.dao;

import ai.basic.x1.adapter.port.dao.mybatis.mapper.DataAnnotationObjectMapper;
import ai.basic.x1.adapter.port.dao.mybatis.model.DataAnnotationObject;
import org.springframework.stereotype.Component;

/**
 * @author chenchao
 * @date 2022/8/26
 */
@Component
public class DataAnnotationObjectDAO extends AbstractDAO<DataAnnotationObjectMapper, DataAnnotationObject>{

    public long countByDatasetIdAndSourceId(Long datasetId, Long sourceId) {
        return lambdaQuery().eq(DataAnnotationObject::getDatasetId, datasetId)
                .eq(DataAnnotationObject::getSourceId, sourceId)
                .count();
    }

    public long countGTByDatasetId(Long datasetId) {
        return lambdaQuery().eq(DataAnnotationObject::getDatasetId, datasetId)
                .and(wq -> wq.eq(DataAnnotationObject::getSourceId, -1L).or().isNull(DataAnnotationObject::getSourceId))
                .count();
    }
}
