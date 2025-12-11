package ai.basic.x1.adapter.port.dao;

import ai.basic.x1.adapter.port.dao.mybatis.mapper.UploadRecordMapper;
import ai.basic.x1.adapter.port.dao.mybatis.model.UploadRecord;
import org.springframework.stereotype.Component;
import java.util.List;

/**
 * @author fyb
 * @date 2022-08-30 11:48:13
 */
@Component
public class UploadRecordDAO extends AbstractDAO<UploadRecordMapper, UploadRecord> {
    public List<UploadRecord> findAllByDatasetId(Long datasetId) {
        return findAllByField(UploadRecord::getDatasetId, datasetId);
    }
}
