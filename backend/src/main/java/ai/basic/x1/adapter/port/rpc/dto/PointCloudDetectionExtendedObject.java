package ai.basic.x1.adapter.port.rpc.dto;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * @author andy
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PointCloudDetectionExtendedObject {

    private String id;
    private String type;
    private Integer classId;
    private String className;
    private String trackId;
    private String trackName;
    private List<Object> classValues;
    private Contour contour;
    private BigDecimal modelConfidence;
    private String modelClass;

    @Data
    public static class Contour {
        private Integer pointN;
        private List<Object> points;
        private Size3D size3D;
        private Center3D center3D;
        private Integer viewIndex;
        private Rotation3D rotation3D;
    }

    @Data
    public static class Size3D {
        private BigDecimal x;
        private BigDecimal y;
        private BigDecimal z;
    }

    @Data
    public static class Center3D {
        private BigDecimal x;
        private BigDecimal y;
        private BigDecimal z;
    }

    @Data
    public static class Rotation3D {
        private BigDecimal x;
        private BigDecimal y;
        private BigDecimal z;
    }

}
