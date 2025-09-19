package ai.basic.x1.entity;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @author andy
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class ObjectBO {

    private String modelClass;
    private String type;
    private BigDecimal confidence;
    private PointBO center3D;
    private PointBO rotation3D;
    private PointBO size3D;

    @Builder.Default
    private String id = "UNKNOWN";
    @Builder.Default
    private String trackId = "UNKNOWN";
    @Builder.Default
    private String trackName = "UNKNOWN";
    @Builder.Default
    private int pointN = -1;
    @Builder.Default
    private long classId = -1L;

}
