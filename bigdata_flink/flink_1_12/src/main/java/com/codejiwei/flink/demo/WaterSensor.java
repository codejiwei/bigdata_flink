package com.codejiwei.flink.demo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author: codejiwei
 * Date: 2022/7/8
 * Desc:
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
