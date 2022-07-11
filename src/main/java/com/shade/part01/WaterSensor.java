package com.shade.part01;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: shade
 * @date: 2022/7/1 19:11
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class WaterSensor {
    private String id;
    private long ts;
    private Integer vc;
}
