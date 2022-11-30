package com.hmdp.dto;

import lombok.Data;

import java.util.List;

/**
 * @author 54656 on 2022/11/21 15:27
 * 滚动分页的返回结果类型
 */
@Data
public class ScrollResult {
    private List<?> list;
    private Long minTime;
    private Integer offset;
}
