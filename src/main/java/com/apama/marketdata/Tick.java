package com.apama.marketdata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/27 11:28</p>
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
public class Tick implements Serializable {
    private String symbol;
    private Float  price;
    private Integer quantity;
    private Map<String, String> extraParams;
}
