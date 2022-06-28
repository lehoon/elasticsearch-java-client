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
 * <p>Date: 2022/6/27 11:38</p>
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
public class KLineDepth implements Serializable {
    private Integer seqNumber;
    private String symbol;
    private String startTime;
    private String endTime;
    private String type;
    private Float openingPrice;
    private Float closingPrice;
    private Float HighesPrice;
    private Float lowestPrice;
    private Float dealAmount;
    private Float dealQty;
    private Float position;
    private Map<String, String> extraParams;
}
