package com.apama.marketdata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/20 16:22</p>
 */
@Setter
@Getter
@NoArgsConstructor
public class Depth implements Serializable {
    private String symbol;
    private List<Float> bidPrices;
    private List<Float> askPrices;
    private List<Float> midPrices;
    private List<Integer> bidQuantities;
    private List<Integer> askQuantities;
    private Map<String, String> extraParams;
}
