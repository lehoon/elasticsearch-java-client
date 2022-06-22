package com.apama.marketdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/20 16:25</p>
 */
public class DepthBuilder {
    private Depth depth;

    public DepthBuilder() {
        depth = new Depth();
    }

    public DepthBuilder symbol(final String symbol) {
        this.depth.setSymbol(symbol);
        return this;
    }

    public DepthBuilder bidPrices(final List<Float> bidPrices) {
        depth.setBidPrices(bidPrices);
        return this;
    }

    public DepthBuilder defaultDepth(final String symbol) {
        depth.setSymbol(symbol);
        //初始化默认的属性
        depth.setBidPrices(bidPrices());
        depth.setAskPrices(askPrices());
        depth.setMidPrices(midPrices());
        depth.setAskQuantities(askQuantities());
        depth.setBidQuantities(bidQuantities());
        depth.setExtraParams(extParams());
        return this;
    }

    private List<Float> bidPrices() {
        List<Float> bidPriceList = new ArrayList<Float>(4);
        bidPriceList.add(5103.000000f);
        bidPriceList.add(0.000000f);
        bidPriceList.add(0.000000f);
        bidPriceList.add(0.000000f);
        bidPriceList.add(0.000000f);
        return bidPriceList;
    }

    private List<Float> askPrices() {
        List<Float> askPriceList = new ArrayList<Float>(4);
        askPriceList.add(5104.000000f);
        askPriceList.add(0.000000f);
        askPriceList.add(0.000000f);
        askPriceList.add(0.000000f);
        askPriceList.add(0.000000f);
        return askPriceList;
    }

    private List<Float> midPrices() {
        List<Float> midPriceList = new ArrayList<Float>(0);
        return midPriceList;
    }

    private List<Integer> bidQuantities() {
        List<Integer> bidQuantitieList = new ArrayList<Integer>(5);
        bidQuantitieList.add(1);
        bidQuantitieList.add(0);
        bidQuantitieList.add(0);
        bidQuantitieList.add(0);
        bidQuantitieList.add(0);
        return bidQuantitieList;
    }

    private List<Integer> askQuantities() {
        List<Integer> askQuantitieList = new ArrayList<Integer>(5);
        askQuantitieList.add(21);
        askQuantitieList.add(0);
        askQuantitieList.add(0);
        askQuantitieList.add(0);
        askQuantitieList.add(0);
        return askQuantitieList;
    }

    private Map<String, String> extParams() {
        Map<String, String> extParam = new HashMap<String, String>(8);
        extParam.put("closingPrice", "0.00000");
        extParam.put("dealAmount", "212791230.00000");
        extParam.put("dealQty", "2831");
        extParam.put("exchangeId", "");
        extParam.put("fallLimit", "4443.00000");
        extParam.put("highestPrice", "5118.00000");
        extParam.put("lastPrice", "5104.00000");
        extParam.put("lowestPrice", "4922.00000");
        extParam.put("openingPrice", "4936.00000");
        extParam.put("positon", "10718.00000");
        extParam.put("preClosingPrice", "4936.00000");
        extParam.put("preSettlePrice", "4937.00000");
        extParam.put("riseLimit", "5430.00000");
        extParam.put("serviceId", "ctp");
        extParam.put("settlePrice", "0.00000");
        extParam.put("tradingTime", "20220224 19:35:16.500");
        return extParam;
    }


    public Depth build() {
        return depth;
    }
}
