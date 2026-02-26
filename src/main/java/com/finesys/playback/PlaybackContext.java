package com.finesys.playback;

import com.finesys.playback.es.domain.DepthModel;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/7/26 16:16</p>
 */
public class PlaybackContext {
    public final static String EVENT_CLOSE_PRICE_KEY                           = "closingPrice";
    public final static String EVENT_SETTLEMENT_PRICE_KEY                      = "settlePrice";

    private float closePrice;
    private float settlementPrice;
    private int currentDepthYear;
    private int currentDepthMonth;
    private int currentDepthDay;
    private Map<String, Long> PLAYBACK_DEPTH_MAP = new HashMap<String, Long>();

    private AtomicLong currentWaitSendCount = new AtomicLong(0);
    private AtomicLong currentToDayWaitSendCount = new AtomicLong(0);

    public PlaybackContext() {
    }

    public float getClosePrice() {
        return closePrice;
    }

    public void setClosePrice(float closePrice) {
        this.closePrice = closePrice;
    }

    public float getSettlementPrice() {
        return settlementPrice;
    }

    public void setSettlementPrice(float settlementPrice) {
        this.settlementPrice = settlementPrice;
    }

    public long getCurrentWaitSendCount() {
        return currentWaitSendCount.get();
    }

    public void setCurrentWaitSendCount(long currentWaitSendCount) {
        this.currentWaitSendCount.set(currentWaitSendCount);
    }

    public long getCurrentToDayWaitSendCount() {
        return currentToDayWaitSendCount.get();
    }

    public void setCurrentToDayWaitSendCount(long currentToDayWaitSendCount) {
        this.currentToDayWaitSendCount.set(currentToDayWaitSendCount);
    }

    public boolean isLastOne() {
        return currentWaitSendCount.get() == 0;
    }

    public boolean isTodayLastOne() {
        return currentToDayWaitSendCount.get() == 0;
    }

    public void decrementCurrentTodayWaitSendCount() {
        this.currentToDayWaitSendCount.decrementAndGet();
    }

    public void decrementCurrentWaitSendCount() {
        this.currentWaitSendCount.decrementAndGet();
    }

    public void resetCurrentDayCount() {
        this.currentToDayWaitSendCount.set(0);
    }

    public boolean isSampleOneDay(final DepthModel depth) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(depth.getCreated());

        if (this.currentDepthYear == 0) {
            this.currentDepthYear = calendar.get(Calendar.YEAR);
            this.currentDepthMonth = calendar.get(Calendar.MONTH);
            this.currentDepthDay = calendar.get(Calendar.DAY_OF_MONTH);
            return false;
        }

        return this.currentDepthDay == calendar.get(Calendar.DAY_OF_MONTH) &&
                this.currentDepthMonth == calendar.get(Calendar.MONTH) &&
                this.currentDepthYear == calendar.get(Calendar.YEAR);
    }

    public void resetDepthYear() {
        this.currentDepthYear = 0;
    }

    public void pushDepth(DepthModel depthModel) {
        String key = String.format("%s-%s-%s", depthModel.getMarket(), depthModel.getType(), depthModel.getSymbol());
        PLAYBACK_DEPTH_MAP.put(key, depthModel.getCreated().getTime());
    }

    public boolean isLastDepth(final DepthModel depthModel) {
        String key = String.format("%s-%s-%s", depthModel.getMarket(), depthModel.getType(), depthModel.getSymbol());
        if (!PLAYBACK_DEPTH_MAP.containsKey(key)) return false;
        long v = PLAYBACK_DEPTH_MAP.get(key);
        return v == depthModel.getCreated().getTime();
    }

    public void resetDepthMap() {
        this.PLAYBACK_DEPTH_MAP.clear();
    }

    public void reset() {
        setClosePrice(0.0f);
        setClosePrice(0.0f);
    }

    protected boolean isEmpty(final String source) {
        return source == null || source.length() == 0;
    }
}

class PlaybackEsDepthQuery {
    public String symbol;
    public String market;
    public String type;

    public PlaybackEsDepthQuery(String symbol, String market, String type) {
        this.symbol = symbol;
        this.market = market;
        this.type = type;
    }
}