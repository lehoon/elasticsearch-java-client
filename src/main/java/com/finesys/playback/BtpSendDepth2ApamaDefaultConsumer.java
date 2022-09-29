package com.finesys.playback;

import com.finesys.playback.es.domain.DepthModel;
import com.finesys.playback.es.utils.DateUtils;
import com.finesys.playback.producer.EsPlaybackDefaultProducer;
import com.finesys.playback.producer.EsPlaybackOnceDayProducer;

import java.math.BigDecimal;

/**
 * <p>Title: es行情回放</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/30 11:44</p>
 */
public class BtpSendDepth2ApamaDefaultConsumer extends AbstractPlayBackSendEvent2ApamaConsumer {
    private BtpSendDepth2ApamaDefaultConsumer() {
    }

    public BtpSendDepth2ApamaDefaultConsumer(String serviceId, String instanceId, String backReqId, String channelKey, int playbackRate) {
        super(serviceId, instanceId, backReqId, channelKey, playbackRate);
    }

    @Override
    public boolean init() {
        if (producer == null) {
            logger.info("没有配置producer,调用错误");
            return false;
        }

        //if (!(producer instanceof EsPlaybackDefaultProducer)) {
        if (!(producer instanceof EsPlaybackOnceDayProducer)) {
            logger.info("不是EsPlaybackDefaultProducer类型的producer,调用错误");
            return false;
        }

        if (isEmpty(channelKey)) {
            logger.info("es行情回放，当前apama连接参数为空, 数据回放未进行.");
            return false;
        }

        playbackContext.setCurrentWaitSendCount(producer.totalCount());
        logger.info(String.format("当前待发送的行情总数为%d条", playbackContext.getCurrentWaitSendCount()));
        return true;
    }

    @Override
    protected void update(final DepthModel depth) {
        if (!isTodayLastOne) {
            return;
        }

        isTodayLastOne = false;

        //发送收盘价信息
        logger.info(String.format("当天最后一条行情数据发送收盘价事件."));
        sendContractClosePriceEvent(depth);
        //发送日结信息
        //logger.info(String.format("当天最后一条行情数据发送日结事件"));
        //sendDailySettlementEvent();
        playbackContext.resetDepthYear();
    }

    @Override
    protected void updateDailyDepth(DepthModel depth) {
        if (playbackContext.isSampleOneDay(depth)) {
            return;
        }

        currentDayTime = DateUtils.beforeDay(depth.getCreated());

        try {
            updateDailyLastDepth();
        } catch (EsReaderException e) {
            e.printStackTrace();
            logger.warning(String.format("获取日期[%s]行情数据失败", currentDayTime));
        }
    }

    @Override
    protected void updateDailyLastDepth() throws EsReaderException {
        updateDateTime();
        logger.info(String.format("当前任务当天[%s]行情数据", currentDayTime));
        playbackContext.setCurrentToDayWaitSendCount(((EsPlaybackDefaultProducer) producer).searchDepthCount(currentDayTime));
    }

    @Override
    protected boolean isLastOne(final DepthModel depth) {
        return playbackContext.isLastOne();
    }

    @Override
    protected boolean isTodayLastOne(DepthModel depth) {
        //return playbackContext.isTodayLastOne();
        return playbackContext.isLastDepth(depth);
    }

    @Override
    public void afterProcess() {
        sendPlaybackFinishEvent();
        logger.info(String.format("行情数据回放完成,共回放[%d]条数据,[%s]", consumerCount.get(), requestInfo()));
    }

    @Override
    public void beforeProcess() {
        super.beforeProcess();
        logger.info(String.format("开始进行行情数据回放 %s", requestInfo()));
    }

    @Override
    protected void fillPlaybackContextDepthEvent(Object event) {
        fillPlaybackContextTickEvent(event);
    }

    @Override
    protected void fillPlaybackContextKlineEvent(Object event) {
        if (event == null) return;
    }

    @Override
    protected void fillPlaybackContextTickEvent(Object event) {
        if (event == null) return;
    }

    private float s2Float(final String source) {
        try {
            BigDecimal bigDecimal = new BigDecimal(source);
            return bigDecimal.floatValue();
        } catch (NumberFormatException e) {
            return 0.0f;
        }
    }

    private float d2Float(final double source) {
        try {
            BigDecimal bigDecimal = new BigDecimal(source);
            return bigDecimal.floatValue();
        } catch (NumberFormatException e) {
            return 0.0f;
        }
    }

    @Override
    protected String serviceId() {
        return "DepthDailyEndService";
    }
}
