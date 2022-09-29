package com.finesys.playback;

import com.finesys.playback.consumer.IConsumer;
import com.finesys.playback.es.domain.DepthModel;
import com.finesys.playback.es.utils.DateUtils;
import com.finesys.playback.producer.EsPlaybackOnceDayProducer;
import com.finesys.playback.producer.IProducer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * <p>Title: playback 2 apama</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/27 16:17</p>
 */
public abstract class AbstractPlayBackSendEvent2ApamaConsumer implements IConsumer<DepthModel> {
    protected Logger logger = Logger.getLogger(AbstractPlayBackSendEvent2ApamaConsumer.class.getName());
    protected String channelKey;
    protected String serviceId;
    protected String instanceId;
    protected String backReqId;
    protected String serverChannel;
    protected String serverPort;
    protected String serverHost;
    protected int playbackRate;
    protected String currentDayTime;
    protected String nextDayTime;
    protected long lastCreated = 0L;
    protected long currentCreated = 0L;
    protected long subTime = 0L;
    protected long oneSecond = 60 * 1000;
    protected PlaybackContext playbackContext = new PlaybackContext();
    protected AtomicLong consumerCount = new AtomicLong(0L);
    protected volatile boolean workDone = false;
    protected volatile boolean isLastOne = false;
    protected volatile boolean isTodayLastOne = false;
    protected IProducer<DepthModel> producer;

    protected AbstractPlayBackSendEvent2ApamaConsumer() {
    }

    public AbstractPlayBackSendEvent2ApamaConsumer(String serviceId,
                                                   String instanceId,
                                                   String backReqId,
                                                   String channelKey,
                                                   int playbackRate) {
        this.serviceId = serviceId;
        this.instanceId = instanceId;
        this.backReqId = backReqId;
        this.channelKey = channelKey;
        this.playbackRate = Math.abs(playbackRate);
    }

    @Override
    public void producer(IProducer<DepthModel> producer) {
        this.producer = producer;
    }

    @Override
    public boolean workDone() {
        if (workDone) return workDone;
        workDone = !producer.hasNext();
        return workDone;
    }

    @Override
    public void process() {
        Date beginTime = DateUtils.formatDepthPlaybackDate1(producer.beginTime()),
                endTime = DateUtils.formatDepthPlaybackDate1(producer.endTime());

        long subDays = DateUtils.subDateTime(beginTime, endTime);
        if (subDays == 0) subDays = 1;
        subDays += 1;

        for (int sub = 0; sub < subDays; sub++) {
            Date beginDate = DateUtils.getMarketBegineTime(beginTime, sub);
            Date endDate = DateUtils.getMarketEndTime(beginDate);

            if (DateUtils.isWorkDay(beginDate)) continue;
            playbackContext.resetDepthMap();
            boolean needDailyEvent = false;
            try {
                ((EsPlaybackOnceDayProducer) producer).setMarketTime(beginDate, endDate);
                if (producer.totalCount() > 0) needDailyEvent = true;
                //查询当日最后一条行情信息数据
                updataTodayLastDepth();
            } catch (EsReaderException e) {
                e.printStackTrace();
                continue;
            }

            while (producer.hasNext()) {
                List<DepthModel> dataList = null;
                try {
                    dataList = producer.next();
                } catch (EsReaderException e) {
                    e.printStackTrace();
                    logger.warning("es查询数据失败");
                }

                if (dataList == null || dataList.isEmpty()) {
                    logger.info(String.format("[%s]日期行情回放完毕,本次共回放[%d]条数据,[%s]",
                            DateUtils.formatSdfDay(beginDate),
                            consumerCount.get(), requestInfo()));
                    break;
                }

                consumerCount.addAndGet(dataList.size());
                logger.info(String.format("开始回放数据,本次回放条数%d", dataList.size()));
                for (DepthModel depth : dataList) {
                    playbackContext.decrementCurrentWaitSendCount();
                    //logger.info(String.format("当前处理行情数据[%s]", depth.toString()));
                    currentCreated = depth.getCreated().getTime();
                    if (lastCreated == 0L) lastCreated = currentCreated;
                    subTime = currentCreated - lastCreated;
                    lastCreated = currentCreated;

                    if (subTime > oneSecond && playbackRate > 0) {
                        long sleepTimeout = subTime / playbackRate;
                        logger.info(String.format("上次行情时间[%d],本次行情时间[%d],回放速率[%d],本次行情需要延迟[%d]毫秒",
                                lastCreated, currentCreated, playbackRate, sleepTimeout));
                        sleep(sleepTimeout);
                    }

                    //发送行情数据
                    logger.info(String.format("当前处理行情数据%s", depth.toString()));
                    isLastOne = isLastOne(depth);
                    isTodayLastOne = isTodayLastOne(depth);
                    send(depth);
                    update(depth);
                    resetPlaybackContext();
                }
            }

            if (!needDailyEvent) continue;
            //发送日结信息
            sendDailySettlementEvent(beginDate);
        }
    }

    //查询当日最后行情数据
    protected void updataTodayLastDepth() throws EsReaderException {
        //获取producer的symbol
        String [] symbols = producer.symbols();
        String [] markets = producer.markets();
        String [] types   = producer.types();

        List<PlaybackEsDepthQuery> depthSymbolList = new ArrayList<PlaybackEsDepthQuery>();
        for (String symbol : symbols) {
            for (String market : markets) {
                for (String type : types) {
                    depthSymbolList.add(new PlaybackEsDepthQuery(symbol, market, type));
                }
            }
        }

        if (depthSymbolList.isEmpty()) return;

        for (PlaybackEsDepthQuery query : depthSymbolList) {
            DepthModel depthModel = ((EsPlaybackOnceDayProducer) producer).searchLastOfToDay(query.market, query.type, query.symbol);
            if (depthModel == null) continue;
            playbackContext.pushDepth(depthModel);
        }
    }

    protected void send(final DepthModel depth) {
        String type = depth.getType();
        if ("L1".equals(type) || "L2".equals(type)) {
            sendDepthEvent(depth);
        } else if ("Tick".equals(type)) {
            sendTickEvent(depth);
        } else if (type.contains("KLine_")) {
            sendKLineEvent(depth);
        }
    }

    protected void sendEvent(Object event) {
        if (event == null) {
            System.out.println("非法的行情数据类型");
            return;
        }
        //logger.info(String.format("发送PlaybackBaseEvent到apama. %s", requestInfo()));
    }

    private void sendDepthEvent(final DepthModel depth) {
        try {
            Object event = depth;
            if (isTodayLastOne) {
                fillPlaybackContextDepthEvent(event);
            }

        } catch (Exception e) {
           e.printStackTrace();
        }
    }

    private void sendTickEvent(final DepthModel depth) {
        try {
            if (isTodayLastOne) {
                fillPlaybackContextTickEvent(depth);
            }
            sendEvent(depth);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendKLineEvent(final DepthModel depth) {
        try {
            if (isTodayLastOne) {
                fillPlaybackContextKlineEvent(depth);
            }
            sendEvent(depth);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //发送日结事件
    protected void sendDailySettlementEvent(Date currentDate) {
        //发送日结事件
        //logger.info(String.format("发送日结事件到行情服务"));

        //获取当日时间字符串

        //获取下一个交易日期字符串
        Date nextTradeDay = currentDate;
        while(true) {
            nextTradeDay = DateUtils.nextDay(nextTradeDay);
            if (!DateUtils.isWorkDay(nextTradeDay)) break;
        }

        String currentTradeDate = DateUtils.formatSdfDay(currentDate);
        String nextTradeDate = DateUtils.formatSdfDay(nextTradeDay);

        System.out.println("日结事件, 当前交易日期:" + currentTradeDate + ", 下个交易日期:" + nextTradeDate);
    }

    protected void sendContractClosePriceEvent(final DepthModel depth) {
        //logger.info(String.format("发送收盘价事件到行情服务"));
    }

    /**读取行情结束事件发送*/
    protected void sendPlaybackFinishEvent() {
        logger.info(String.format("发送回放结束事件到行情服务"));
    }

    private void resetPlaybackContext() {
        playbackContext.reset();
    }

    protected void sleep(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException ignored) {
        }
    }

    protected void updateDateTime() {
        if (isEmpty(currentDayTime)) {
            currentDayTime = DateUtils.playbackDateTime(producer.beginTime());
        } else {
            currentDayTime = DateUtils.nextDay(currentDayTime);
        }

        nextDayTime = DateUtils.nextDay(currentDayTime);
    }

    protected boolean isEmpty(final String source) {
        return source == null || source.length() == 0;
    }

    protected boolean validDepth(final DepthModel depth) {
        if (isEmpty(depth.getType())) {
            logger.info(String.format("当前回放行情类型为空, 忽略该行情数据 {}", depth));
            return false;
        }

        if (isEmpty(depth.getMarket())) {
            logger.info(String.format("当前回放行情类型为空, 忽略该行情数据 {}", depth));
            return false;
        }

        if (isEmpty(depth.getData())) {
            logger.info(String.format("当前回放行情类型为空, 忽略该行情数据 {}", depth));
            return false;
        }

        if (isEmpty(depth.getTime())) {
            logger.info(String.format("当前回放行情类型为空, 忽略该行情数据 {}", depth));
            return false;
        }

        if (isEmpty(depth.getSymbol())) {
            logger.info(String.format("当前回放行情类型为空, 忽略该行情数据 {}", depth));
            return false;
        }

        if (depth.getCreated() == null) {
            logger.info(String.format("当前回放行情类型为空, 忽略该行情数据 {}", depth));
            return false;
        }

        return true;
    }

    protected String requestInfo() {
        StringBuilder requestSb = new StringBuilder(128);
        requestSb.append("serviceId=")
                .append(serviceId)
                .append(",instanceId=")
                .append(instanceId)
                .append(",backReqId=")
                .append(backReqId)
                .append(",playbackRate=")
                .append(playbackRate)
                .append(",channelKey")
                .append(channelKey)
                .append(",")
                .append(serviceId)
                .append(",")
                .append(serviceId)
                .append(",")
                .append(producer.request());
        return requestSb.toString();
    }

    public void setServerChannel(String serverChannel) {
        this.serverChannel = serverChannel;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public void setServerHost(String serverHost) {
        this.serverHost = serverHost;
    }

    @Override
    public void beforeProcess() {
    }

    @Override
    public String instanceId() {
        return instanceId;
    }

    protected abstract void update(final DepthModel depth);
    protected abstract boolean isLastOne(final DepthModel depth);
    protected abstract boolean isTodayLastOne(final DepthModel depth);
    protected abstract void updateDailyDepth(final DepthModel depth);
    protected abstract void updateDailyLastDepth() throws EsReaderException;
    protected abstract String serviceId();
    protected abstract void fillPlaybackContextDepthEvent(Object event);
    protected abstract void fillPlaybackContextKlineEvent(Object event);
    protected abstract void fillPlaybackContextTickEvent(Object event);

    protected final static String EVENT_OUT_DEPTH_ISLAST_TRUE_VALUE           = "1";
    protected final static String EVENT_OUT_DEPTH_ISLAST_FALSE_VALUE          = "0";

    protected final static String EVENT_OUT_DEPTH_ISLAST_KEY                  = "isLast";
    protected final static String EVENT_OUT_DEPTH_BACKREQ_ID_KEY              = "BackReqId";
    protected final static String EVENT_OUT_DEPTH_SERVER_IP_KEY               = "ServerIp";
    protected final static String EVENT_OUT_DEPTH_SERVER_PORT_KEY             = "ServerPort";
    protected final static String EVENT_OUT_DEPTH_SERVER_CHANNEL_KEY          = "ServerChannel";

    protected final static String EVENT_OUT_CLIENT_NAME_KEY                   = "ClientName";
    protected final static String EVENT_OUT_CLIENT_VERSION_KEY                = "ClientVersion";
    protected final static String EVENT_OUT_DATA_SOURCE_KEY                   = "DataSource";
}
