package com.finesys.playback.es.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQuery;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.json.JsonData;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.es.utils.DateUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/30 9:25</p>
 */
public class EsDepthDefaultClient extends AbstractEsDepthClient {
    private String [] symbols = null;
    private String [] markets = null;
    private String [] types = null;
    private String beginTime = null;
    private String endTime = null;

    public EsDepthDefaultClient(String indexName) {
        super(indexName);
    }

    public EsDepthDefaultClient(ElasticsearchClient client) {
        super(client);
    }

    public EsDepthDefaultClient(String [] symbols, String [] markets, String [] types,
                                Date beginTime, Date endTime) {
        super();
        this.symbols = symbols;
        this.markets = markets;
        this.types = types;
        setMarketTime(beginTime, endTime);
    }

    public EsDepthDefaultClient(String indexName, String [] symbols, String [] markets, String [] types,
                                Date beginTime, Date endTime, ElasticsearchClient client) {
        super(indexName, client);
        this.symbols = symbols;
        this.markets = markets;
        this.types = types;
        setMarketTime(beginTime, endTime);
    }

    @Override
    public long count() throws EsReaderException {
        if (client == null) throw new EsReaderException("elasticsearch连接已断开");
        CountRequest.Builder countRequestBuilder = new CountRequest.Builder();
        countRequestBuilder.index(indexName);
        countRequestBuilder.query(q -> q.bool( b -> b
                .must(q1 -> q1.range(t1 -> t1.field("created").gte(JsonData.of(DateUtils.fromStringFormat(beginTime))).lte(JsonData.of(DateUtils.fromStringFormat(endTime)))))
                .must(s -> s.terms(symbolQuery()))
                .must(m -> m.terms(marketQuery()))
                .must(t -> t.terms(typeQuery()))
        ));

        CountRequest request = countRequestBuilder.build();
        System.out.println(request.toString());
        try {
            CountResponse response = client.count(request);
            return response.count();
        } catch (IOException e) {
            throw new EsReaderException("查询elasticsearch回放数据异常", e);
        }
    }

    @Override
    public long count(String today) throws EsReaderException {
        Date todayDateTime = DateUtils.fomatDate(today);
        Date nextDateTime = DateUtils.nextDay(todayDateTime);
        String begineDateTime = DateUtils.formatDepthPlaybackDate(todayDateTime);
        String endDateTime = DateUtils.formatDepthPlaybackDate(nextDateTime);
        if (client == null) throw new EsReaderException("elasticsearch连接已断开");
        CountRequest.Builder countRequestBuilder = new CountRequest.Builder();
        countRequestBuilder.index(indexName);
        countRequestBuilder.query(q -> q.bool( b -> b
                .must(c1 -> c1.range(t1 -> t1.field("created")
                        .gt(JsonData.of(DateUtils.fromStringFormat(begineDateTime)))
                        .lt(JsonData.of(DateUtils.fromStringFormat(endDateTime)))))
                .must(s -> s.terms(symbolQuery()))
                .must(m -> m.terms(marketQuery()))
                .must(t -> t.terms(typeQuery()))
        ));

        CountRequest request = countRequestBuilder.build();
        try {
            CountResponse response = client.count(request);
            return response.count();
        } catch (IOException e) {
            throw new EsReaderException("查询elasticsearch回放数据异常", e);
        }
    }

    @Override
    protected SearchRequest makeSearchRequest(int page, int size) {
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                        .must(c1 -> c1.range(t1 -> t1.field("created")
                                .gte(JsonData.of(DateUtils.fromStringFormat(beginTime)))
                                .lte(JsonData.of(DateUtils.fromStringFormat(endTime)))))
                        .must(s -> s.terms(symbolQuery()))
                        .must(m -> m.terms(marketQuery()))
                        .must(t -> t.terms(typeQuery()))
                ))
                .from(page == 0 ? 0 : (page - 1) * size)
                .size(size);

        return requestBuilder.build();
    }

    @Override
    protected SearchRequest makeSearchRequestWithSearchAfter(int size) {
        //request builder instance
        SearchRequest.Builder requestBuilder = searchRequestBuilder();
        requestBuilder.sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                        .must(c1 -> c1.range(t1 -> t1.field("created")
                                .gte(JsonData.of(DateUtils.fromStringFormat(beginTime)))
                                .lte(JsonData.of(DateUtils.fromStringFormat(endTime)))))
                        .must(s -> s.terms(symbolQuery()))
                        .must(m -> m.terms(marketQuery()))
                        .must(t -> t.terms(typeQuery()))
                ))
                .from(0)
                .size(size)
                .searchAfter(lastId);

        return requestBuilder.build();
    }

    @Override
    protected SearchRequest makeLastOfToDaySearchRequest(String type, String today) {
        Date todayDateTime = DateUtils.fomatDate(today);
        Date nextDateTime = DateUtils.nextDay(todayDateTime);
        String begineDateTime = DateUtils.formatDepthPlaybackDate(todayDateTime);
        String endDateTime = DateUtils.formatDepthPlaybackDate(nextDateTime);
        //request builder instance
        SearchRequest.Builder requestBuilder = searchRequestBuilder();
        requestBuilder.sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Desc)))
                .query(q -> q.bool( q1 -> q1
                        .must(c1 -> c1.range(t1 -> t1.field("created")
                                .gt(JsonData.of(DateUtils.fromStringFormat(begineDateTime)))
                                .lt(JsonData.of(DateUtils.fromStringFormat(endDateTime)))))
                        .must(s -> s.terms(symbolQuery()))
                        .must(m -> m.terms(marketQuery()))
                        .must(t -> t.terms(typeQuery()))
                ))
                .from(0)
                .size(1);
        return requestBuilder.build();
    }

    @Override
    protected SearchRequest makeLastOfToDaySearchRequest(String market,
                                                         String type,
                                                         String symbol,
                                                         Date beginDate,
                                                         Date endDate) {
        String begineDateTime = DateUtils.formatDepthPlaybackDate(beginDate);
        String endDateTime = DateUtils.formatDepthPlaybackDate(endDate);
        //request builder instance
        SearchRequest.Builder requestBuilder = searchRequestBuilder();
        requestBuilder.sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Desc)))
                .query(q -> q.bool( q1 -> q1
                        .must(c1 -> c1.range(t1 -> t1.field("created")
                                .gt(JsonData.of(DateUtils.fromStringFormat(begineDateTime)))
                                .lt(JsonData.of(DateUtils.fromStringFormat(endDateTime)))))
                        .must(s -> s.term(st -> st.field("symbol").value(FieldValue.of(symbol))))
                        .must(m -> m.term(mt -> mt.field("market").value(FieldValue.of(market))))
                        .must(t -> t.term(tt -> tt.field("type").value(FieldValue.of(type))))
                ))
                .from(0)
                .size(1);
        return requestBuilder.build();
    }

    @Override
    protected SearchRequest makeLastSearchRequest() {
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Desc)))
                .query(q -> q.bool( q1 -> q1
                        .must(c1 -> c1.range(t1 -> t1.field("created")
                                .gt(JsonData.of(DateUtils.fromStringFormat(beginTime)))
                                .lte(JsonData.of(DateUtils.fromStringFormat(endTime)))))                                .must(s -> s.terms(symbolQuery()))
                        .must(m -> m.terms(marketQuery()))
                        .must(t -> t.terms(typeQuery()))
                ))
                .from(0)
                .size(1);
        return requestBuilder.build();
    }

    @Override
    public String [] symbol() {
        return symbols;
    }

    @Override
    public String [] market() {
        return markets;
    }

    @Override
    public String[] type() {
        return types;
    }

    @Override
    public String beginTime() {
        return beginTime;
    }

    @Override
    public String endTime() {
        return endTime;
    }

    @Override
    public void setMarketTime(Date beginTime, Date endTime) {
        this.beginTime = DateUtils.formatDepthPlaybackDate(beginTime);
        this.endTime = DateUtils.formatDepthPlaybackDate(checkTime(endTime));
    }

    protected Date checkTime(Date datetime) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(datetime);
        if (calendar.get(Calendar.HOUR_OF_DAY) != 23) {
            calendar.set(Calendar.HOUR_OF_DAY, 23);
        }

        if (calendar.get(Calendar.MINUTE) != 59) {
            calendar.set(Calendar.MINUTE, 59);
        }

        if (calendar.get(Calendar.SECOND) != 59) {
            calendar.set(Calendar.SECOND, 59);
        }

        return calendar.getTime();
    }

    private TermsQuery symbolQuery() {
        List<FieldValue> symbolList = new ArrayList<>(symbols.length);
        for (String symbol : symbols) {
            symbolList.add(FieldValue.of(symbol));
        }

        TermsQuery.Builder builder = new TermsQuery.Builder();
        builder.field("symbol").terms(s -> s.value(symbolList));
        return builder.build();
    }

    private TermsQuery marketQuery() {
        List<FieldValue> marketList = new ArrayList<>(markets.length);
        for (String market : markets) {
            marketList.add(FieldValue.of(market));
        }

        TermsQuery.Builder builder = new TermsQuery.Builder();
        builder.field("market").terms(s -> s.value(marketList));
        return builder.build();
    }

    private TermsQuery typeQuery() {
        List<FieldValue> typeList = new ArrayList<>(types.length);
        for (String type : types) {
            typeList.add(FieldValue.of(type));
        }

        TermsQuery.Builder builder = new TermsQuery.Builder();
        builder.field("type").terms(s -> s.value(typeList));
        return builder.build();
    }
}
