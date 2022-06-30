package com.finesys.elasticsearch.client.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQuery;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.json.JsonData;
import com.finesys.elasticsearch.client.utils.DateUtils;

import java.io.IOException;
import java.util.ArrayList;
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
    private long longBeginTime = 0l;
    private long longEndTime = 0l;

    public EsDepthDefaultClient(String indexName) {
        super(indexName);
    }

    public EsDepthDefaultClient(ElasticsearchClient client) {
        super(client);
    }

    public EsDepthDefaultClient(String indexName, String [] symbols, String [] markets, String [] types,
                                 String beginTime, String endTime, ElasticsearchClient client) {
        super(indexName, client);
        this.symbols = symbols;
        this.markets = markets;
        this.types = types;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.longBeginTime = DateUtils.fromStringFormat(beginTime);
        this.longEndTime = DateUtils.fromStringFormat(endTime);
    }

    @Override
    public long count() throws IOException {
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
        CountResponse response = client.count(request);
        return response.count();
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


    @Override
    protected SearchRequest makeSearchRequest(int page, int size) {
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                                .must(m1 -> m1.range(c1 -> c1.field("created").gte(JsonData.of(longBeginTime)).lte(JsonData.of(longEndTime))))
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
                        .must(m1 -> m1.range(c1 -> c1.field("created").gte(JsonData.of(longBeginTime)).lte(JsonData.of(longEndTime))))
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
        final long nextDayTime = DateUtils.nextDayZeroTime(today);
        final long upDayTime = DateUtils.upDayZeroTime(today);
        //request builder instance
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Desc)))
                .query(q -> q.bool( q1 -> q1
                        .must(m1 -> m1.range(c1 -> c1.field("created").lt(JsonData.of(nextDayTime)).gt(JsonData.of(upDayTime))))
                        .must(s -> s.terms(symbolQuery()))
                        .must(m -> m.terms(marketQuery()))
                        .must(t -> t.terms(typeQuery()))
                ))
                .from(0)
                .size(1);

        return requestBuilder.build();
    }

    @Override
    public String symbol() {
        return symbol();
    }

    @Override
    public String market() {
        return null;
    }

    @Override
    public String beginTime() {
        return null;
    }

    @Override
    public String endTime() {
        return null;
    }
}
