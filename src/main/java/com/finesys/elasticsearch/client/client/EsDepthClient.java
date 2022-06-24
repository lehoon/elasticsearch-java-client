package com.finesys.elasticsearch.client.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.json.JsonData;
import com.finesys.elasticsearch.client.utils.DateUtils;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

/**
 * <p>Title: 行情数据es客户端</p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/23 9:34</p>
 */
@Setter
@Getter
public class EsDepthClient extends AbstractEsDepthClient {
    private String symbol = null;
    private String market = null;
    private String type = null;
    private String beginTime = null;
    private String endTime = null;
    private long longBeginTime = 0l;
    private long longEndTime = 0l;

    public EsDepthClient(ElasticsearchClient client) {
        super(client);
    }

    public EsDepthClient(String indexName, String symbol, String market, String type,
                         String beginTime, String endTime, ElasticsearchClient client) {
        super(indexName, client);
        this.indexName = indexName;
        this.symbol = symbol;
        this.market = market;
        this.type = type;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.longBeginTime = DateUtils.fromStringFormat(beginTime);
        this.longEndTime = DateUtils.fromStringFormat(endTime);
    }

    //查询符合条件的数据数量
    @Override
    public long count() throws IOException {
        CountResponse response = client.count(c -> c
                .index(indexName)
                .query(q -> q.bool( t -> t
                        .must(q1 -> q1.range(t1 -> t1.field("created").gte(JsonData.of(DateUtils.fromStringFormat(beginTime))).lte(JsonData.of(DateUtils.fromStringFormat(endTime))))) //时间区间
                        .must(ss -> ss.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))   //symbol=symbol
                        .must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))     //market=martket
                        .must(ty -> ty.term(t1 -> t1.field("type").value(v -> v.stringValue(type)))) //type=type
                ))
        );

        return response.count();
    }

     /**
     * 分页查询数据
     * @param page
     * @param size
     * @return
     */
     @Override
     protected SearchRequest makeSearchRequest(final int page,
                                            final int size) {
        //request builder instance
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                        .must(m1 -> m1.range(c1 -> c1.field("created").gte(JsonData.of(longBeginTime)).lte(JsonData.of(longEndTime))))
                        .must(m2 -> m2.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))
                        .must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))
                        .must(s -> s.term(t1 -> t1.field("type").value(v -> v.stringValue(type))))
                ))
                .from(page == 0 ? 0 : page * size)
                .size(size);

        return requestBuilder.build();
    }

    //根据searchAfter查询分页数据
    @Override
    protected SearchRequest makeSearchRequestWithSearchAfter(final int size) {
        //request builder instance
        SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
        requestBuilder.index(indexName)
                .sort(sort -> sort.field(f -> f.field("created").order(SortOrder.Asc)))
                .query(q -> q.bool( q1 -> q1
                        .must(m1 -> m1.range(c1 -> c1.field("created").gte(JsonData.of(longBeginTime)).lte(JsonData.of(longEndTime))))
                        .must(m2 -> m2.term(s1 -> s1.field("symbol").value(v -> v.stringValue(symbol))))
                        .must(m -> m.term(m1 -> m1.field("market").value(v -> v.stringValue(market))))
                        .must(s -> s.term(t1 -> t1.field("type").value(v -> v.stringValue(type))))
                ))
                .from(0)
                .size(size)
                .searchAfter(lastId)
        ;

        return requestBuilder.build();
    }

    public void setBeginTime(String beginTime) {
        this.beginTime = beginTime;
        this.longBeginTime = DateUtils.fromStringFormat(beginTime);

    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
        this.longEndTime = DateUtils.fromStringFormat(endTime);
    }
}
