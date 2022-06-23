package com.lehoon.elasticsearch.client.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.lehoon.elasticsearch.client.domain.DepthModel;
import com.lehoon.elasticsearch.client.domain.EsDataModel;
import com.lehoon.elasticsearch.client.utils.DateUtils;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
public class EsDepthClient {
    private String indexName = null;
    private String symbol = null;
    private String market = null;
    private String type = null;
    private String beginTime = null;
    private String endTime = null;
    //search response last _id
    private String lastId;
    private long longBeginTime = 0l;
    private long longEndTime = 0l;

    //es search client
    private ElasticsearchClient client = null;

    public EsDepthClient(ElasticsearchClient client) {
        this.client = client;
    }

    public EsDepthClient(String indexName, String symbol, String market, String type,
                         String beginTime, String endTime, ElasticsearchClient client) {
        this.indexName = indexName;
        this.symbol = symbol;
        this.market = market;
        this.type = type;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.client = client;
        this.longBeginTime = DateUtils.fromStringFormat(beginTime);
        this.longEndTime = DateUtils.fromStringFormat(endTime);
    }

    //是否工作状态
    public boolean isWorking() throws IOException {
        if (client == null) return false;
        return client.ping().value();
    }

    //查询符合条件的数据数量
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

    private void updateState(final int page) {
        if (page == 1) {
            lastId = "";
        }
    }

    public List<DepthModel> searchPage(final int page,
                                       final int size) throws IOException {
        int currentPage = page < 1 ? 1 : page;
        updateState(currentPage);

        if (currentPage == 1) {
            return searchDefaultPage(currentPage, size);
        }

        return searchPageWithSearchAfter(size);
    }

    /**
     * 分页查询
     */
    public List<DepthModel> searchDefaultPage(final int page,
                                             final int size) throws IOException {

        //根据created排序
        SearchRequest searchRequest = makeSearchRequest(page, size);
        return searchFromEs(searchRequest);
    }

    /**
     * 分页查询
     */
    public List<DepthModel> searchPageWithSearchAfter(final int size) throws IOException {
        //根据created排序
        SearchRequest searchRequest = makeSearchRequestWithSearchAfter(size);
        return searchFromEs(searchRequest);
    }

    /**
     * 写入单条数据
     * @param dataModel
     * @return
     * @throws IOException
     */
    public boolean pushData(final EsDataModel dataModel) throws IOException {
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.operations(op -> op
                .index(idx -> idx
                        .index("finesys_depth01")
                        .document(dataModel.getData())
                        .id(dataModel.getId())));

        BulkResponse response = client.bulk(builder.build());
        return response.errors();
    }

    /**
     * 批量写入数据
     * @param depthList  数据集合
     * @throws IOException
     */
    public void batchPushData(List<EsDataModel<DepthModel>> depthList) throws IOException {
        for (EsDataModel depth : depthList) {
            pushData(depth);
        }
    }

    /**
     * 分页查询数据
     * @param page
     * @param size
     * @return
     */
    private SearchRequest makeSearchRequest(final int page,
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
    private SearchRequest makeSearchRequestWithSearchAfter(final int size) {
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

    private List<DepthModel> searchFromEs(final SearchRequest request) throws IOException {
        SearchResponse<DepthModel> response = client.search(request, DepthModel.class);
        if (response.hits().hits().size() > 0) {
            List<Hit<DepthModel>> hits = response.hits().hits();
            DepthModel lastDepth = hits.get(hits.size() - 1).source();
            lastId = String.valueOf(lastDepth.getCreated().getTime());
            return hits.stream().map((e) -> e.source()).collect(Collectors.toList());
        }
        return new ArrayList<DepthModel>();
    }

}
