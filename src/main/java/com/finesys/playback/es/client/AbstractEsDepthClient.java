package com.finesys.playback.es.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.finesys.playback.EsReaderException;
import com.finesys.playback.EsWriteException;
import com.finesys.playback.es.domain.DepthModel;
import com.finesys.playback.es.domain.EsDataModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/24 16:20</p>
 */
public abstract class AbstractEsDepthClient {
    protected String lastId;
    protected String indexName = null;
    //es search client
    protected ElasticsearchClient client = null;

    public AbstractEsDepthClient() {
    }

    public AbstractEsDepthClient(String indexName) {
        this.indexName = indexName;
    }

    public AbstractEsDepthClient(ElasticsearchClient client) {
        this.client = client;
    }

    public AbstractEsDepthClient(String indexName, ElasticsearchClient client) {
        this.indexName = indexName;
        this.client = client;
    }

    //是否工作状态
    public boolean isWorking() throws IOException {
        if (client == null) return false;
        return client.ping().value();
    }

    public List<DepthModel> searchPage(final int page,
                                       final int size) throws EsReaderException {
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
    protected List<DepthModel> searchDefaultPage(final int page,
                                              final int size) throws EsReaderException {
        //根据created排序
        SearchRequest searchRequest = makeSearchRequest(page, size);
        return searchFromEs(searchRequest);
    }

    /**
     * 分页查询
     */
    protected List<DepthModel> searchPageWithSearchAfter(final int size) throws EsReaderException {
        //根据created排序
        SearchRequest searchRequest = makeSearchRequestWithSearchAfter(size);
        return searchFromEs(searchRequest);
    }

    public DepthModel searchLast() throws EsReaderException {
        //根据created排序
        SearchRequest searchRequest = makeLastSearchRequest();
        List<DepthModel> dataList = searchFromEs(searchRequest);
        return dataList.get(0);
    }

    public DepthModel searchLastOfToDay(final String type, final String today) throws EsReaderException {
        //根据created排序
        SearchRequest searchRequest = makeLastOfToDaySearchRequest(type, today);
        List<DepthModel> dataList = searchFromEs(searchRequest);
        return (dataList == null || dataList.size() == 0) ? null : dataList.get(0);
    }

    public DepthModel searchLastOfToDay(final String market,
                                        final String type,
                                        final String symbol,
                                        final Date beginDate,
                                        final Date endDate) throws EsReaderException {
        //根据created排序
        SearchRequest searchRequest = makeLastOfToDaySearchRequest(market, type, symbol, beginDate, endDate);
        List<DepthModel> dataList = searchFromEs(searchRequest);
        return (dataList == null || dataList.size() == 0) ? null : dataList.get(0);
    }


    public abstract long count(final String today) throws EsReaderException;

    public abstract long count() throws EsReaderException;

    public abstract void setMarketTime(Date beginTime, Date endTime);

    protected abstract SearchRequest makeSearchRequest(final int page,
                                              final int size);

    protected abstract SearchRequest makeSearchRequestWithSearchAfter(final int size);

    /**
     * @param today   yyyy-mm-dd格式
     * @return
     */
    protected abstract SearchRequest makeLastOfToDaySearchRequest(final String type, final String today);
    protected abstract SearchRequest makeLastOfToDaySearchRequest(final String market,
                                                                  final String type,
                                                                  final String symbol,
                                                                  final Date beginDate,
                                                                  final Date endDate);


    protected abstract SearchRequest makeLastSearchRequest();

    protected List<DepthModel> searchFromEs(final SearchRequest request) throws EsReaderException {
        if (client == null) throw new EsReaderException("elasticsearch连接已断开");
        SearchResponse<DepthModel> response = null;
        try {
            response = client.search(request, DepthModel.class);
        } catch (Exception e) {
            e.printStackTrace();
            throw new EsReaderException("查询行情回放数据异常", e);
        }

        if (response != null && response.hits().hits().size() > 0) {
            List<Hit<DepthModel>> hits = response.hits().hits();
            DepthModel lastDepth = hits.get(hits.size() - 1).source();
            lastId = String.valueOf(lastDepth.getCreated().getTime());
            return hits.stream().map((e) -> e.source()).collect(Collectors.toList());
        }
        return new ArrayList<DepthModel>();
    }

    protected void updateState(final int page) {
        if (page == 1) {
            lastId = "";
        }
    }

    /**
     * 写入单条数据
     * @param dataModel
     * @return
     * @throws IOException
     */
    public boolean pushData(final EsDataModel dataModel) throws EsWriteException {
        if (client == null) throw new EsWriteException("elasticsearch连接已断开");
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.operations(op -> op
                .index(idx -> idx
                        .index(indexName)
                        .document(dataModel.getData())
                        .id(dataModel.getId())));

        try {
            BulkResponse response = client.bulk(builder.build());
            return response.errors();
        } catch (IOException e) {
            throw new EsWriteException("行情数据写入失败", e);
        }
    }

    public void setLastId(String lastId) {
        this.lastId = lastId;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setClient(ElasticsearchClient client) {
        this.client = client;
    }

    protected boolean isEmpty(final String source) {
        return source == null || source.length() == 0;
    }

    protected SearchRequest.Builder searchRequestBuilder() {
        SearchRequest.Builder builder = new SearchRequest.Builder();
        builder.index(indexName);
        return builder;
    }

    public abstract String[] symbol();
    public abstract String[] market();
    public abstract String[] type();
    public abstract String beginTime();
    public abstract String endTime();


}
