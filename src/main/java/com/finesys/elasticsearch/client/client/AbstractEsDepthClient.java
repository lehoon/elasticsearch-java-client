package com.finesys.elasticsearch.client.client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.finesys.elasticsearch.client.domain.DepthModel;
import com.finesys.elasticsearch.client.domain.EsDataModel;

import java.io.IOException;
import java.util.ArrayList;
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
    protected List<DepthModel> searchDefaultPage(final int page,
                                              final int size) throws IOException {
        //根据created排序
        SearchRequest searchRequest = makeSearchRequest(page, size);
        return searchFromEs(searchRequest);
    }

    /**
     * 分页查询
     */
    protected List<DepthModel> searchPageWithSearchAfter(final int size) throws IOException {
        //根据created排序
        SearchRequest searchRequest = makeSearchRequestWithSearchAfter(size);
        return searchFromEs(searchRequest);
    }

    public abstract long count() throws IOException;

    protected abstract SearchRequest makeSearchRequest(final int page,
                                              final int size);

    protected abstract SearchRequest makeSearchRequestWithSearchAfter(final int size);

    protected List<DepthModel> searchFromEs(final SearchRequest request) throws IOException {
        SearchResponse<DepthModel> response = client.search(request, DepthModel.class);
        if (response.hits().hits().size() > 0) {
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

    public void setLastId(String lastId) {
        this.lastId = lastId;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setClient(ElasticsearchClient client) {
        this.client = client;
    }
}
