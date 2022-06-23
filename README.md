# elasticsearch-java-client
Elasticsearch java client api 使用样例

## maven 依赖
```xml
    <dependencies>
        <dependency>
          <groupId>org.projectlombok</groupId>
          <artifactId>lombok</artifactId>
          <version>1.18.24</version>
        </dependency>
    
        <dependency>
          <groupId>co.elastic.clients</groupId>
          <artifactId>elasticsearch-java</artifactId>
          <version>7.17.4</version>
        </dependency>
    
        <dependency>
          <groupId>com.fasterxml.jackson.core</groupId>
          <artifactId>jackson-core</artifactId>
          <version>2.13.3</version>
        </dependency>
    
        <dependency>
          <groupId>com.fasterxml.jackson.core</groupId>
          <artifactId>jackson-annotations</artifactId>
          <version>2.13.3</version>
        </dependency>
    
        <dependency>
          <groupId>com.fasterxml.jackson.core</groupId>
          <artifactId>jackson-databind</artifactId>
          <version>2.12.3</version>
        </dependency>
    
        <dependency>
          <groupId>jakarta.json</groupId>
          <artifactId>jakarta.json-api</artifactId>
          <version>2.0.1</version>
        </dependency>
    </dependencies>
    
```

## 创建index
### create index
```json
{
	"finesys_depth01": {
		"settings": {
			"index": {
				"creation_date": "1655794673844",
				"max_result_window": "10000",
				"number_of_replicas": "1",
				"number_of_shards": "3",
				"provided_name": "finesys_depth01",
				"routing": {
					"allocation": {
						"include": {
							"_tier_preference": "data_content"
						}
					}
				},
				"uuid": "60Aail9bQnCct4Enj6P5IQ",
				"version": {
					"created": "7170499"
				}
			}
		}
	}
}
```

### create mapping
```json
{
	"properties": {
		"created": {
			"format": "yyyy-MM-dd HH:mm:ss.SSSZ||yyyy-MM-dd||epoch_millis",
			"type": "date"
		},
		"data": {
			"type": "text"
		},
		"market": {
			"type": "keyword"
		},
		"symbol": {
			"type": "keyword"
		},
		"time": {
			"type": "text"
		},
		"type": {
			"type": "keyword"
		}
	}
}
```

## 使用说明
    * EsDepthClient.java 多个字段属性单一条件查询,转换成sql比如: where a='' and b = '' and c='' order by created asc limit size;
    * EsDepthMutiTypeClient.java 多个字段属性中包括多条件查询,转换成sql比如:where a='' and b = '' and (c='0' or c = '1' or c = '2') order by created asc limit size;

### 方法说明
```java
    //es search client
    private ElasticsearchClient client = null;

    //elasticsearch client 对象
    public EsDepthClient(ElasticsearchClient client) {
        this.client = client;
    }

    //传入index名称  查询条件
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

    //根据分页page确定走默认分页查询还是通过searchafter查询
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

``` 

## 怎么使用
```java
public static void main(String args[]) throws IOException {
    //rest client
    RestClient restClient = RestClient.builder(
                     new HttpHost(hostName, port)).build();
    // Create the transport with a Jackson mapper
    ElasticsearchTransport transport = new RestClientTransport(
            restClient, new JacksonJsonpMapper());

    // And create the API client
    ElasticsearchClient client = new ElasticsearchClient(transport);
    
    //初始化一个特定index es client对象
    EsDepthMutiTypeClient depthClient = new EsDepthMutiTypeClient(searchClient.getClient());
    depthClient.setIndexName("finesys_depth01");
    depthClient.setSymbol("ag2204");
    depthClient.setMarket("SH");
    depthClient.setTypes(new String[] { "KLine_30sec", "KLine_5min", "KLine_1hour" });
    depthClient.setBeginTime("2022-06-22 10:58:05.527");
    depthClient.setEndTime("2022-06-22 11:58:05.527");

    int page = 1;
    long count = depthClient.count();
    System.out.println("count is " + depthClient.count());

    while (count <= 0) {
        List<DepthModel> dataList = depthClient.searchPage(page, 100);
        for (DepthModel depth : dataList) {
            System.out.println(depth.getSymbol() + "<----->" + depth.getType() + "<----->" + depth.getTime());
        }

        page++;
        count -= page * 100;
    }

    client.shutdown();
    restClient.close();
}
```
