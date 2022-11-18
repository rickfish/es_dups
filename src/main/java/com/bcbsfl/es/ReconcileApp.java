package com.bcbsfl.es;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.net.ssl.SSLContext;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.bcbsfl.mail.EmailSender;

public class ReconcileApp {
    protected static final String TASK_DOC_TYPE = "task";
    protected static final String WORKFLOW_DOC_TYPE = "workflow";
    protected static final int SLEEP_SECS_BEFORE_NEXT_QUERY = 5;
    protected static final int MAX_DUPS = 1000;
    private static final TimeZone GMT = TimeZone.getTimeZone("GMT");
    protected static final SimpleDateFormat ES_TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    protected RestHighLevelClient elasticSearchClient;
    protected String indexName = null;
    protected String env = null;
    protected Map<String, List<String>> info = new HashMap<String, List<String>>();
    
    protected String doctype = null;
    protected boolean updateElasticSearch = false;
    protected boolean logEachRecord = false;
    protected EmailSender emailSender = new EmailSender();
    protected static final String[] digits = {
    	"0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"	
    };

    protected Map<String, List<String>> firstPageOfDups = new HashMap<String, List<String>>();
    protected boolean addToFirstPageOfDups = true;
	protected int totalDups = 0;
	protected int shardSize = 0;

	static {
        ES_TIMESTAMP_FORMAT.setTimeZone(GMT);
    }

    public ReconcileApp(String doctype) {
		try {
            this.env = Utils.getProperty("env");
            this.doctype = doctype;
	       	this.elasticSearchClient = getRestHighLevelClient();
	        this.indexName = "conductor_" + this.env + "_" + doctype;
	        this.updateElasticSearch = Utils.getBooleanProperty("update.elastic.search");
	        this.logEachRecord = Utils.getBooleanProperty("log.each.record");
	        this.firstPageOfDups = new HashMap<String, List<String>>();
	        this.addToFirstPageOfDups = true;
	    	this.totalDups = 0;
	    	this.shardSize = Utils.getIntProperty("shard.size");
	    	if(this.shardSize == 0) {
	    		this.shardSize = 10000;
	        	System.out.println("[" + new Date().toString() + "] Using a shard size of the default of 10000");
	    	} else { 
	        	System.out.println("[" + new Date().toString() + "] Using a shard size of " + this.shardSize);
	    	}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
    protected RestHighLevelClient getRestHighLevelClient() throws Exception {

		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(Utils.getProperty("es.user"), Utils.getProperty("es.password")));

		File file = new File("mykeystore");
		
		FileUtils.copyInputStreamToFile(getClass().getClassLoader().getResourceAsStream("my_keystore.jks"), file);
		
		final SSLContext sslContext = SSLContexts.custom()
				.loadTrustMaterial(file, "admin123".toCharArray(), new TrustSelfSignedStrategy()).build();

		RestClientBuilder clientBuilder = null;
		if(null == Utils.getProperty("es.url2") || "".equals(Utils.getProperty("es.url2"))) {
			clientBuilder = RestClient.builder(new HttpHost(Utils.getProperty("es.url1"), 9200, "https"));
		} else {
			clientBuilder = RestClient
				.builder(new HttpHost(Utils.getProperty("es.url1"), 9200, "https"),
					new HttpHost(Utils.getProperty("es.url2"), 9200, "https"));
		}
		clientBuilder
			.setRequestConfigCallback(requestConfigBuilder -> 
				requestConfigBuilder.setConnectTimeout(150000).setSocketTimeout(150000))
			.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLContext(sslContext));

		RestHighLevelClient higLevelRestClient = new RestHighLevelClient(clientBuilder);

		return higLevelRestClient;
	}
    
    protected void reconcile() throws Exception {
    	String regexp = "";
    	int total = 0;
		for(String firstDigit : digits) {
			for(String secondDigit : digits) {
				for(String thirdDigit : digits) {
					regexp = firstDigit + secondDigit + thirdDigit + ".*";
					reconcile(regexp);
					total++;
		    	}
	    	}
    	}
    	if(totalDups > 0) {
    		this.emailSender.sendDuplicatesEmail(this.doctype, firstPageOfDups, totalDups, MAX_DUPS);
    	}
    	System.out.println("[" + new Date().toString() + "] done");
    }
    
    protected void reconcile(String regexp) throws Exception {
    	boolean keepGoing = true;
		while(keepGoing) {
        	List<String> dups = getDuplicates(regexp);
        	for(String id : dups) {
        		try {
        			List<String> events = new ArrayList<String>();
        			correctElasticSearch(id, events);
        			this.info.put(id,  events);
        			if(addToFirstPageOfDups) {
        				firstPageOfDups.put(id,  events);
        				if(firstPageOfDups.size() >= MAX_DUPS) {
        					addToFirstPageOfDups = false;
        				}
        			}
        		} catch(Exception e) {
        			e.printStackTrace();
        		}
        	}
        	totalDups += dups.size();
        	if(!this.logEachRecord) {
    	    	System.out.println("***************************** DUPLICATES ****************************");
    	    	this.info.keySet().forEach(id -> {
    	    		System.out.println(id);
    	    		this.info.get(id).forEach(event -> {
    	    			System.out.println("    " + event);
    	    		});
    	    	});
    	    	System.out.println("*************************** END DUPLICATES **************************");
        	}
        	if(dups == null || dups.size() < MAX_DUPS) {
        		keepGoing = false;
        	} else {
        		System.out.println("***** Found maximum of " + MAX_DUPS + " duplicate " + this.doctype + "s and fixed them. Getting more after sleeping for " + SLEEP_SECS_BEFORE_NEXT_QUERY + " seconds ...");
        		Thread.sleep(SLEEP_SECS_BEFORE_NEXT_QUERY * 1000);
        	    info = new HashMap<String, List<String>>();
        	}
		}
    }
    
    protected List<String> getDuplicates(String regexp) throws Exception {
    	System.out.print("[" + new Date().toString() + "] " + this.doctype + "s matching '" + regexp + "'...");
    	long start = System.currentTimeMillis();
    	List<String> dups = new ArrayList<String>();
       	String docIdField = WORKFLOW_DOC_TYPE.equals(this.doctype) ? "workflowId" : "taskId";
    	BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
    	QueryBuilder qb = QueryBuilders.regexpQuery(docIdField, regexp);
    	queryBuilder.must(qb);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(0);

        AggregationBuilder dupIdAggregation = AggregationBuilders.terms("dupIds").field(docIdField).size(MAX_DUPS).shardSize(this.shardSize).minDocCount(2);
   		searchSourceBuilder.aggregation(dupIdAggregation);
        
        // Generate the actual request to send to ES.
        SearchRequest searchRequest = new SearchRequest(this.indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        ParsedStringTerms terms = response.getAggregations().get("dupIds");
        if(terms.getBuckets() != null && terms.getBuckets().size() > 0) {
        	terms.getBuckets().forEach(bucket -> {
        		if(bucket.getDocCount() > 0) {
       				dups.add(bucket.getKeyAsString());
        		}
        	});
        }
        System.out.println(" took " + (System.currentTimeMillis() - start) + " ms and found " + dups.size() + " duplicates with doc_count_error_upper_bound of " + terms.getDocCountError() + ".");
        return dups;
    }

    private void correctElasticSearch(String id, List<String> events) throws Exception {
       	String docIdField = WORKFLOW_DOC_TYPE.equals(this.doctype) ? "workflowId" : "taskId";
    	BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
    	QueryBuilder qb = QueryBuilders.termQuery(docIdField, id);
    	queryBuilder.must(qb);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(20);

        // Generate the actual request to send to ES.
        SearchRequest searchRequest = new SearchRequest(this.indexName);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = this.elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();
        if(hits.getHits().length < 2) {
           	events.add("There were only " + hits.getHits().length + " documents found but there was supposed to be at least 2!");
        } else {
            Map<String, String> indexesToDeleteFrom = new HashMap<String, String>();
            LatestDocumentInfo latestDocumentInfo = new LatestDocumentInfo();
            findLatestDocument(hits, latestDocumentInfo, "@timestamp");
            if(latestDocumentInfo.latestSourceAsMap == null) {
                findLatestDocument(hits, latestDocumentInfo, "updateTime");
            }
            if(latestDocumentInfo.latestSourceAsMap == null) {
                findLatestDocument(hits, latestDocumentInfo, "startTime");
            }
            if(latestDocumentInfo.indexWithMatchingDoc == null) {
            	events.add("DID NOT FIND ANY LATEST ELASTIC SEARCH DOCUMENT?!?!?!");
	        }
            Map<String, Object> sourceAsMap = null;
            String status = null;
            String type = null;
            String timestamp = null;
            for(SearchHit hit : hits.getHits()) {
            	sourceAsMap = hit.getSourceAsMap();
            	status = (String) sourceAsMap.get("status");
            	type = (String) sourceAsMap.get(this.doctype == TASK_DOC_TYPE ? "taskType" : "workflowType");
            	timestamp = (String) sourceAsMap.get("@timestamp");
            	if(hit.getIndex().equals(latestDocumentInfo.indexWithMatchingDoc)) {
            		if(!hit.getId().equals(latestDocumentInfo.latestDocId)) {
            			events.add("DELETING from " + hit.getIndex() + ", type: " + type + ", status: " + status + ", timestamp: " +  timestamp + " - The _id of the document was '" + hit.getId() + "', not sure why");
                		indexesToDeleteFrom.put(hit.getId(), hit.getIndex());
            		} else {
            			events.add("KEEPING in " + hit.getIndex() + ", type: " + type + ", status: " + status + ", timestamp: " +  timestamp);
            		}
            	} else {
            		if(!hit.getId().equals(latestDocumentInfo.latestDocId)) {
            			events.add("DELETING from " + hit.getIndex() + ", type: " + type + ", status: " + status + ", timestamp: " +  timestamp + " - The _id of the document was '" + hit.getId() + "', not sure why");
            		} else {
            			events.add("DELETING from " + hit.getIndex() + ", type: " + type + ", status: " + status + ", timestamp: " +  timestamp);
            		}
            		indexesToDeleteFrom.put(hit.getId(), hit.getIndex());
            	}
        	}
            if(indexesToDeleteFrom.size() > 0) {
	            indexesToDeleteFrom.keySet().forEach(docid -> {
					removeObjectFromIndex(indexesToDeleteFrom.get(docid), docid);
	            });
            }
        }
        if(this.logEachRecord) {
        	System.out.println("  " + id);
    		events.forEach(event -> {
    			System.out.println("      " + event);
    		});
        }
	}

	private void findLatestDocument(SearchHits hits, LatestDocumentInfo latestDocumentInfo, String timestampAttr) {
        Map<String, Object> sourceAsMap = null;
        for(SearchHit hit : hits.getHits()) {
            sourceAsMap = hit.getSourceAsMap();
            Object o = sourceAsMap.get(timestampAttr);
            if(o != null) {
            	try {
        			Date date = ES_TIMESTAMP_FORMAT.parse((String)o);
        			if(date.getTime() > latestDocumentInfo.latestTimestamp) {
        				latestDocumentInfo.latestTimestamp = date.getTime();
        				latestDocumentInfo.latestSourceAsMap = sourceAsMap;
        				latestDocumentInfo.indexWithMatchingDoc = hit.getIndex();
        				latestDocumentInfo.latestDocId = hit.getId();
        			}
        		} catch(Exception e) {
        		}
            }
    	}
	}
	

    private void removeObjectFromIndex(final String index, String docId) {
    	if(!this.updateElasticSearch) {
    		return;
    	}
		ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
		    @Override
		    public void onResponse(DeleteResponse deleteResponse) {
		    }

		    @Override
		    public void onFailure(Exception e) {
	        	System.err.println("Failed to remove workflow '" + docId + "' from index '" + index + "'");
		    }
		};
        try {
    		DeleteRequest request = new DeleteRequest(index, docId);
    		elasticSearchClient.deleteAsync(request, RequestOptions.DEFAULT, listener);    		
        } catch (Exception e) {
        	System.err.println("Failed to remove workflow '" + docId + "' from index '" + index + "'");
            e.printStackTrace();
        }
	}

    private class LatestDocumentInfo {
    	public long latestTimestamp = 0;
        public String indexWithMatchingDoc = null;
        public Map<String, Object> latestSourceAsMap = null;
		public String latestDocId = null;
	}
}
