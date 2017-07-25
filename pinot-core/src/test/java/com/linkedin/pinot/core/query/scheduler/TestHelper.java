package com.linkedin.pinot.core.query.scheduler;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.request.QuerySource;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

  public static ServerQueryRequest createServerQueryRequest(String table, ServerMetrics metrics) {
    InstanceRequest request = new InstanceRequest();
    request.setBrokerId("broker");
    request.setEnableTrace(false);
    request.setRequestId(1);
    request.setSearchSegments(Arrays.asList("segment1", "segment2"));
    BrokerRequest br = new BrokerRequest();
    QuerySource qs = new QuerySource();
    qs.setTableName(table);
    br.setQuerySource(qs);
    request.setQuery(br);
    ServerQueryRequest qr = new ServerQueryRequest(request, metrics);
    qr.getTimerContext().setQueryArrivalTimeNs(System.currentTimeMillis()* 1000 * 1000);
    return qr;
  }

  public static SchedulerQueryContext createQueryRequest(String table, ServerMetrics metrics) {
    return new SchedulerQueryContext(createServerQueryRequest(table, metrics));
  }

}
