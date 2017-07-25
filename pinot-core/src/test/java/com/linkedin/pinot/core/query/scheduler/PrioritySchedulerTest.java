package com.linkedin.pinot.core.query.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableFactory;
import com.linkedin.pinot.core.common.datatable.DataTableImplV2;
import com.linkedin.pinot.core.query.scheduler.resources.PolicyBasedResourceManager;
import com.linkedin.pinot.core.query.scheduler.resources.ResourceLimitPolicy;
import com.linkedin.pinot.core.query.scheduler.resources.ResourceManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static com.linkedin.pinot.core.query.scheduler.TestHelper.*;
import static org.testng.Assert.*;


public class PrioritySchedulerTest {
  private static final ServerMetrics metrics = new ServerMetrics(new MetricsRegistry());
  private static boolean useBarrier = false;
  private static CyclicBarrier startupBarrier;
  private static CyclicBarrier validationBarrier;
  private static CountDownLatch numQueries = new CountDownLatch(1);

  @AfterMethod
  public void afterMethod() {
    useBarrier = false;
    startupBarrier = null;
    validationBarrier = null;
    numQueries = new CountDownLatch(1);
  }

  // Tests that there is no "hang" on stop
  @Test
  public void testStartStop() throws InterruptedException {
    TestPriorityScheduler scheduler = TestPriorityScheduler.create();
    scheduler.start();
    Thread.sleep(100);
    scheduler.stop();
  }

  @Test
  public void testOneQuery() throws InterruptedException, ExecutionException, IOException, BrokenBarrierException {
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setProperty(ResourceLimitPolicy.THREADS_PER_QUERY_PCT, 50);
    conf.setProperty(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 40);
    conf.setProperty(ResourceLimitPolicy.TABLE_THREADS_SOFT_LIMIT, 20);
    useBarrier = true;
    startupBarrier = new CyclicBarrier(2);
    validationBarrier = new CyclicBarrier(2);

    TestPriorityScheduler scheduler = TestPriorityScheduler.create(conf);
    int totalPermits = scheduler.getRunningQueriesSemaphore().availablePermits();
    scheduler.start();
    ListenableFuture<byte[]> result = scheduler.submit(
        createServerQueryRequest("1", metrics));
    startupBarrier.await();
    TestSchedulerGroup group = TestPriorityScheduler.groupFactory.groupMap.get("1");
    assertEquals(group.numRunning(), 1);
    assertEquals(group.getThreadsInUse(), 1);
    // this is odd...the runner thread count does not apply towards totalReservedThreads but
    // accounts towards getThreadsInUse
    assertEquals(group.totalReservedThreads(), 2 /* equals numSegments in request*/);
    validationBarrier.await();
    byte[] resultData = result.get();
    DataTable table = DataTableFactory.getDataTable(resultData);
    assertEquals(table.getMetadata().get("table"), "1");
    // verify that accounting is handled right
    assertEquals(group.numPending(), 0);
    assertEquals(group.getThreadsInUse(), 0);
    assertEquals(group.totalReservedThreads(), 0);
    // -1 because we expect that 1 permit is blocked by the scheduler main thread
    assertEquals(scheduler.getRunningQueriesSemaphore().availablePermits(), totalPermits - 1);
    scheduler.stop();
  }

  @Test
  public void testMultiThreaded() throws InterruptedException {
    // add queries from multiple threads and verify that all those are executed
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setProperty(ResourceManager.QUERY_WORKER_CONFIG_KEY, 60);
    conf.setProperty(ResourceManager.QUERY_RUNNER_CONFIG_KEY, 20);
    conf.setProperty(ResourceLimitPolicy.THREADS_PER_QUERY_PCT, 50);
    conf.setProperty(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 60);
    conf.setProperty(ResourceLimitPolicy.TABLE_THREADS_SOFT_LIMIT, 40);
    conf.setProperty(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 10);

    final TestPriorityScheduler scheduler = TestPriorityScheduler.create(conf);
    scheduler.start();
    final Random random = new Random();
    final ConcurrentLinkedQueue<ListenableFuture<byte[]>> results = new ConcurrentLinkedQueue<>();
    final int numThreads = 3;
    final int queriesPerThread = 10;
    numQueries = new CountDownLatch(numThreads * queriesPerThread);

    for (int i = 0; i < numThreads; i++) {
      final int index = i;
      new Thread(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < queriesPerThread; j++) {
            results.add(scheduler.submit(createServerQueryRequest(Integer.toString(index), metrics)));
            Uninterruptibles.sleepUninterruptibly(random.nextInt(100), TimeUnit.MILLISECONDS);
          }
        }
      }).start();
    }
    numQueries.await();
    scheduler.stop();
  }

  @Test
  public void testOutOfCapacityResponse() throws ExecutionException, InterruptedException, IOException {
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setProperty(ResourceLimitPolicy.TABLE_THREADS_HARD_LIMIT, 5);
    conf.setProperty(MultiLevelPriorityQueue.MAX_PENDING_PER_GROUP_KEY, 1);
    TestPriorityScheduler scheduler = TestPriorityScheduler.create(conf);
    scheduler.start();
    List<ListenableFuture<byte[]>> results = new ArrayList<>();
    results.add(scheduler.submit(createServerQueryRequest("1", metrics)));
    TestSchedulerGroup group = TestPriorityScheduler.groupFactory.groupMap.get("1");
    group.addReservedThreads(10);
    group.addLast(createQueryRequest("1", metrics));
    results.add(scheduler.submit(createServerQueryRequest("1", metrics)));
    DataTable dataTable = DataTableFactory.getDataTable(results.get(1).get());
    assertTrue(dataTable.getMetadata().containsKey(
        DataTable.EXCEPTION_METADATA_KEY + QueryException.INTERNAL_ERROR.getErrorCode()));
    scheduler.stop();
  }

  @Test
  public void testSubmitBeforeRunning() throws ExecutionException, InterruptedException, IOException {
    TestPriorityScheduler scheduler = TestPriorityScheduler.create();
    ListenableFuture<byte[]> result = scheduler.submit(
        createServerQueryRequest("1", metrics));
    // start is not called
    DataTable response = DataTableFactory.getDataTable(result.get());
    assertTrue(response.getMetadata().containsKey(
        DataTable.EXCEPTION_METADATA_KEY + QueryException.INTERNAL_ERROR.getErrorCode()));
    assertFalse(response.getMetadata().containsKey("table"));
    scheduler.stop();
  }

  static class TestPriorityScheduler extends PriorityScheduler {
    static TestSchedulerGroupFactory groupFactory;

    public static TestPriorityScheduler create(Configuration conf) {
      ResourceManager rm = new PolicyBasedResourceManager(conf);
      QueryExecutor qe = new TestQueryExecutor();
      groupFactory = new TestSchedulerGroupFactory();
      MultiLevelPriorityQueue queue = new MultiLevelPriorityQueue(conf, rm,
          groupFactory, new TableBasedGroupMapper());
      return new TestPriorityScheduler(rm, qe, queue, metrics);
    }

    public static TestPriorityScheduler create() {
      PropertiesConfiguration conf = new PropertiesConfiguration();
      return create(conf);
    }

    // store locally for easy access
    public TestPriorityScheduler(@Nonnull ResourceManager resourceManager, @Nonnull QueryExecutor queryExecutor,
        @Nonnull SchedulerPriorityQueue queue, @Nonnull ServerMetrics metrics) {
      super(resourceManager, queryExecutor, queue, metrics);
    }

    ResourceManager getResourceManager() {
      return this.resourceManager;
    }

    @Override
    public String name() {
      return "TestScheduler";
    }

    public Semaphore getRunningQueriesSemaphore() {
      return runningQueriesSemaphore;
    }

  }

  static class TestQueryExecutor implements QueryExecutor {

    @Override
    public void init(Configuration queryExecutorConfig, DataManager dataManager, ServerMetrics serverMetrics)
        throws ConfigurationException {

    }

    @Override
    public void start() {

    }

    @Override
    public DataTable processQuery(ServerQueryRequest queryRequest,
        ExecutorService executorService) {
      if (useBarrier) {
        try {
          startupBarrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      DataTableImplV2 result = new DataTableImplV2();
      result.getMetadata().put("table", queryRequest.getTableName());
      if (useBarrier) {
        try {
          validationBarrier.await();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      numQueries.countDown();
      return result;
    }

    @Override
    public void shutDown() {

    }

    @Override
    public boolean isStarted() {
      return true;
    }

    @Override
    public void updateResourceTimeOutInMs(String resource, long timeOutMs) {

    }
  }
}