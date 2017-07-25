package com.linkedin.pinot.core.query.scheduler.resources;

import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ResourceManagerTest {

  @Test
  public void testCanSchedule() throws Exception {
    ResourceManager rm = getResourceManager(2, 5, 1, 3);

    SchedulerGroupAccountant accountant = mock(SchedulerGroupAccountant.class);
    when(accountant.totalReservedThreads()).thenReturn(3);
    assertFalse(rm.canSchedule(accountant));

    when(accountant.totalReservedThreads()).thenReturn(2);
    assertTrue(rm.canSchedule(accountant));
  }

  private ResourceManager getResourceManager(int runners, int workers, final int softLimit, final int hardLimit) {

    return new ResourceManager(getConfig(runners, workers)) {

      @Override
      public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
        return new QueryExecutorService() {
          @Override
          public void execute(Runnable command) {
            getQueryWorkers().execute(command);
          }
        };
      }

      @Override
      public int getTableThreadsHardLimit() {
        return hardLimit;
      }

      @Override
      public int getTableThreadsSoftLimit() {
        return softLimit;
      }
    };
  }
  private Configuration getConfig(int runners, int workers) {
    Configuration config = new PropertiesConfiguration();
    config.setProperty(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    config.setProperty(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);
    return config;
  }
}
