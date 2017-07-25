package com.linkedin.pinot.core.query.scheduler.resources;

import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class UnboundedResourceManagerTest {

  @Test
  public void testDefault() {
    Configuration config = new PropertiesConfiguration();

    UnboundedResourceManager rm = new UnboundedResourceManager(config);
    assertTrue(rm.getNumQueryRunnerThreads() > 1);
    assertTrue(rm.getNumQueryWorkerThreads() >= 1);
    assertEquals(rm.getTableThreadsHardLimit(), rm.getNumQueryRunnerThreads() + rm.getNumQueryWorkerThreads());
    assertEquals(rm.getTableThreadsSoftLimit(), rm.getNumQueryRunnerThreads() + rm.getNumQueryWorkerThreads());
  }

  @Test
  public void testWithConfig() {
    Configuration config = new PropertiesConfiguration();
    final int workers = 5;
    final int runners = 2;

    config.setProperty(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    config.setProperty(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);

    UnboundedResourceManager rm = new UnboundedResourceManager(config);
    assertEquals(rm.getNumQueryWorkerThreads(), workers);
    assertEquals(rm.getNumQueryRunnerThreads(), runners);
    assertEquals(rm.getTableThreadsHardLimit(), runners + workers);
    assertEquals(rm.getTableThreadsSoftLimit(), runners + workers);

    SchedulerGroupAccountant accountant = mock(SchedulerGroupAccountant.class);
    when(accountant.totalReservedThreads()).thenReturn(3);
    assertTrue(rm.canSchedule(accountant));

    when(accountant.totalReservedThreads()).thenReturn(workers + runners + 2);
    assertFalse(rm.canSchedule(accountant));
  }
}
