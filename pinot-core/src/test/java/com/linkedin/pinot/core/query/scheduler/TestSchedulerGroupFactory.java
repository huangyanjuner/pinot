package com.linkedin.pinot.core.query.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.configuration.Configuration;

import static org.testng.Assert.*;


class TestSchedulerGroupFactory implements SchedulerGroupFactory {
  AtomicInteger numCalls = new AtomicInteger(0);
  ConcurrentHashMap<String, TestSchedulerGroup> groupMap = new ConcurrentHashMap<>();
  @Override
  public SchedulerGroup create(Configuration config, String groupName) {
    numCalls.incrementAndGet();
    assertNull(groupMap.get(groupName));
    TestSchedulerGroup group = new TestSchedulerGroup(groupName);
    groupMap.put(groupName, group);
    return group;
  }

  public void reset() {
    numCalls.set(0);
    groupMap.clear();
  }
}
