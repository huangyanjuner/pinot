package com.linkedin.pinot.core.query.scheduler.fcfs;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.query.scheduler.SchedulerQueryContext;
import com.yammer.metrics.core.MetricsRegistry;
import org.testng.annotations.Test;

import static com.linkedin.pinot.core.query.scheduler.TestHelper.*;
import static org.testng.Assert.*;


public class FCFSGroupTest {
  static final ServerMetrics metrics = new ServerMetrics(new MetricsRegistry());

  @Test
  public void testCompare() {
    FCFSGroup lhs = new FCFSGroup("one");
    FCFSGroup rhs = new FCFSGroup("two");

    // both groups are empty
    assertNull(lhs.peekFirst());
    assertNull(rhs.peekFirst());
    assertEquals(lhs.compareTo(lhs), 0);
    assertEquals(lhs.compareTo(rhs), 0);
    assertEquals(rhs.compareTo(lhs), 0);
    SchedulerQueryContext firstRequest = createQueryRequest("groupOne", metrics);
    firstRequest.getQueryRequest().getTimerContext().setQueryArrivalTimeNs(1000 * 1_000_000L);
    lhs.addLast(firstRequest);

    assertEquals(lhs.compareTo(rhs), 1);
    assertEquals(rhs.compareTo(lhs), -1);
    SchedulerQueryContext secondRequest = createQueryRequest("groupTwo", metrics);
    secondRequest.getQueryRequest().getTimerContext().setQueryArrivalTimeNs(2000 * 1_000_000L);
    rhs.addLast(secondRequest);
    assertEquals(lhs.compareTo(rhs), 1);
    assertEquals(rhs.compareTo(lhs), -1);
    secondRequest.getQueryRequest().getTimerContext().setQueryArrivalTimeNs(1 * 1_000_000L);
    assertEquals(lhs.compareTo(rhs), -1);
    assertEquals(rhs.compareTo(lhs), 1);
  }
}