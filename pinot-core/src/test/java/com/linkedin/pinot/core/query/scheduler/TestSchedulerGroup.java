package com.linkedin.pinot.core.query.scheduler;

import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nonnull;


class TestSchedulerGroup extends AbstractSchedulerGroup {

  TestSchedulerGroup(@Nonnull String name) {
    super(name);
  }

  private ConcurrentLinkedQueue<SchedulerQueryContext> getQueue() {
    return pendingQueries;
  }

  @Override
  public int compareTo(SchedulerGroupAccountant o) {
    int lhs = Integer.parseInt(name);
    int rhs = Integer.parseInt(((TestSchedulerGroup) o).name);
    if (lhs < rhs) {
      return 1;
    } else if (lhs > rhs) {
      return -1;
    }
    return 0;
  }
}
