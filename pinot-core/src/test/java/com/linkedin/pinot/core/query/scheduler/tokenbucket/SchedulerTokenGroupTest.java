/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.scheduler.tokenbucket;

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SchedulerTokenGroupTest {

  int timeMillis = 100;
  class TestTokenGroup extends SchedulerTokenGroup {
    static final int numTokensPerMs = 100;
    static final int tokenLifetimeMs = 100;
    TestTokenGroup() {
      super("testGroup", numTokensPerMs, tokenLifetimeMs);
    }

    @Override
    public long currentTimeMillis() {
      return timeMillis;
    }
  }
  @Test
  public void testIncrementThreads() throws Exception {
    // set test time first
    timeMillis = 100;
    TestTokenGroup group = new TestTokenGroup();

    int availableTokens = group.getAvailableTokens();
    // verify token count is correctly set
    assertEquals(availableTokens, TestTokenGroup.numTokensPerMs * TestTokenGroup.tokenLifetimeMs);

    // no threads in use...incrementing time has no effect
    timeMillis += 2 * TestTokenGroup.tokenLifetimeMs;
    availableTokens = group.getAvailableTokens();
    int startTime = timeMillis;
    assertEquals(availableTokens, TestTokenGroup.numTokensPerMs * TestTokenGroup.tokenLifetimeMs);

    int nThreads = 1;
    incrementThreads(group, nThreads);
    assertEquals(group.getThreadsInUse(), nThreads);
    assertEquals(group.getAvailableTokens(), availableTokens);

    // advance time
    int timeIncrement = 20;
    timeMillis += timeIncrement;
    group.decrementThreads();
    assertEquals(group.getThreadsInUse(), 0);
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);


    // more threads
    availableTokens = group.getAvailableTokens();
    nThreads = 5;
    incrementThreads(group, nThreads);
    assertEquals(group.getThreadsInUse(), nThreads);
    // advance time now
    timeMillis += timeIncrement;
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);

    // simple getAvailableTokens() updates tokens and reservedThreads has no effect
    group.addReservedThreads(2 * nThreads);
    availableTokens = group.getAvailableTokens();
    timeIncrement = 10;
    timeMillis += timeIncrement;
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);
    availableTokens = group.getAvailableTokens();

    // decrement some threads
    decrementThreads(group, 2);
    nThreads -= 2;
    timeMillis += timeIncrement;
    assertEquals(group.getAvailableTokens(), availableTokens - timeIncrement * nThreads);

    // 3 threads still in use. Advance time beyond time quantum
    availableTokens = group.getAvailableTokens();
    int pendingTimeInQuantum = startTime + TestTokenGroup.tokenLifetimeMs - timeMillis;
    timeMillis = startTime + TestTokenGroup.tokenLifetimeMs + timeIncrement;
    int timeAdvance = pendingTimeInQuantum + timeIncrement;
    // these are "roughly" the tokens in use since we apply decay. So we don't test for exact value
    int expectedTokens = TestTokenGroup.numTokensPerMs * TestTokenGroup.tokenLifetimeMs - timeAdvance * nThreads;
    assertTrue(group.getAvailableTokens() < expectedTokens);
    availableTokens = group.getAvailableTokens();

    // increment by multiple quantums
    timeMillis = startTime + 3 * TestTokenGroup.tokenLifetimeMs + timeIncrement;
    expectedTokens = TestTokenGroup.numTokensPerMs * TestTokenGroup.tokenLifetimeMs - timeIncrement * nThreads;
    assertTrue(group.getAvailableTokens() < expectedTokens );
  }

  @Test
  public void testStartStopQuery() {
    timeMillis = 100;
    TestTokenGroup group = new TestTokenGroup();
    assertEquals(group.numRunning(), 0);
    assertEquals(group.numPending(), 0);
    assertEquals(group.getThreadsInUse(), 0);
    group.startQuery();
    assertEquals(group.numRunning(), 1);
    assertEquals(group.getThreadsInUse(), 1);

    group.endQuery();
    assertEquals(group.numRunning(), 0);
    assertEquals(group.getThreadsInUse(), 0);
  }

  private void incrementThreads(SchedulerTokenGroup group, int nThreads) {
    for (int i = 0; i < nThreads; i++) {
      group.incrementThreads();
    }
  }

  private void decrementThreads(SchedulerTokenGroup group, int nThreads) {
    for (int i = 0; i < nThreads; i++) {
      group.decrementThreads();
    }
  }
}
