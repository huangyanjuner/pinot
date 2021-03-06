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
package com.linkedin.pinot.transport.metrics;

import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;


public interface PoolStatsProvider<T extends Sampling & Summarizable> {

  /**
   * Get a snapshot of pool statistics. The specific statistics are described in
   * {@link PoolStats}. Calling getStats will reset any 'latched' statistics.
   *
   * @return An {@link PoolStats} object representing the current pool
   * statistics.
   */
  PoolStats<T> getStats();
}
