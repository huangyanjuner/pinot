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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.response.broker.GroupByResult;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang3.tuple.ImmutablePair;


/**
 * The <code>AggregationGroupByTrimmingService</code> class provides trimming service for aggregation group-by query.
 */
public class AggregationGroupByTrimmingService {
  public static final String GROUP_KEY_DELIMITER = "\t";

  private final AggregationFunction[] _aggregationFunctions;
  private final int _numAggregationFunctions;
  private final boolean[] _minOrders;

  private final int _groupByTopN;
  // To keep the precision, _trimSize is the larger of (_groupByTopN * 5) or 5000.
  private final int _trimSize;
  // To trigger the trimming, number of groups should be larger than _trimThreshold which is (_trimSize * 4).
  private final int _trimThreshold;

  public AggregationGroupByTrimmingService(@Nonnull AggregationFunction[] aggregationFunctions, int groupByTopN) {
    Preconditions.checkArgument(groupByTopN > 0);

    _aggregationFunctions = aggregationFunctions;
    _numAggregationFunctions = aggregationFunctions.length;
    _minOrders = new boolean[_numAggregationFunctions];
    for (int i = 0; i < _numAggregationFunctions; i++) {
      String aggregationFunctionName = aggregationFunctions[i].getName();
      if (aggregationFunctionName.equals(AggregationFunctionFactory.AggregationFunctionType.MIN.getName())
          || aggregationFunctionName.equals(AggregationFunctionFactory.AggregationFunctionType.MINMV.getName())) {
        _minOrders[i] = true;
      }
    }
    _groupByTopN = groupByTopN;
    _trimSize = Math.max(_groupByTopN * 5, 5000);
    _trimThreshold = _trimSize * 4;
  }

  /**
   * Given a map from group key to the intermediate results for multiple aggregation functions, trim the results to
   * desired size and put them into a list of maps from group key to intermediate result for each aggregation function.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public List<Map<String, Object>> trimIntermediateResultsMap(@Nonnull Map<String, Object[]> intermediateResultsMap) {
    List<Map<String, Object>> trimmedResults = new ArrayList<>(_numAggregationFunctions);
    for (int i = 0; i < _numAggregationFunctions; i++) {
      trimmedResults.add(new HashMap<String, Object>());
    }

    if (intermediateResultsMap.isEmpty()) {
      return trimmedResults;
    }

    // Trim the result only if the size of intermediateResultsMap is larger than the threshold
    if (intermediateResultsMap.size() > _trimThreshold) {
      // Construct the sorters
      // For comparable intermediate result, use PriorityQueue as the sorter
      // For incomparable intermediate result, use TreeMapBasedSorter as the sorter
      Object[] sorters = new Object[_numAggregationFunctions];
      for (int i = 0; i < _numAggregationFunctions; i++) {
        if (_aggregationFunctions[i].isIntermediateResultComparable()) {
          if (_minOrders[i]) {
            sorters[i] = new PriorityQueue<GroupKeyResultPair>(_trimSize, Collections.reverseOrder());
          } else {
            sorters[i] = new PriorityQueue<GroupKeyResultPair>(_trimSize, new ComparableComparator());
          }
        } else {
          // NOTE: reverse the comparator so that keys are ordered in descending order
          if (_minOrders[i]) {
            sorters[i] = new TreeMapBasedSorter(_trimSize, new ComparableComparator());
          } else {
            sorters[i] = new TreeMapBasedSorter(_trimSize, Collections.reverseOrder());
          }
        }
      }

      // Fill results into the sorting structure
      for (Map.Entry<String, Object[]> entry : intermediateResultsMap.entrySet()) {
        String groupKey = entry.getKey();
        Object[] intermediateResults = entry.getValue();
        for (int i = 0; i < _numAggregationFunctions; i++) {
          Object intermediateResult = intermediateResults[i];
          if (_aggregationFunctions[i].isIntermediateResultComparable()) {
            GroupKeyResultPair value = new GroupKeyResultPair(groupKey, (Comparable) intermediateResult);
            addToPriorityQueue((PriorityQueue<GroupKeyResultPair>) sorters[i], value, _trimSize);
          } else {
            Comparable finalResult = _aggregationFunctions[i].extractFinalResult(intermediateResult);
            ImmutablePair<String, Object> value = new ImmutablePair<>(groupKey, intermediateResult);
            ((TreeMapBasedSorter) sorters[i]).addToTreeMap(finalResult, value);
          }
        }
      }

      // Fill trimmed results into the maps.
      for (int i = 0; i < _numAggregationFunctions; i++) {
        Map<String, Object> trimmedResult = trimmedResults.get(i);
        if (_aggregationFunctions[i].isIntermediateResultComparable()) {
          PriorityQueue<GroupKeyResultPair> priorityQueue = (PriorityQueue<GroupKeyResultPair>) sorters[i];
          while (!priorityQueue.isEmpty()) {
            GroupKeyResultPair groupKeyResultPair = priorityQueue.poll();
            trimmedResult.put(groupKeyResultPair._groupKey, groupKeyResultPair._result);
          }
        } else {
          TreeMap<Comparable, List<ImmutablePair<String, Object>>> treeMap = ((TreeMapBasedSorter) sorters[i])._treeMap;

          // Track the number of results added because there could be more then trim size values inside the map
          int numResultsAdded = 0;
          for (List<ImmutablePair<String, Object>> groupKeyResultPairs : treeMap.values()) {
            for (ImmutablePair<String, Object> groupKeyResultPair : groupKeyResultPairs) {
              if (numResultsAdded != _trimSize) {
                trimmedResult.put(groupKeyResultPair.getLeft(), groupKeyResultPair.getRight());
                numResultsAdded++;
              } else {
                // This will end the outer loop because there is no extra values inside the map
                break;
              }
            }
          }
        }
      }
    } else {
      // Simply put results from intermediateResultsMap into trimmedResults
      for (Map.Entry<String, Object[]> entry : intermediateResultsMap.entrySet()) {
        String groupKey = entry.getKey();
        Object[] intermediateResults = entry.getValue();
        for (int i = 0; i < _numAggregationFunctions; i++) {
          trimmedResults.get(i).put(groupKey, intermediateResults[i]);
        }
      }
    }

    return trimmedResults;
  }

  /**
   * Given an array of maps from group key to final result for each aggregation function, trim the results to topN size.
   */
  @SuppressWarnings("unchecked")
  @Nonnull
  public List<GroupByResult>[] trimFinalResults(@Nonnull Map<String, Comparable>[] finalResultMaps) {
    List<GroupByResult>[] trimmedResults = new List[_numAggregationFunctions];

    for (int i = 0; i < _numAggregationFunctions; i++) {
      LinkedList<GroupByResult> groupByResults = new LinkedList<>();
      trimmedResults[i] = groupByResults;
      Map<String, Comparable> finalResultMap = finalResultMaps[i];
      if (finalResultMap.isEmpty()) {
        continue;
      }

      // Construct the priority queue
      PriorityQueue<GroupKeyResultPair> priorityQueue;
      if (_minOrders[i]) {
        priorityQueue = new PriorityQueue<>(_groupByTopN, Collections.reverseOrder());
      } else {
        priorityQueue = new PriorityQueue<>(_groupByTopN, new ComparableComparator());
      }

      // Fill results into the priority queue
      for (Map.Entry<String, Comparable> entry : finalResultMap.entrySet()) {
        String groupKey = entry.getKey();
        Comparable finalResult = entry.getValue();

        GroupKeyResultPair newValue = new GroupKeyResultPair(groupKey, finalResult);
        addToPriorityQueue(priorityQueue, newValue, _groupByTopN);
      }

      // Fill trimmed results into the list
      while (!priorityQueue.isEmpty()) {
        GroupKeyResultPair groupKeyResultPair = priorityQueue.poll();
        GroupByResult groupByResult = new GroupByResult();
        // Set limit to -1 to prevent removing trailing empty strings
        String[] groupKeys = groupKeyResultPair._groupKey.split(GROUP_KEY_DELIMITER, -1);
        groupByResult.setGroup(Arrays.asList(groupKeys));
        groupByResult.setValue(AggregationFunctionUtils.formatValue(groupKeyResultPair._result));
        groupByResults.addFirst(groupByResult);
      }
    }

    return trimmedResults;
  }

  /**
   * Helper method to add a value into priority queue:
   * <ul>
   *   <li>
   *     If the queue size is less than maxQueueSize, simply add the value into the priority queue.
   *   </li>
   *   <li>
   *     If the queue size is equal to maxQueueSize, compare the given value against the min value of priority queue. If
   *     the new value is greater than the min value, remove the min value and insert the new value to keep the size of
   *     the queue bounded.
   *   </li>
   * </ul>
   *
   * @param priorityQueue Priority queue
   * @param value Value to be inserted
   * @param maxQueueSize Max allowed queue size
   */
  private static void addToPriorityQueue(PriorityQueue<GroupKeyResultPair> priorityQueue, GroupKeyResultPair value,
      int maxQueueSize) {
    if (priorityQueue.size() == maxQueueSize) {
      GroupKeyResultPair minValue = priorityQueue.peek();
      if (priorityQueue.comparator().compare(value, minValue) > 0) {
        priorityQueue.poll();
        priorityQueue.add(value);
      }
    } else {
      priorityQueue.add(value);
    }
  }

  private static class GroupKeyResultPair implements Comparable<GroupKeyResultPair> {
    private String _groupKey;
    private Comparable _result;

    public GroupKeyResultPair(@Nonnull String groupKey, @Nonnull Comparable result) {
      _groupKey = groupKey;
      _result = result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(@Nonnull GroupKeyResultPair o) {
      return _result.compareTo(o._result);
    }
  }

  /**
   * Helper class based on tree map to sort on given key-value pairs:
   * <ul>
   *   <li>
   *     The value of the map is a list of values that inserted with the same key.
   *   </li>
   *   <li>
   *     If the number values added is greater or equal to trim size, compare the given key against the min key of tree
   *     map. If the new key is greater than the min key, add the value into the map.
   *   </li>
   *   <li>
   *     When possible, remove the min key-values pair from map when enough values added.
   *   </li>
   * </ul>
   */
  private static class TreeMapBasedSorter {
    private final int _trimSize;
    private final Comparator<? super Comparable> _comparator;
    private final TreeMap<Comparable, List<ImmutablePair<String, Object>>> _treeMap;
    private int _numValuesAdded;

    public TreeMapBasedSorter(int trimSize, Comparator<? super Comparable> comparator) {
      _trimSize = trimSize;
      _comparator = comparator;
      _treeMap = new TreeMap<>(comparator);
    }

    public void addToTreeMap(Comparable key, ImmutablePair<String, Object> value) {
      List<ImmutablePair<String, Object>> values = _treeMap.get(key);
      if (_numValuesAdded >= _trimSize) {
        // Check whether the value should be added
        Map.Entry<Comparable, List<ImmutablePair<String, Object>>> lastEntry = _treeMap.lastEntry();
        Comparable minKey = lastEntry.getKey();
        if (_comparator.compare(key, minKey) < 0) {
          // Add the value into the list of values
          if (values == null) {
            values = new ArrayList<>();
            _treeMap.put(key, values);
          }
          values.add(value);
          _numValuesAdded++;

          // Check if the last key can be removed
          if (lastEntry.getValue().size() + _trimSize == _numValuesAdded) {
            _treeMap.remove(minKey);
          }
        }
      } else {
        // Value should be added
        if (values == null) {
          values = new ArrayList<>();
          _treeMap.put(key, values);
        }
        values.add(value);
        _numValuesAdded++;
      }
    }
  }
}
