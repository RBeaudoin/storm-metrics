package org.rbeaudoin.storm.metrics;


import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of IMetrics used to track counts and percentages for topology events
 */
public class RateMetric implements IMetric {
    Map<String, Integer> metricValues = new HashMap<>();

    final String totalCountKey;
    final String partialCountKey;
    final String percentKey;

    public RateMetric(String totalCountKey, String partialCountKey,
                      String percentKey) {

        if(totalCountKey == null || totalCountKey.isEmpty()) {
            throw new IllegalArgumentException("totalCount key must all be non-null and non-empty");
        } else if (partialCountKey == null || partialCountKey.isEmpty()) {
            throw new IllegalArgumentException("partialCount key must all be non-null and non-empty");
        } else if (percentKey == null || percentKey.isEmpty()) {
            throw new IllegalArgumentException("percentage key must all be non-null and non-empty");
        }

        // Keys for each value must be unique
        if(totalCountKey.equals(partialCountKey) || totalCountKey.equals(percentKey)
                || partialCountKey.equals(percentKey)) {
            throw new IllegalArgumentException("Keys must be different for totalCount, partialCount, and percent");
        }

        this.totalCountKey = totalCountKey;
        this.partialCountKey = partialCountKey;
        this.percentKey = percentKey;

        resetMetricValues();
    }

    public void incrTotal() {
        this.metricValues.put(this.totalCountKey, this.metricValues.get(this.totalCountKey) + 1);
    }

    public void incrPartial() {
        this.metricValues.put(this.partialCountKey, this.metricValues.get(this.partialCountKey) + 1);
    }

    @Override
    public Object getValueAndReset() {
        Map<String, Integer> currentMetricValues = new HashMap<>();

        double totalCount = this.metricValues.get(this.totalCountKey);
        double partialCount = this.metricValues.get(this.partialCountKey);

        int rate = (int)(( partialCount / totalCount ) * 100);
        this.metricValues.put(this.percentKey, rate);

        for(Map.Entry<String, Integer> entry: metricValues.entrySet()) {
            currentMetricValues.put(entry.getKey(), entry.getValue());
        }

        resetMetricValues();

        return currentMetricValues;
    }

    private void resetMetricValues() {
        this.metricValues.clear();

        metricValues.put(this.totalCountKey, 0);
        metricValues.put(this.partialCountKey, 0);
    }
}
