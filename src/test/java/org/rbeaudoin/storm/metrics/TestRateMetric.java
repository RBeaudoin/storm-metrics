package org.rbeaudoin.storm.metrics;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Unit tests for the RateMetric class
 */
public class TestRateMetric {

    private final String totalCountKey = "total-count-key";
    private final String partialCountKey = "partial-count-key";
    private final String percentKey = "percent-key";

    @Test(expected=IllegalArgumentException.class)
    public void testNullEmptyKeyException() {
        RateMetric rateMetric = new RateMetric("", null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSameKeyException() {
        RateMetric rateMetric = new RateMetric(totalCountKey, totalCountKey, totalCountKey);
    }

    @Test
    public void testIncrTotal() {
        //Arrange
        RateMetric rateMetric = new RateMetric(totalCountKey,partialCountKey,percentKey);

        //Act
        rateMetric.incrTotal();

        //Assert
        int totalCount = rateMetric.metricValues.get(totalCountKey);
        Assert.assertEquals(1, totalCount);
    }

    @Test
    public void testIncrTotal_multiple_calls() {
        //Arrange
        RateMetric rateMetric = new RateMetric(totalCountKey,partialCountKey,percentKey);

        //Act
        rateMetric.incrTotal();
        rateMetric.incrTotal();
        rateMetric.incrTotal();

        //Assert
        int totalCount = rateMetric.metricValues.get(totalCountKey);
        Assert.assertEquals(3, totalCount);
    }

    @Test
    public void testIncrPartial() {
        //Arrange
        RateMetric rateMetric = new RateMetric(totalCountKey,partialCountKey,percentKey);

        //Act
        rateMetric.incrPartial();

        //Assert
        int partialCount = rateMetric.metricValues.get(partialCountKey);
        Assert.assertEquals(1, partialCount);
    }

    @Test
    public void testIncrPartial_multiple_calls() {
        //Arrange
        RateMetric rateMetric = new RateMetric(totalCountKey,partialCountKey,percentKey);

        //Act
        rateMetric.incrPartial();
        rateMetric.incrPartial();
        rateMetric.incrPartial();
        rateMetric.incrPartial();

        //Assert
        int partialCount = rateMetric.metricValues.get(partialCountKey);
        Assert.assertEquals(4, partialCount);
    }

    @Test
    public void testGetValueAndReset() {
        //Arrange
        RateMetric rateMetric = new RateMetric(totalCountKey,partialCountKey,percentKey);

        //Act
        rateMetric.incrTotal();
        rateMetric.incrTotal();
        rateMetric.incrTotal();
        rateMetric.incrPartial();

        Map<String, Integer> metricValues = (Map<String, Integer>) rateMetric.getValueAndReset();
        int totalCount = metricValues.get(totalCountKey);
        int partialCount = metricValues.get(partialCountKey);
        int percentage = metricValues.get(percentKey);

        //Assert
        Assert.assertEquals(3, totalCount);
        Assert.assertEquals(1, partialCount);
        Assert.assertEquals(33, percentage);
    }

    @Test
    public void testResetMetricValues() {
        //Arrange
        RateMetric rateMetric = new RateMetric("totalRecords","partialRecords","percent");

        //Act
        rateMetric.metricValues.put(rateMetric.totalCountKey, 1);
        rateMetric.metricValues.put(rateMetric.partialCountKey, 1);
        rateMetric.getValueAndReset();

        //Assert
        Assert.assertEquals(2, rateMetric.metricValues.size());
        Assert.assertEquals(0, (int)rateMetric.metricValues.get(rateMetric.totalCountKey));
        Assert.assertEquals(0, (int)rateMetric.metricValues.get(rateMetric.partialCountKey));
    }
}
