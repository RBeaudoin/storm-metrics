package org.rbeaudoin.storm.metrics;

import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.*;

import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Unit tests for the KafkaMetricsConsumer class
 */
public class TestKafkaMetricsConsumer {

    @Test
    public void testgetKafkaProducerProps() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();

        String brokerKey = "metrics.consumer.kafka.brokers";
        String brokers = "test.somewhere.com:9092,test.somewhere.com:9092";

        Map<String, String> metricsConsumerConfig = new HashMap<>();
        metricsConsumerConfig.put(brokerKey, brokers);

        //Act
        ProducerConfig props = kafkaMetricsConsumer.getKafkaProducerProps(metricsConsumerConfig);

        //Assert
        Assert.assertEquals(true, props.props().containsKey("metadata.broker.list"));
        Assert.assertEquals(brokers, props.props().getString("metadata.broker.list"));

        Assert.assertEquals(true, props.props().containsKey("request.required.acks"));
        Assert.assertEquals(1, props.props().getInt("request.required.acks"));

        Assert.assertEquals(true, props.props().containsKey("serializer.class"));
        Assert.assertEquals("kafka.serializer.StringEncoder", props.props().getString("serializer.class"));
    }

    @Test
    public void testPrepare() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();

        Map stormConf = new HashMap();
        Map<String, String> registrationArgument = new HashMap<>();
        registrationArgument.put(kafkaMetricsConsumer.topicKey, "test-topic");
        registrationArgument.put(kafkaMetricsConsumer.brokerKey, "test.somewhere.com:9092,test.somewhere.com:9092");
        registrationArgument.put(kafkaMetricsConsumer.metricPrefixKey, "test_metric");
        TopologyContext context = Mockito.mock(TopologyContext.class);
        IErrorReporter errorReporter = Mockito.mock(IErrorReporter.class);

        //Act
        kafkaMetricsConsumer.prepare(stormConf, registrationArgument, context, errorReporter);

        //Assert
        Assert.assertNotNull(kafkaMetricsConsumer.kafkaProducer);
        Assert.assertEquals("test-topic", kafkaMetricsConsumer.kafkaOutputTopic);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrepare_missing_broker_config() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();

        Map stormConf = new HashMap();
        Map<String, String> registrationArgument = new HashMap<>();
        registrationArgument.put(kafkaMetricsConsumer.topicKey, "test-topic");
        TopologyContext context = Mockito.mock(TopologyContext.class);
        IErrorReporter errorReporter = Mockito.mock(IErrorReporter.class);

        //Act
        kafkaMetricsConsumer.prepare(stormConf, registrationArgument, context, errorReporter);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPrepare_missing_topic_config() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();

        Map stormConf = new HashMap();
        Map<String, String> registrationArgument = new HashMap<>();
        registrationArgument.put(kafkaMetricsConsumer.brokerKey, "test.somewhere.com:9092,test.somewhere.com:9092");
        TopologyContext context = Mockito.mock(TopologyContext.class);
        IErrorReporter errorReporter = Mockito.mock(IErrorReporter.class);

        //Act
        kafkaMetricsConsumer.prepare(stormConf, registrationArgument, context, errorReporter);
    }

    @Test
    public void testHandleDataPoints() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();
        kafkaMetricsConsumer.kafkaOutputTopic = "test-topic";
        kafkaMetricsConsumer.metricPrefix = "test_metric";
        Producer mockProducer = Mockito.mock(Producer.class);
        kafkaMetricsConsumer.kafkaProducer = mockProducer;
        ArgumentCaptor<KeyedMessage> producerArgumentCaptor = ArgumentCaptor.forClass(KeyedMessage.class);

        IMetricsConsumer.TaskInfo taskInfo = new IMetricsConsumer.TaskInfo();
        taskInfo.srcComponentId = "testComponentId";
        taskInfo.srcTaskId = 1;
        taskInfo.srcWorkerHost = "test.worker.host";
        taskInfo.srcWorkerPort = 100;
        taskInfo.timestamp = 0L;
        taskInfo.updateIntervalSecs = 1;

        IMetricsConsumer.DataPoint dataPoint1 = new IMetricsConsumer.DataPoint();
        dataPoint1.name = kafkaMetricsConsumer.metricPrefix + "datapoint-1";
        Map<String, Integer> metricValue1 = new HashMap<>();
        metricValue1.put("metric1", 1);
        metricValue1.put("metric2", 2);
        dataPoint1.value = metricValue1;

        IMetricsConsumer.DataPoint dataPoint2 = new IMetricsConsumer.DataPoint();
        dataPoint2.name =  kafkaMetricsConsumer.metricPrefix + "datapoint-2";
        Map<String, Integer> metricValue2 = new HashMap<>();
        metricValue2.put("metric3", 3);
        metricValue2.put("metric4", 4);
        dataPoint2.value = metricValue2;

        Collection<IMetricsConsumer.DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(dataPoint1);
        dataPoints.add(dataPoint2);

        //Act
        kafkaMetricsConsumer.handleDataPoints(taskInfo, dataPoints);

        //Assert

        // verify that 'send' was called twice on producer, and apture args to 'send'
        Mockito.verify(mockProducer, times(2)).send(producerArgumentCaptor.capture());
        List<KeyedMessage> producerRecords = producerArgumentCaptor.getAllValues();
        Gson gson = new Gson();

        // assert on values for first ProducerRecord sent to Kafka
        KeyedMessage producerRecord1 = producerRecords.get(0);
        KafkaMetricsConsumer.KafkaMetric kafkaMetric1 =
                gson.fromJson((String)producerRecord1.message(), KafkaMetricsConsumer.KafkaMetric.class);
        Assert.assertEquals(dataPoint1.name, kafkaMetric1.metricName);
        Assert.assertEquals(((Map<String, Integer>)dataPoint1.value).get("metric1"), kafkaMetric1.metricValue.get("metric1"));
        Assert.assertEquals(((Map<String, Integer>)dataPoint1.value).get("metric2"), kafkaMetric1.metricValue.get("metric2"));
        Assert.assertEquals(taskInfo.srcComponentId, kafkaMetric1.taskInfo.srcComponentId);
        Assert.assertEquals(taskInfo.srcWorkerHost, kafkaMetric1.taskInfo.srcWorkerHost);
        Assert.assertEquals(taskInfo.srcTaskId, kafkaMetric1.taskInfo.srcTaskId);
        Assert.assertEquals(taskInfo.srcWorkerPort, kafkaMetric1.taskInfo.srcWorkerPort);
        Assert.assertEquals(taskInfo.timestamp, kafkaMetric1.taskInfo.timestamp);
        Assert.assertEquals(taskInfo.updateIntervalSecs, kafkaMetric1.taskInfo.updateIntervalSecs);

        // assert on values for second ProducerRecord sent to Kafka
        KeyedMessage producerRecord2 = producerRecords.get(1);
        KafkaMetricsConsumer.KafkaMetric kafkaMetric2 =
                gson.fromJson((String)producerRecord2.message(), KafkaMetricsConsumer.KafkaMetric.class);
        Assert.assertEquals(dataPoint2.name, kafkaMetric2.metricName);
        Assert.assertEquals(((Map<String, Integer>)dataPoint2.value).get("metric3"), kafkaMetric2.metricValue.get("metric3"));
        Assert.assertEquals(((Map<String, Integer>)dataPoint2.value).get("metric4"), kafkaMetric2.metricValue.get("metric4"));
        Assert.assertEquals(taskInfo.srcComponentId, kafkaMetric2.taskInfo.srcComponentId);
        Assert.assertEquals(taskInfo.srcWorkerHost, kafkaMetric2.taskInfo.srcWorkerHost);
        Assert.assertEquals(taskInfo.srcTaskId, kafkaMetric2.taskInfo.srcTaskId);
        Assert.assertEquals(taskInfo.srcWorkerPort, kafkaMetric2.taskInfo.srcWorkerPort);
        Assert.assertEquals(taskInfo.timestamp, kafkaMetric2.taskInfo.timestamp);
        Assert.assertEquals(taskInfo.updateIntervalSecs, kafkaMetric2.taskInfo.updateIntervalSecs);
    }

    @Test
    public void testHandleDataPoints_ignore_non_prefix_metric() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();
        kafkaMetricsConsumer.kafkaOutputTopic = "test-topic";
        kafkaMetricsConsumer.metricPrefix = "test_metric";
        Producer mockProducer = Mockito.mock(Producer.class);
        kafkaMetricsConsumer.kafkaProducer = mockProducer;
        ArgumentCaptor<KeyedMessage> producerArgumentCaptor = ArgumentCaptor.forClass(KeyedMessage.class);

        IMetricsConsumer.TaskInfo taskInfo = new IMetricsConsumer.TaskInfo();
        taskInfo.srcComponentId = "testComponentId";
        taskInfo.srcTaskId = 1;
        taskInfo.srcWorkerHost = "test.worker.host";
        taskInfo.srcWorkerPort = 100;
        taskInfo.timestamp = 0L;
        taskInfo.updateIntervalSecs = 1;

        IMetricsConsumer.DataPoint dataPoint1 = new IMetricsConsumer.DataPoint();
        dataPoint1.name =  kafkaMetricsConsumer.metricPrefix + "datapoint-1";
        Map<String, Integer> metricValue1 = new HashMap<>();
        metricValue1.put("metric1", 1);
        metricValue1.put("metric2", 2);
        dataPoint1.value = metricValue1;

        IMetricsConsumer.DataPoint dataPoint2 = new IMetricsConsumer.DataPoint();
        dataPoint2.name = "datapoint-2";
        Map<String, Integer> metricValue2 = new HashMap<>();
        metricValue2.put("metric3", 3);
        metricValue2.put("metric4", 4);
        dataPoint2.value = metricValue2;

        Collection<IMetricsConsumer.DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(dataPoint1);
        dataPoints.add(dataPoint2);

        //Act
        kafkaMetricsConsumer.handleDataPoints(taskInfo, dataPoints);

        //Assert

        // verify that 'send' was called twice on producer, and apture args to 'send'
        Mockito.verify(mockProducer, times(1)).send(producerArgumentCaptor.capture());
        List<KeyedMessage> producerRecords = producerArgumentCaptor.getAllValues();
        Gson gson = new Gson();

        // assert on values for first ProducerRecord sent to Kafka
        KeyedMessage producerRecord1 = producerRecords.get(0);
        KafkaMetricsConsumer.KafkaMetric kafkaMetric1 =
                gson.fromJson((String)producerRecord1.message(), KafkaMetricsConsumer.KafkaMetric.class);
        Assert.assertEquals(dataPoint1.name, kafkaMetric1.metricName);
        Assert.assertEquals(((Map<String, Integer>) dataPoint1.value).get("metric1"), kafkaMetric1.metricValue.get("metric1"));
        Assert.assertEquals(((Map<String, Integer>)dataPoint1.value).get("metric2"), kafkaMetric1.metricValue.get("metric2"));
        Assert.assertEquals(taskInfo.srcComponentId, kafkaMetric1.taskInfo.srcComponentId);
        Assert.assertEquals(taskInfo.srcWorkerHost, kafkaMetric1.taskInfo.srcWorkerHost);
        Assert.assertEquals(taskInfo.srcTaskId, kafkaMetric1.taskInfo.srcTaskId);
        Assert.assertEquals(taskInfo.srcWorkerPort, kafkaMetric1.taskInfo.srcWorkerPort);
        Assert.assertEquals(taskInfo.timestamp, kafkaMetric1.taskInfo.timestamp);
        Assert.assertEquals(taskInfo.updateIntervalSecs, kafkaMetric1.taskInfo.updateIntervalSecs);
    }

    @Test
    public void testCleanup() {
        //Arrange
        KafkaMetricsConsumer kafkaMetricsConsumer = new KafkaMetricsConsumer();
        Producer mockProducer = Mockito.mock(Producer.class);
        kafkaMetricsConsumer.kafkaProducer = mockProducer;

        //Act
        kafkaMetricsConsumer.cleanup();

        //Assert
        Mockito.verify(mockProducer, times(1)).close();
    }
}
