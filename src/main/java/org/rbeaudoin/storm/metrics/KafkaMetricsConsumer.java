package org.rbeaudoin.storm.metrics;

import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of IMetricsConsumer used to consume Storm toplology metrics
 */
public class KafkaMetricsConsumer implements IMetricsConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsConsumer.class);
    final String brokerKey = "metrics.consumer.kafka.brokers";
    final String topicKey = "metrics.consumer.kafka.topic";
    final String metricPrefixKey = "metrics.consumer.metric.prefix";

    String metricPrefix;
    String kafkaOutputTopic;
    Producer<String, String> kafkaProducer;

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context, IErrorReporter errorReporter) {
        Map<String, String> metricsConsumerConfig = (Map<String, String>) registrationArgument;

        // broker and topic configuration is required
        if(!metricsConsumerConfig.containsKey(brokerKey)) {
            throw new IllegalArgumentException("Registration argument for metrics consumer is missing broker configuration");
        } else if(!metricsConsumerConfig.containsKey(topicKey)) {
            throw new IllegalArgumentException("Registration argument for metrics consumer is missing topic configuration");
        } else if(!metricsConsumerConfig.containsKey(metricPrefixKey)) {
            throw new IllegalArgumentException("Registration argument for metrics consumer is missing metrics prefix configuration");
        }

        ProducerConfig kafkaProducerProps = getKafkaProducerProps(metricsConsumerConfig);

        kafkaOutputTopic = metricsConsumerConfig.get(topicKey);
        metricPrefix = metricsConsumerConfig.get(metricPrefixKey);

        log.info("Configuring KafkaMetricsConsumer with topic properties: {}", kafkaOutputTopic);

        kafkaProducer = new Producer<>(kafkaProducerProps);
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        Gson gson = new Gson();

        for(DataPoint dataPoint : dataPoints) {
            //only process metrics with the specified prefix, ignore other storm metrics
            if(dataPoint.name.startsWith(metricPrefix)){
                KafkaMetric kafkaMetric = new KafkaMetric();
                kafkaMetric.metricName = dataPoint.name;
                kafkaMetric.metricValue = (Map<String, Integer>) dataPoint.value;
                kafkaMetric.taskInfo = taskInfo;

                log.info("Processing metric: {} with value: {}", dataPoint.name, dataPoint.value);

                KeyedMessage<String, String> metricProducerRecord =  new KeyedMessage<>(kafkaOutputTopic, gson.toJson(kafkaMetric));
                kafkaProducer.send(metricProducerRecord);
            }
        }
    }

    @Override
    public void cleanup() {
        kafkaProducer.close();
    }

    ProducerConfig getKafkaProducerProps(Map<String, String> metricsConsumerConfig) {
        String kafkaBrokers;

        //Get metric consumer config from registrationArgument
        kafkaBrokers = metricsConsumerConfig.get(brokerKey);

        log.info("Configuring KafkaMetricsConsumer with brokers: {}", kafkaBrokers);

        //Setup kafka producer
        Properties props = new Properties();
        props.put("metadata.broker.list", kafkaBrokers);
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        log.info("Configuring KafkaMetricsConsumer with producer properties: {}", props);

        return new ProducerConfig(props);
    }

    static class KafkaMetric {
        String metricName;
        Map<String, Integer> metricValue;
        TaskInfo taskInfo;
    }
}
