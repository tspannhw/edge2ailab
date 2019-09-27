package FlinkConsumer;

import commons.Commons;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Marcel Daeppen
 * @version 2019/09/16 15:08
 */
@Slf4j
public class iotConsumer {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Commons.GROUP_ID_CONFIG);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // get trx stream from kafka - topic "iot"
        DataStream<String> opcStream = env.addSource(
                new FlinkKafkaConsumer<>("iot", new SimpleStringSchema(), properties));

        opcStream.print("input message: ");

        // deserialization of the received JSONObject into Tuple
        DataStream<Tuple5<Long, String, String, String, Integer>> aggStream = opcStream
                .flatMap(new trxJSONDeserializer())
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(4)  ;

        aggStream.print();



        // write the aggregated data stream to a Kafka sink
        aggStream.addSink(new FlinkKafkaProducer<>(
                Commons.EXAMPLE_KAFKA_SERVER,
                "simulation_sum",
                new serializeSum2String()));

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }

    public static class trxJSONDeserializer implements FlatMapFunction<String, Tuple5<Long, String, String, String, Integer>> {
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple5<Long, String, String, String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            // get shop_name AND fx from JSONObject
            Long sensor_ts = jsonNode.get("sensor_ts").asLong();
            String sensor_id = jsonNode.get("sensor_id").toString();
            String sensor_0 = jsonNode.get("sensor_0").toString();
            String sensor_1 = jsonNode.get("sensor_1").toString();
            out.collect(new Tuple5<>(sensor_ts, sensor_id, sensor_0, sensor_1, 1));

        }

    }


    private static class serializeSum2String implements KeyedSerializationSchema<Tuple5<Long, String, String, String, Integer>> {
        @Override
        public byte[] serializeKey(Tuple5 element) {
            return (null);
        }
        @Override
        public byte[] serializeValue(Tuple5 value) {

            String str = "{"
                    + "\"type\"" + ":" +"\"Sum over 10 sec window\""
                    + "," + "\"sensor_ts_start\"" + ":" + value.getField(0).toString()
                    + "," + "\"sensor_id\"" + ":" + value.getField(1).toString()
                    + "," + "\"sensor_0\"" + ":" + value.getField(2).toString()
                    + "," + "\"window_count\"" + ":" + value.getField(4).toString() + "}";
            return str.getBytes();
        }
        @Override
        public String getTargetTopic(Tuple5 tuple5) {
            // use always the default topic
            return null;
        }
    }
}