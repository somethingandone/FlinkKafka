package org.example;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.example.clickHouseUtil.MyClickHouseUtil;
import org.example.pojo.Data_mx;
import org.example.pojo.Data_pojo;



import java.util.Properties;

/**
 * FlinkConsumer，并且能够进行数据存入
 */
public class FlinkConsumer {

    private final StreamExecutionEnvironment env;

    private final FlinkKafkaConsumer<String> consumer;

    private final Types types = new Types();

    /**
     * 一些基本设置，抄助教文档的
     */
    public FlinkConsumer(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201250115");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2023\";");
        String topic = "data";
        this.consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }

    /**
     * 数据消费的主要运行函数
     */
    public void run(){
        // 每 5s 做一次 checkpoint
        env.enableCheckpointing(5000);
        // 设置从最新的开始消费
        consumer.setStartFromLatest();
        // 添加数据源
        DataStreamSource<String> stream = env.addSource(this.consumer);
        // 貌似是获得单条信息并且进行ETL的部分
        SingleOutputStreamOperator<Data_mx> dataStream = stream.map(new MapFunction<String, Data_mx>() {
            @Override
            public Data_mx map(String s) throws Exception {
                // 根据Data_pojo模板将传入的string进行json语句解析
                Data_pojo dataPojo = JSONObject.parseObject(s, Data_pojo.class);

                String type = types.getTypeString(dataPojo.getEventType());
                // 生成对应的pojo名
                type = type.substring(0, 1).toUpperCase() + type.substring(1) + "_mx";
                Data_mx dataDetails = null;
                try {
                    // 利用类名进行对应类的生成和实例化
                    dataDetails = (Data_mx) Class.forName(type).getConstructor().newInstance();

                    boolean flag = dataDetails.setDataMx(dataPojo.getEventBody());
                    if (!flag) return null;
                }catch (Exception e){
                    e.printStackTrace();
                }
                return dataDetails;
            }
        });
        // 取得的单条信息判定为有效则进行写入数据库操作
        if (dataStream != null) {
            MyClickHouseUtil myClickHouseUtil = new MyClickHouseUtil();
            dataStream.addSink(myClickHouseUtil);
        }

        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
            System.err.println("env execute error!");
        }
    }

    public static void main(String[] args) {
        FlinkConsumer flinkConsumer = new FlinkConsumer();
        flinkConsumer.run();
    }
}
