package org.example.SchemaMethods;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Objects;

/**
 * 用于解析Kafka发来的record对象
 */
public class MyKafkaDeserializationSchema implements KafkaDeserializationSchema<String> {
    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        Header groupIdHeader =  consumerRecord.headers().lastHeader("groupId");
        if (Objects.nonNull(groupIdHeader)) {
            byte[] groupId = groupIdHeader.value();
            // 此处yourGroupId替换成你们组的组号
            if(Arrays.equals("18".getBytes(), groupId)){
                // 额外记录这条数据
                try(FileOutputStream outputStream = new FileOutputStream("./specialMessage.txt", true)) {
                    outputStream.write(consumerRecord.value());
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        return Arrays.toString(consumerRecord.value());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
