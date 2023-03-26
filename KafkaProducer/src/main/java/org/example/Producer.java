package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.io.*;
import java.util.Properties;

public class Producer {

    private final KafkaProducer<String, String> producer;

    /**
     * 文件夹路径
     */
    private final String path;

    /**
     * 构造函数
     * @param path 文件夹路径
     */
    public Producer(String path){
        Properties props = new Properties();
        props.put("bootstrap.servers", "121.43.165.220:9092");
        props.put("acks", "0");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.path = path;
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * 实现功能为读取文件夹路径，将文件夹下文件进行按行读取为字符串
     * 之后以data为主题进行发送，没有根据eventDate进行判断发送
     */
    public void run() {
        File dir = new File(path);
        File[] files = dir.listFiles();
        assert files != null;
        int num_message = 0;
        for (File file : files) {
            BufferedReader br;
            try {
                br = new BufferedReader(new FileReader(file));
                while (br.readLine() != null){
                    // 生成信息对象，内容为读取的一行文本
                    ProducerRecord<String, String> record = new ProducerRecord<>("data", br.readLine());
                    producer.send(record);
                    num_message++;
                    if (num_message%2000000 == 0){
                        System.out.println("已经发送的信息数量"+num_message);
                    }
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        Producer pro = new Producer("给个路径吧求求了");
        pro.run();
        System.out.println("Hello world!");
    }
}