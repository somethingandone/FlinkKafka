package org.example.clickHouseUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.pojo.Data_mx;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;

/**
 * 进行数据sink到clickHouse的工具类
 * 考虑到的一个优化的点是把connection变成static或者另写一个类进行连接，避免反复连接
 */
public class MyClickHouseUtil extends RichSinkFunction<Data_mx> {
    private ClickHouseConnection connection = null;

    /**
     * 抄的，应该不会有问题
     * @param parameters 不知道
     * @throws Exception 默认的
     */
    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
    }

    /**
     * 抄的，应该不会有问题
     * @throws Exception 默认的
     */
    @Override
    public void close() throws Exception{
        super.close();
        if (connection != null) connection.close();
    }

    /**
     * 用于实现连接和sink的主要函数
     * @param dataDetails 即pojo对象
     * @param context 不晓得
     * @throws Exception 默认的
     */
    public void invoke(Data_mx dataDetails, Context context) throws Exception{
        String url = "求求了，给个路径吧";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser("default");
        properties.setPassword("default");
        properties.setSessionId("default-session-id");
        // 新建数据源
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        try {
            connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(dataDetails.getSql());
            dataDetails.setPreparedStatement(preparedStatement);
            preparedStatement.execute();
        }catch (Exception e){
            e.printStackTrace();
            System.err.println("MyClickHouseUtil invoke connection error!");
        }
    }
}
