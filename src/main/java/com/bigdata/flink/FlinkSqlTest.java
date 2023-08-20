package com.bigdata.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSqlTest {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        //设置WebUI绑定的本地端口
        configuration.setString(RestOptions.BIND_PORT,"8088");
        // 0.初始化环境 调用父类方法初始化环境 调用父类方法获取StreamTableEnvironment 不要手工设置相关运行参数,由作业管理平台设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 主单源SQL
        String source_table = "CREATE TABLE sourceT ( " +
                "  uuid varchar(20), " +
                "  name varchar(10), " +
                "  age int, " +
                "  ts timestamp(3), " +
                "  `partition` varchar(20) " +
                ") WITH ( " +
                "  'connector' = 'datagen', " +
                "  'rows-per-second' = '1' " +
                ")";

        tEnv.executeSql(source_table);

        Table table = tEnv.sqlQuery("select * from sourceT");
        DataStream<Row> appendStream = tEnv.toAppendStream(table, Row.class);
        appendStream.print();

        env.execute();
    }

}
