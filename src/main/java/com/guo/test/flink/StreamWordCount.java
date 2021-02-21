package com.guo.test.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author guo
 * @date 2021/2/22
 */
public class StreamWordCount {
	public static void main(String[] args) throws Exception {
		// 创建流处理执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(8);// 并行度

		// 从文件中读取数据
//		String inputPath = "/Users/chun/workspace/workspace_test/flink-test/src/main/resources/hello.txt";
//		DataStream<String> inputDataStream = env.readTextFile(inputPath);

		// 用 ParameterTool 工具从程序启动参数中提取配置项
//		ParameterTool parameterTool = ParameterTool.fromArgs(args);
//		String host = parameterTool.get("host");
//		int port = parameterTool.getInt("port");
		String host = "localhost";
		int port = 8379;

		// 从 Socket 文本流读取数据，测试命令：nc -lk 8379
		DataStream<String> inputDataStream = env.socketTextStream(host, port);

		// 基于数据流进行转换计算
		DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
				.keyBy(0)
				.sum(1);

		resultStream.print();

		// 执行任务
		env.execute();
	}
}
