package com.guo.test.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author guo
 * @date 2021/2/22
 */
public class WordCount {
	public static void main(String[] args) throws Exception {
		// 创建执行环境
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// 从文件中读取数据
		String inputPath = "/Users/chun/workspace/workspace_test/flink-test/src/main/resources/hello.txt";
		DataSet<String> inputDataSet = env.readTextFile(inputPath);

		// 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
		DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
				.groupBy(0)// 按照第一个位置的word分组
				.sum(1);// 将第二个位置上的数据求和

		resultSet.print();
	}

	public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

		public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
			// 按空格分词
			String[] words = value.split(" ");
			for (String word : words) {
				collector.collect(new Tuple2<>(word, 1));
			}
		}
	}

}
