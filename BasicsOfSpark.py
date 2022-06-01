
from pyspark import SparkContext
def main():
	sc = SparkContext (appName ="Basics")
	#Creating RDD from Collections
	data = ["DS", "ESR", "IS","iOS", "Android"]
	rdd = sc.parallelize(data)
	rdd.glom().collect()
	rdd.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo3')
	
	data2 = [1, 2, 3, 4, 5, 6]
	rdd2 = sc.parallelize(data2)
	map_result = rdd2.map(lambda x: x * 2)
	map_result.collect()
	map_result.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo4')
	
	data3 = [1, 2, 3, 4, 5, 6]
	rdd3 = sc.parallelize(data3)
	filter_result = rdd3.filter(lambda x: x % 2 == 0)
	filter_result.collect()
	filter_result.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo5')
	
	data4 = [1, 2, 3, 2, 4, 1]
	rdd4 = sc.parallelize(data4)
	distinct_result = rdd4.distinct()
	distinct_result.collect()
	distinct_result.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo6')
	
	
	data5 = [1, 2, 3, 4]
	rdd = sc.parallelize(data5)
	flat_map = rdd.flatMap(lambda x: [x, pow(x,2)])
	flat_map.collect()
	flat_map.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo8')
	
	data6 = [1, 2, 3]
	rdd5 = sc.parallelize(data6)
	rdd5.reduce(lambda a, b: a * b)
	rdd5.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo9')
	
	data7 = [1, 2, 3]
	rdd7 = sc.parallelize(data7)
	rdd7.take(2)
	rdd7.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo10')
	
	data8 = [1, 2, 3, 4, 5]
	rdd8 = sc.parallelize(data8)
	rdd8.collect()
	rdd8.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo11')
	
	data9 = [6,1,5,2,4,3]
	rdd9 = sc.parallelize(data9)
	rdd9.takeOrdered(4, lambda s: -s)
	rdd9.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/out_demo12')


if __name__ =='__main__':
	main()