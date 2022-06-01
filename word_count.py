from pyspark import SparkContext
import sys

#filename = sys.argv[1]
def main():
   sc = SparkContext(appName='SparkWordCount')
   input_file = sc.textFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/Test.txt')
   counts = input_file.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
   counts.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/output_demo2')
   sc.stop()
if __name__ == '__main__':
   main()


#spark-submit --master local Reviews_Music.py
	

