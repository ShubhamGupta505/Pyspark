wimport findspark
import os
from pyspark import SparkContext
import re
import sys

spark_location='/usr/spark/spark-2.4.4-bin-hadoop2.6'  # Set your own
java8_location= '/usr/lib/jvm/java-8-openjdk-amd64/' # Set your own
os.environ['JAVA_HOME'] = java8_location
findspark.init(spark_home=spark_location)


def main():
   # Insure a search term was supplied at the command line
   if len(sys.argv) != 2:
       sys.stderr.write('Usage: {} <search_term>'.format(sys.argv[0]))
       sys.exit()
       
   # Create the SparkContext
   sc = SparkContext(appName='SparkWordCount')
   
   # Broadcast the requested term
   requested_movie = sc.broadcast(sys.argv[1])
   
   # Load the input file
   source_file = sc.textFile('hdfs://localhost:9000/demo/movies.csv')
   
   # Get the movie title from the second fields
   titles = source_file.map(lambda line: line.split(',')[1])
   
   # Create a map of the normalized title to the raw title
   normalized_title = titles.map(lambda title: (re.sub(r'\s*\(\d{4}\)','', title).lower(), title))
   
   # Find all movies matching the requested_movie
   matches = normalized_title.filter(lambda x: requested_movie.value in x[0]) 
   
   # Collect all the matching titles
   matching_titles = matches.map(lambda x: x[1]).distinct().collect()
   
   # Display the result

   for title in matching_titles:
      print (title)
      
   #matching_titles.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/output_Specific2')
   sc.stop()
   
if __name__ == '__main__':
    
    main()
