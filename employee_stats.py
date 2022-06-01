from pyspark import SparkContext
import sys
import csv
cols='Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')
#filename = sys.argv[1]

def main():
   sc = SparkContext(appName='SparkWordCount')
   requested_value = sc.broadcast(sys.argv[1])
   input_file = sc.textFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/employeeDetail.csv')
   records = input_file.map(parserecord).filter (lambda rec : ( requested_value.value in rec[1]  and rec[2] > 50000 )).map(lambda x : x[0])
   #mapvalues = records.map(lambda rec: ( (rec[0]), 1))
   #reduceout = mapvalues.reduceByKey(lambda a, b: a + b)
   records.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/output_emp')
   sc.stop()


def parserecord(line):
    row = dict(zip(cols, [ a.strip() for a in next(csv.reader([line]))]))
    c_name = (row['Name'])  
    c_des = (row['Agency'])
    c_sal = float(row['AnnualSalary'])
    return(c_name,c_des,c_sal)



if __name__ == '__main__':
   main()
