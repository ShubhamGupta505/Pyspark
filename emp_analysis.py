from pyspark import SparkContext
import csv
import sys

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

def main():
	sc = SparkContext (appName = "empAnalyasis")
	requested_value = sc.broadcast(sys.argv[1])
	inputfile = sc.textFile ('/home/administrator/Desktop/MapReduceStreaming/spark_demo/employeeDetail.csv')
	records = inputfile.map(parserecord)
	final_records = records.filter(lambda x : requested_value.value in x[1])
	
	
	salary = final_records.map(lambda x :[ x[2]])
	average_salary = salary.flatMap (lambda x : x).map (lambda x : float(x)) 
	#average_salary.collect()
	sal = average_salary.mean()
	final_records = records.filter(lambda x : x[2] > sal)
	records1 = final_records.map (lambda x : (x[0] ,x[1], x [2]) )
	records1.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/output_empAnalyasis6')
	sc.stop()
	
	
	
	
	
def parserecord(line):
	row = dict (zip(cols , [a.strip()  for a in next (csv.reader ([line]))]))
	name = row ['Name']
	title = row['JobTitle']
	salary  = float (row['AnnualSalary'])
	return (name, title ,salary)
	
if __name__ == '__main__':
	main()
