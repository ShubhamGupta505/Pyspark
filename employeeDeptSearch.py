from pyspark import SparkContext
import re
import csv
cols='Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

def main():
	sc = SparkContext (appName = "employeeDept")
	filename = sc.textFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/employeeDetail.csv')
	records = filename.map(parserecords)
	req_records = records.filter(lambda rec : (rec[1] == "SECRETARY II" and rec[2] > 12000)).map(lambda x : x)
	req_records.saveAsTextFile('/home/administrator/Desktop/MapReduceStreaming/spark_demo/output_empDept')
	sc.stop()





def parserecords(line):
	row=dict(zip(cols , [a.strip() for a in next(csv.reader([line]))]))
	emp_name = row['Name']
	emp_des = row['JobTitle']
	salary = float(row['AnnualSalary'])
	return(emp_name,emp_des,salary)

if __name__ == '__main__':
	main()
