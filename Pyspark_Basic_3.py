# -*- coding: utf-8 -*-
"""
Created on Tue Jul 12 23:00:25 2022

@author: Monali
"""
########################################################
# Pyspark Handling Missing Values
# Dropping column
# Dropping Rows
# handling missing values by mean, median, mode
########################################################

# start pyspark session
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyPractise").getOrCreate()
spark

df_emp = spark.read.csv(r'D:\Diggibyte\SelfStudy_PySpark_Project\Spark Demo\Spark_SelfStudy\emp_details.csv', header= True, inferSchema= (True))
df_emp.show()
df_emp.printSchema()

### Drop the columns of dataframe ###
df_emp.drop('Experience').show()

### Drop rows with Null values ###
df_emp.na.drop().show() # all rows with null values get deleted if nothing mentioned inside drop()

# .na.drop(default how = any)
# if any : If any of the value is null then drop : by default how="any"
df_emp.na.drop(how="any").show() # df_emp.na.drop().show()

# if all : If all the values are null then only drop.
df_emp.na.drop(how="all").show()

## threshold (default thresh= None)
df_emp.na.drop(how="any", thresh=2).show()
# thresh=2 means Rows with at least 2 NOT NULL values should b present, drop rest rows with Null.

## subset (default subset= None)
df_emp.na.drop(how="any", subset=['Salary']).show()
# subset=['Salary'] : means drop the rows where Salary coulmn having Null value

### Filling Missing values/ Missing Value Imputation ###
df_emp.na.fill(value ='M.S.').show()
# value ='Missing value': Replace all null values with 'Missing value' only for string type columns

df_emp.na.fill('Missing', 'Emp_First_name').show()
df_emp.na.fill(value = 'Missing', subset=['Emp_First_name', 'Emp_Last_Same']).show()

### Imputation ###
## Mean Imputation ##
from pyspark.ml.feature import Imputer

imputer = Imputer(inputCols = ['Employee_id'],
                  outputCols = ["{}_Imputed".format(c) for c in ['Employee_id']]).setStrategy("mean")

imputer.fit(df_emp).transform(df_emp).show()

imputer1 = Imputer(inputCols = ['Experience', 'Salary'],
                  outputCols = ["{}_Imputed".format(c) for c in ['Experience', 'Salary']]).setStrategy("median")

imputer1.fit(df_emp).transform(df_emp).show()

imputer2 = Imputer(inputCols = ['Emp_Last_Same'],
                  outputCols = ["{}_Imputed".format(c) for c in ['Emp_Last_Same']]).setStrategy("mode")

imputer2.fit(df_emp).transform(df_emp).show()
