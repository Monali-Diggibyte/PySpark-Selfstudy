# -*- coding: utf-8 -*-
"""
Created on Wed Jul 13 01:08:28 2022

@author: Monali
"""
#####################################
# Pyspark dataframe
# Reading dataset
# Checking datatype of Columns(Schema)
# Selecting columns and indexing
# checking describe option similar to pandas
# adding columns
# dropping columns
# #####################################

# Create new environment while working with PySpark

# pip install PySpark

import pyspark as ps

# To read file using pandas
import pandas as pd
df_emp = pd.read_csv('D:\Diggibyte\SelfStudy_PySpark_Project\Spark Demo\emp_details.csv')

## always start Spark Session ###
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('MyPractise').getOrCreate
spark

# Read dataset wrt spark option:1
df_emp_pyspark = spark.read.csv(r'D:\Diggibyte\SelfStudy_PySpark_Project\Spark Demo\emp_details.csv')
df_emp_pyspark   ## DataFrame[_c0: string, _c1: string, _c2: string, _c3: string, _c4: string] : column names also 
                 ## taken as values to avoid these use option('header', 'true') while loading data file
df_emp_pyspark.show()

df_emp_pyspark1 = spark.read.option('header', 'true').csv(r'D:\Diggibyte\SelfStudy_PySpark_Project\Spark Demo\emp_details.csv')
df_emp_pyspark1  ## DataFrame[Employee_id: string, Emp_First_name: string, Emp_Last_Same: string, Experience: string, Salary: string]
df_emp_pyspark1.show()

type(df_emp_pyspark1)  ## pyspark.sql.dataframe.DataFrame

df_emp_pyspark1.head(5) ## by default its only gives first row .head()

df_emp_pyspark1.printSchema() ## gives Schema/columns information
