from dm import g_barplot, g_histogram, g_scatter, g_chi2, g_kde

import argparse
import sys
import logging

from pyspark import SparkContext, SQLContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StructField

from itertools import combinations
import numpy as np
import pandas as pd

def quiet_log(sc):
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    return sc

def load_parquet(database, table, quiet):
    sc = SparkContext()
    if quiet:
        sc = quiet_log(sc)
        
    sqlContext = SQLContext(sc)    
    sqlContext.setConf('spark.sql.parquet.binaryAsString', 'True')
    print (database, table)
    return sqlContext.sql('Select * from parquet.`/user/hive/warehouse/{:s}.db/{:s}`'.format(database, table)), sc, sqlContext
    
path = '/var/lib/hadoop-hdfs/Jannes Test/dm_library/graphs'
http_path = 'http://cdh578egzp.telkom.co.za:8880/files/Jannes%20Test/dm_library/graphs'

def create_table(df, table_name, sqlContext, cols = None, size_limit = 30):
    
    df.persist()
    no_plot_cols = []
    output = []
    cols_complete = []
    var_cols = ['colm', 'col_type', 'uniques', 'missing', 'mean', 'stddev', 'graph']

    type_dict = {'float':'numeric','long':'numeric', 'integer':'numeric', 
                 'smallint':'numeric', 'int':'numeric', 'bigint':'numeric', 'string':'categorical', 
                 'timestamp':'date', 'binary':'indicator','decimal(9,2)':'numeric'}   
    
    if cols == None:
        cols = df.columns

    for c in cols:
        print 'Getting {:s} data'.format(c)  
        sys.stdout.flush()
        
        #print("Producing graphs" + str(col_graphs))
        cols_complete.append(c)
        rem_cols = list(set(df.columns) - set(cols_complete))
        
        #Initialize columns
        uniq = 0
        null = 0 
        mean = 0 
        std_dev = 0
        g = 0
        g_path = 0
        col_g = []
        # col_g_paths = []
        # col_g.extend(np.zeros(len(col_graphs)))        
        
        uniq = df.select(c).distinct().count()        
        print ('... uniques: {:d}'.format(uniq))

        col_type = df.select(c).dtypes[0][1]
        col_type = type_dict[col_type]
        if uniq == 2:
            col_type = 'indicator'
        print ('... column type: {:s}'.format(col_type))    

        null = df.where(F.col(c).isNull()).count()        
        print ('... nulls: {:d}'.format(null))
        
        if (uniq < size_limit) & (col_type in ['categorical', 'indicator']):
            g, g_path = g_barplot(df, c) 
        
        if col_type in ['numeric']:   
            df_sum = df.select(c).agg(F.avg(F.col(c)),
                                   F.stddev(F.col(c))).take(1)
            mean = df_sum[0][0]
            std_dev = df_sum[0][1]
            
            print ('... numerical summary: {:0.2f}, {:0.2f}'.format(mean, std_dev))               
            g, g_path = g_histogram(df, c)
        
        print('... Single Graph Done')                       
        output.append(tuple([c, col_type, uniq, null, mean, std_dev, g_path]))        
               
    # 2 factor charts here               
        
    # create the table
    schema_list = [T.StructField("colm", T.StringType(), True),
                   T.StructField("col_type", T.StringType(), True),
                   T.StructField("uniques", T.IntegerType(), True),
                   T.StructField("missing", T.IntegerType(), True),
                   T.StructField("mean", T.FloatType(), True),
                   T.StructField("stddev", T.FloatType(), True),
                   T.StructField("graph", T.StringType(), True) ]

    # graph_schema_list = [T.StructField(x, T.StringType(), True) for x in col_graphs]
    # schema_list.extend(graph_schema_list)
    schema = T.StructType(schema_list)         
    print schema
    
    rdd = sc.parallelize(output) 
    
    hive = HiveContext(sc)
    hive.createDataFrame(rdd, schema=schema)\
        .write.mode('overwrite')\
        .saveAsTable('datamining.' + table_name,format='parquet')    
    df.unpersist()
    print '... {:s} saved to cluster'.format(table_name)
    sys.stdout.flush()
    
if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-db', '--database', help='please provide the database in the cluster',required=True)
    ap.add_argument('-t', '--table', help='please provide the table in the cluster',required=True) 
    ap.add_argument('-q', '--quiet', help='silence logging', action='store_true') 
    
    args = vars(ap.parse_args())     
    print args       

    df, sc, sqlContext = load_parquet(args['database'], args['table'], args['quiet'])               
    create_table(df, 
                 '{:s}_{:s}'.format(args['database'], args['table']), 
                 sqlContext)

    sc.stop()
    
    
    