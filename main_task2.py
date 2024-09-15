from __future__ import print_function

import sys
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def correctRows(record): # Modified Helper Function to Convert Trip Time, Distance, Fare Amount, Total Amount to Float
    if len(record) == 17:
        try:
            field_4 = float(record[4])
            field_5 = float(record[5])
            field_11 = float(record[11])
            field_16 = float(record[16])
            if field_4 > 60 and field_5 > 0 and field_11 > 0 and field_16 > 0:
                return record[1], (field_16, field_4 / 60)  # Driver ID, (Total Earnings, Trip Time)
        except ValueError:
            return None
    return None

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output>", file=sys.stderr)
        sys.exit(-1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    try:
        sc = SparkContext(appName="Assignment-1")
        rdd = sc.textFile(input_file)
        
        # Clean data
        clean_rdd = rdd.map(lambda line: line.split(",")).map(correctRows).filter(lambda x: x is not None)
        
        # Calculate (Driver ID, (Total Earnings, Trip Time))
        earnings_by_time_rdd = clean_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        
        # Calculate average earnings per minute
        avg_earnings_per_minute_rdd = earnings_by_time_rdd.mapValues(lambda x: x[0] / x[1] if x[1] > 0 else 0)
        
        # Get Top 10 Drivers by Average Earnings per Minute
        top_10_drivers = avg_earnings_per_minute_rdd.top(10, key=lambda x: x[1])
        
        # Save Results to Output
        results_rdd = sc.parallelize(top_10_drivers)
        results_rdd.coalesce(1).saveAsTextFile(output_dir)

        sc.stop()
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(-1)
