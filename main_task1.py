from __future__ import print_function

import sys
from pyspark import SparkContext

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def correctRows(p):
    if len(p) == 17:
        if isfloat(p[5]) and isfloat(p[11]):
            if float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0:
                return p
    return None

if __name__ == "__main__":
    print("Arguments received:", sys.argv)
    
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output>", file=sys.stderr)
        sys.exit(-1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    
    print("Input file:", input_file)
    print("Output directory:", output_dir)
    
    try:
        sc = SparkContext(appName="Assignment-1")
        print("SparkContext initialized")

        # Read the input file
        rdd = sc.textFile(input_file)
        print(f"Number of lines read: {rdd.count()}")
        
        # Clean the data
        clean_rdd = rdd.map(lambda line: line.split(",")).map(correctRows).filter(lambda x: x is not None)
        print(f"Clean RDD count: {clean_rdd.count()}")
        
        # Create medallion and hack license pairs
        medallion_pair_rdd = clean_rdd.map(lambda p: (p[0], p[1]))
        print(f"Medallion Pair RDD count: {medallion_pair_rdd.count()}")
        
        # Count distinct drivers per medallion
        driver_count_rdd = medallion_pair_rdd.distinct().map(lambda p: (p[0], 1)).reduceByKey(lambda a, b: a + b)
        print(f"Driver Count RDD count: {driver_count_rdd.count()}")
        
        # Get top 10 medallions by distinct driver count
        results_1 = driver_count_rdd.top(10, key=lambda x: x[1])
        print("Top 10 results:", results_1)
        
        # Convert results to RDD and coalesce to 1 partition
        results_rdd = sc.parallelize(results_1)
        results_rdd.coalesce(1).saveAsTextFile(output_dir)
        print(f"Results saved to {output_dir}")

        sc.stop()
        print("SparkContext stopped")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(-1)
