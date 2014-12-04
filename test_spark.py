__author__ = 'pliu'

import os
import sys

import os
import sys
import pdb
from numpy import random
from operator import add


def spark_conf(profile='local', num_cores="1", memory="512m", appname='test_app'):
    #profile options are 'local' (default), 'cluster' for the big spark cluster

    # Append to PYTHONPATH so that pyspark could be found
    sys.path.append("/Users/lliao/Downloads/spark-1.1.0/python")
    sys.path.append("/Users/lliao/Downloads/spark-1.1.0/python/pyspark")
    os.environ['SPARK_HOME'] = "/Users/lliao/Downloads/spark-1.1.0"
    # Now we are ready to import Spark Modules
    try:
        from pyspark import SparkContext
        from pyspark import SparkConf
    except ImportError as e:
        print ("Error importing Spark Modules", e)
        sys.exit(1)

    if profile == 'cluster':
        os.environ['PYSPARK_PYTHON'] = '/usr/bin/python'
#        conf = SparkConf().setAppName(appname).setMaster('spark://13.4.44.6:7077')
        conf = SparkConf().setAppName(appname).setMaster('spark://my-instance:7077')
#        conf.set("spark.home", "/opt/apache/spark-1.0.0-bin-cdh4")
        conf.set("spark.home", "/mnt/data/download/spark-1.1.0/bin")
        conf.set("spark.executor.memory", memory)
        conf.set("spark.cores.max", num_cores)

        sc = SparkContext(conf=conf)

    elif profile == 'local':
        os.environ['PYSPARK_PYTHON'] = sys.executable
        conf = SparkConf().setAppName(appname).setMaster('local[{num_cores}]'.format(num_cores=num_cores))
        conf.set("spark.home", "/Users/lliao/Downloads/spark-1.1.0/python")
        conf.set("spark.executor.memory", memory)
        conf.set("spark.cores.max", num_cores)
        conf.set("worker_max_heapsize", "128m")
#        conf.set("spark.driver.host", "13.1.101.152")

        sc = SparkContext('local[{num_cores}]'.format(num_cores=num_cores), conf=conf)

    else:
        print ("Incorrect profile option")
        sys.exit(2)

    return sc


sc = spark_conf(profile="cluster", num_cores='1', memory="256m", appname='test_app')

if __name__ == "__main__":
    slices = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 10**4

    def f(_):
        x = random.rand() * 2 - 1
        y = random.rand() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0

    count = sc.parallelize(xrange(1, n+1)).map(f).reduce(add)
    #pdb.set_trace()

    print "Pi is roughly %f" % (4.0 * count / n)

    sc.stop()