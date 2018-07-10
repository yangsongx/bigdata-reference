from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.ml.linalg import Vector, Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import IntegerType, FloatType

#Demo how to use Linear Regression usage

class MyLinearRegression:
    __mem_fn = 'foo.data'
    __mem_sp = None

    def __init__(self, fn):
        self.__mem_fn = fn
        self.__mem_sp = SparkSession.builder.appName("mystudy").getOrCreate()

    def __del__(self):
        print("destructor")
        #self.__mem_sp.stop()


    def start(self):
        print("regression study begin\n\n")

        print("Try read the %s file...." %(self.__mem_fn))
        csvdf = self.__mem_sp.read.csv(self.__mem_fn, inferSchema = True)
        csvdf.printSchema()
        csvdf2 = csvdf.select('_c1', '_c0').toDF('label', 'features')
        csvdf2.printSchema()
        csvdf2.show()

        df_vec = csvdf2.rdd\
                 .map(lambda row: \
                         Row(label = row['label'], \
                             features=Vectors.dense(row['features'])\
                            )\
                     )\
                 .toDF()\
                 .select('label', 'features')
        df_vec.printSchema()
        df_vec.show()

        lr = LinearRegression()
        lrm = lr.fit(df_vec)

        print("==============\nNow, intercept:%s, coefficients:%s\n===========" \
                %(str(lrm.intercept), str(lrm.coefficients)))
        print(type(lrm.intercept))
        print(type(lrm.coefficients))
        print(type(lrm.summary))
        print(lrm.summary.totalIterations)
        print(lrm.summary.rootMeanSquaredError)
        lrm.summary.residuals.show()

        print("\n\nregression study end")
        return 0

    #an un-necessary steps... SHOULD use start() instead
    def bad_start(self):
        print("regression study begin\n\n")

        print("Try read the %s file...." %(self.__mem_fn))
        csvdf = self.__mem_sp.read.csv(self.__mem_fn)
        csvdf.printSchema()
        csvdf2 = csvdf.select('_c1', '_c0').toDF('label', 'features')
        csvdf2.printSchema()
        csvdf2.show()

        csvdf3 = csvdf2.withColumn('features', csvdf2.features.cast(FloatType()))
        csvdf3.printSchema()
        csvdf3.show()

        # next, try convert features into Vector...
        df_vec = csvdf3.rdd.map(lambda row: Row(label = row['label'], feature=Vectors.dense(row['features']))).toDF()
        df_vec.printSchema()
        df_vec.show()

        print("\n\nregression study end")
        return 0

# Show how-to handle the CSV format....
class CsvOperationDemo():
    __mem_fn = 'foo.csv'
    __mem_sp = None

    def __init__(self, fn):
        self.__mem_fn = fn
        self.__mem_sp = SparkSession.builder.appName("mystudy").getOrCreate()

    def __del__(self):
        print("destructor")
        #self.__mem_sp.stop()

    def start(self):
        csv = self.__mem_sp.read.csv(self.__mem_fn)
        csv.printSchema()
        csv.show()

        csv2 = self.__mem_sp.read.csv(self.__mem_fn, inferSchema=True)
        csv2.printSchema()
        csv2.show()

def demo_csv():
    c = CsvOperationDemo('/home/yang/src/spark-learning/books.csv')
    c.start()
    return 0

def demo_linear_regression():
    mylr = MyLinearRegression('/home/yang/src/spark-learning/lr.csv')
    mylr.start()
    return 0

####################################################################

sc = SparkContext()
print("All my task going here...")
demo_linear_regression()
#demo_csv()

sc.stop()
