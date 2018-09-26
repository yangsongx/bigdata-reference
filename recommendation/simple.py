import sys
from pyspark.sql import SparkSession
import pyspark.ml.recommendation

# example from API guide doc...
def simple_test(sp):
    training_data_df = sp.createDataFrame(
            [(0, 0, 4.0),
             (0, 1, 2.0), 
             (1, 1, 3.0), 
             (1, 2, 4.0), 
             (2, 1, 1.0), 
             (2, 2, 5.0)],
            ["user","item", "rating"])

    valid_data_df = sp.createDataFrame(
            [(0, 2),
             (1, 0), 
             (2, 0)],
             ["user", "item"])

    als = pyspark.ml.recommendation.ALS(maxIter = 5,
                regParam = 0.01,
                userCol = "user",
                itemCol = "item",
                ratingCol = "rating",
                coldStartStrategy = "drop")
    m = als.fit(training_data_df)

    predict_data = m.transform(valid_data_df)
    print(predict_data)
    predict_data.show()
    print("=== end of simple test code ===")

    m.save('./alsmod')

    m2 = pyspark.ml.recommendation.ALSModel.load('./alsmod')

    predict2 = m2.transform(valid_data_df)
    predict2.show()
    print("--end of load result")

    return 0

if __name__ == "__main__":
    print("recommendation learning code")

    sp = SparkSession.builder.appName("Sample").getOrCreate()

    # read into a DataFrame
    df = sp.read.csv('./ratings.csv',
            header = True,
            inferSchema = True)
    df.show()
    print("===end of the df show===")

    # Cross-Validation
    (training_df, cv_df) = df.randomSplit([0.8, 0.2])


    als = pyspark.ml.recommendation.ALS(maxIter = 5,
                regParam = 0.01,
                userCol = "userId",
                itemCol = "movieId",
                ratingCol = "rating",
                coldStartStrategy = "drop")

    my_model = als.fit(training_df)

    predict_data = my_model.transform(cv_df)
    print(type(predict_data))
    print(predict_data)
    predict_data.show()
    print("--- end of predict ---")
    cv_df.show()
    recommend = my_model.recommendForAllUsers(10)
    recommend.show()


    simple_test(sp)

    sp.stop()

