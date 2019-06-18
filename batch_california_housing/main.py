import logging
from typing import List, Union

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, FloatType, Row
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import StandardScaler
from pyspark.ml.regression import LinearRegression


def main():
    spark = SparkSession.builder \
        .master('local') \
        .appName('california-housing') \
        .config('spark.executor.memory', '1gb') \
        .getOrCreate()
    sc: SparkContext = spark.sparkContext
    sc.setLogLevel('WARN')
    logger.info("app_id = {}".format(sc.applicationId))

    df_data = read_data(spark, './data/CaliforniaHousing/cal_housing.data')
    log_data_frame("df_data", df_data)

    df_data = df_data.withColumn('medianHouseValue', df_data['medianHouseValue'] / 100000)\
        .withColumn('roomsPerHousehold', df_data['totalRooms'] / df_data['households'])\
        .withColumn('populationPerHousehold', df_data['population'] / df_data['households'])\
        .withColumn('bedroomsPerRoom', df_data['totalBedRooms'] / df_data['totalRooms'])\
        .select('medianHouseValue', 'totalBedRooms', 'population', 'households', 'medianIncome',
                'roomsPerHousehold', 'populationPerHousehold', 'bedroomsPerRoom')
    log_data_frame("df_data", df_data)

    rdd_labeled_data = df_data.rdd.map(lambda x: (x[0], DenseVector(x[1:])))
    df_labeled_data = spark.createDataFrame(rdd_labeled_data, ['y', 'x'])
    log_data_frame("df_labeled_data", df_labeled_data)

    standard_scaler = StandardScaler(inputCol='x', outputCol='x_scaled')
    scaler = standard_scaler.fit(df_labeled_data)
    df_scaled = scaler.transform(df_labeled_data)
    log_data_frame("df_scaled", df_scaled)

    df_train, df_test = df_scaled.randomSplit([.8, .2], seed=52)
    lr = LinearRegression(featuresCol='x_scaled', labelCol='y', maxIter=10, regParam=0.3,
                          elasticNetParam=0.8)
    model = lr.fit(df_train)

    df_predicted = model.transform(df_test)
    log_data_frame("df_predicted", df_predicted)


def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    rdd_data: RDD = spark.sparkContext.textFile(file_path).map(lambda line: line.split(','))

    schema = StructType([
        StructField('longitude', FloatType()),
        StructField('latitude', FloatType()),
        StructField('housingMedianAge', FloatType()),
        StructField('totalRooms', FloatType()),
        StructField('totalBedrooms', FloatType()),
        StructField('population', FloatType()),
        StructField('households', FloatType()),
        StructField('medianIncome', FloatType()),
        StructField('medianHouseValue', FloatType()),
    ])

    rdd_rows = rdd_data.map(format_row)
    return spark.createDataFrame(rdd_rows, schema)


def format_row(fields: List[str]) -> Row:
    return Row(
        longitude=float(fields[0]),
        latitude=float(fields[1]),
        housingMedianAge=float(fields[2]),
        totalRooms=float(fields[3]),
        totalBedrooms=float(fields[4]),
        population=float(fields[5]),
        households=float(fields[6]),
        medianIncome=float(fields[7]),
        medianHouseValue=float(fields[8]),
    )


def log_data_frame(tag: str, df_data: DataFrame, row: int = 5, print_schema: bool = False,
                   truncate: Union[int, bool] = True):
    df_data.cache()
    logger.info("{}({})".format(tag, df_data.count()))
    df_data.show(row, truncate=truncate)
    if print_schema:
        df_data.printSchema()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('main')
    main()
