from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName('database').getOrCreate()

    df_customers = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/db_example",
        driver="com.mysql.jdbc.Driver",
        dbtable="Customers",
        user="user",
        password="password"
    ).load()

    df_customers.show()

    spark.stop()


if __name__ == '__main__':
    main()
