from pyspark import SparkConf, SparkContext, SparkFiles


def main():
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    file_path = SparkFiles.get('98-0.txt')
    print("main(): file_path = {}".format(file_path))

    rdd_file = sc.textFile(file_path)
    count = rdd_file\
        .flatMap(lambda line: line.split(' '))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda x, y: x + y)
    print("main(): count = {}".format(count.collect()))


if __name__ == '__main__':
    main()
