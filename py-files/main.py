from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf().setMaster('local').setAppName('my-app')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    num_samples = 100000
    count = sc.parallelize(range(0, num_samples)).filter(test_inside).count()
    print("Pi is roughly {}".format(4.0 * count / num_samples))


def test_inside(p):
    # noinspection PyUnresolvedReferences
    import util
    return util.inside()


if __name__ == '__main__':
    main()
