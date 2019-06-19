import json
import logging
import time

from kafka import KafkaProducer


def main():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    topic = 'word-count'

    with open('./data/98-0.txt') as f:
        while True:
            line = f.readline()
            if not line:
                break

            line = line.rstrip()
            logger.info("line = {}".format(line))
            producer.send(topic, line.encode('utf-8'))

            try:
                time.sleep(0.5)
            except KeyboardInterrupt:
                logger.info("closing producer")
                producer.close()
                break


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s  %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('main')
    main()
