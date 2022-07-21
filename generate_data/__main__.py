from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9093',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         key_serializer=lambda x: json.dumps(x).encode('utf-8'),
                         )


def sample1():
    sample_lats = [35.703686,
                   35.686806,
                   35.719774,
                   35.716914,
                   35.706258,
                   35.693407,
                   35.668782,
                   35.715623]
    sample_longs = [51.385687,
                    51.435012,
                    51.360006,
                    51.438037,
                    51.371695,
                    51.42686,
                    51.425666,
                    51.376047]
    sample_times = ['2020-10-20 12:54:11.745483',
                    '2020-10-20 12:54:21.747462',
                    '2020-10-20 12:54:21.748287',
                    '2020-10-20 12:54:26.748505',
                    '2020-10-20 12:54:41.752291',
                    '2020-10-20 12:54:41.752665',
                    '2020-10-20 12:54:41.753048',
                    '2020-10-20 12:54:56.755640']
    sample_items = [
        {
            "driverId": 1,
            "time": time,
            "lat": lat,
            "long": lng,
        } for lat, lng, time in zip(sample_lats, sample_longs, sample_times)
    ]
    print(sample_items)
    for item in sample_items:
        print('send')
        producer.send(topic='tehran-locations', key={"driverId": item['driverId']}, value=item)
        producer.flush()


def sample_from_file():
    df = pd.read_csv("../storage/sample_data.txt")
    print(df.to_records())
    sample_items = [
        {
            "driverId": int(driverId),
            "time": time,
            "lat": float(lat),
            "long": float(lng),
        } for _, driverId, lat, lng, time in df.to_records()
    ]
    for item in sample_items[1:2000]:
        print('send')
        print(item)
        producer.send(topic='tehran-locations', key={"driverId": item['driverId']}, value=item)
        producer.flush()


sample1()
# sample_from_file()

# producer.send(topic='tehran-locations', key={"driverId": 1}, value={
#     "driverId": 1,
#     "time": '2020-10-20 12:54:11.745483',
#     "lat": 35.703686,
#     "long": 51.385687,
# })
# producer.flush()
