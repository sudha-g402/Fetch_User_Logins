from kafka import KafkaConsumer,KafkaProducer
import json
import pandas as pd
import time
from datetime import datetime

# Kafka consumer configuration
KAFKA_BROKER = 'localhost:29092'
TOPIC_NAME = 'user-login'
OUTPUT_TOPIC = 'transformed-data'
Process_Interval_Seconds = 300

def fetch_data(consumer):
    data_list = []
    start_time = time.time()
    while time.time() - start_time < Process_Interval_Seconds:
        message = consumer.poll(timeout_ms=10000)  # Listen for messages for 10 secs as it produces every 10 sec
        if message is None:
            return []
        for tp, records in message.items():
            for record in records:
                data_list.append(record.value)
    return data_list


def process_data(data):
    """Transform data and return insights."""
    df = pd.DataFrame(data)
    #DATA CLEANSING
    #Drop Duplicates
    df = df.drop_duplicates()

    #Drop records with missing 'user_id', 'device_id' details
    df = df.dropna(subset=['user_id', 'device_id'])

    #Convert timestamp to datetime to perform desired aggregations
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')

    print(df)
    #DATA TRANSFORMATIONS
    # Perform transformations
    insights = {
        "active_users_by_time": {
            "hourly": active_users_per_hour(df).to_dict(orient='records'),
            "daily": active_users_per_day(df).to_dict(orient='records'),
        },
        "app_version_distribution": app_version_distribution(df).to_dict(orient='records'),
        "locale_distribution": locale_distribution(df).to_dict(orient='records'),
        "device_type_distribution": device_type_insights(df).to_dict(orient='records'),
        "fraud_device_usage_analysis": fraud_device_usage_analysis(df).to_dict(orient='records')
    }
    return insights


def fraud_device_usage_analysis(df):
    """Analyse single device usage by multiple users"""
    usr_cnts_per_device = df.groupby('device_id')['user_id'].nunique().reset_index()
    users_per_device_df = usr_cnts_per_device[usr_cnts_per_device['user_id'] > 1]
    users_per_device_df.columns = ['device_id', 'user_count']
    return users_per_device_df


def active_users_per_hour(df):
    """Hourly Usage Distribution by aggregating timestamp field"""
    df['Hour'] = df['datetime'].dt.floor('h')
    hourly_counts = df.groupby('Hour').size().reset_index(name='user_count')
    return hourly_counts


def active_users_per_day(df):
    """Daily Usage Distribution by aggregating timestamp field"""
    df['Day'] = df['datetime'].dt.date
    daily_counts = df.groupby('Day').size().reset_index(name='usage_count')
    return daily_counts


def locale_distribution(df):
    """App usage by location"""
    return df['locale'].value_counts().reset_index(name='usage_count')


def app_version_distribution(df):
    """App Version usage distribution"""
    return df['app_version'].value_counts().reset_index(name='usage_count')


def device_type_insights(df):
    """Usage distribution among different devices"""
    return df['device_type'].value_counts().reset_index(name='usage_count')


def push_to_kafka(producer, topic, data):
    """Send transformed data to Kafka."""
    try:
        producer.send(topic, value=json.dumps(data).encode('utf-8'))  # Convert to JSON and send
        producer.flush()  # Ensure all messages are sent
        print(f"Sent insights to Kafka topic '{topic}'.")
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")


def custom_serializer(obj):
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    if isinstance(obj, datetime.date):
        return obj.strftime("%Y-%m-%d")
    raise TypeError("Type not serializable")


def main():
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print("Kafka Consumer started. Listening for messages...")
    while True:
        # Extract data
        raw_data = fetch_data(consumer)
        if len(raw_data) == 0:
            break
        print(f"data extracted: {len(raw_data)}")
        if not raw_data:
            print("No new data in this interval.")
            continue
        # # Transform data
        insights = process_data(raw_data)
        print(insights)
        # push transformed data to Kafka
        # push_to_kafka(producer, OUTPUT_TOPIC, insights)
        # print(f"Transformed data sent to Kafka topic '{OUTPUT_TOPIC}'.")

if __name__ == "__main__":
    main()
