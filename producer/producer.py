import time
import json
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'logs-raw-data'
LOG_FILE = 'nasa.log'
RATE_PER_SECOND = 5 

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BROKER"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(2, 7, 0)
)

print(f"Kafka Producer Başlatılıyor")
try:
    with open(f"/app/{LOG_FILE}", 'r') as f:
        for line in f:
            message = {'raw_log': line.strip(), 'producer_time': time.time()}
            producer.send(TOPIC_NAME, value=message)
            print(f"Gönderildi: {line.strip()[:50]}...")
            time.sleep(1.0 / RATE_PER_SECOND)
except FileNotFoundError:
    print(f"HATA: Log dosyası bulunamadı: /app/{LOG_FILE}. Konteyner bağlantısını kontrol edin.")
finally:
    producer.flush() 
    print("Log Gönderimi Tamamlandı veya Durduruldu")