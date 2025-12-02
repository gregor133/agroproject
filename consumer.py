from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

load_dotenv()

class VideoProcessor:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'video_upload_topic')
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='video-processing-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def process_video(self, video_data):
        """Обрабатывает загруженное видео"""
        print(f"Processing video: {video_data['original_filename']}")
        print(f"Video ID: {video_data['video_id']}")
        print(f"File path: {video_data['file_path']}")
        print(f"Size: {video_data['file_size']} bytes")
        print(f"Uploaded via: {video_data['upload_source']}")
        print("-" * 50)
        
        # Здесь можно добавить логику обработки видео:
        # - Конвертация форматов
        # - Извлечение метаданных
        # - Генерация превью
        # - Анализ контента
        # - и т.д.
        
        # Пример: просто проверяем существование файла
        if os.path.exists(video_data['file_path']):
            print(f"File exists, ready for processing")
        else:
            print(f"File not found: {video_data['file_path']}")
    
    def start_consuming(self):
        """Начинает потребление сообщений из Kafka"""
        print(f"Starting video processor. Listening to topic: {self.topic}")
        
        try:
            for message in self.consumer:
                video_data = message.value
                print(f"Received video upload event from {video_data['upload_source']}")
                self.process_video(video_data)
                
        except KeyboardInterrupt:
            print("Stopping consumer...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    processor = VideoProcessor()
    processor.start_consuming()
