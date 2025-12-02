from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import os
import uuid
from datetime import datetime
import logging
from dotenv import load_dotenv
from kafka_producer import kafka_producer

# Загружаем переменные окружения
load_dotenv()

# Создаем приложение API
app = FastAPI(title="Video Upload API")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Настройки
UPLOAD_DIR = os.getenv('UPLOAD_DIR', 'uploads')
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE_GB', 3)) * 1024 * 1024 * 1024  # 3GB
ALLOWED_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.wmv'}

# Создаем директорию для загрузок если ее нет
os.makedirs(UPLOAD_DIR, exist_ok=True)

def validate_video_file(file: UploadFile):
    """Проверяет файл видео на соответствие требованиям"""
    
    # Проверка расширения файла
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400, 
            detail=f"Недопустимый формат файла. Разрешенные форматы: {', '.join(ALLOWED_EXTENSIONS)}"
        )
    
    # Проверка размера файла
    file.file.seek(0, 2)  # Перемещаемся в конец файла
    file_size = file.file.tell()
    file.file.seek(0)  # Возвращаемся в начало
    
    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"Размер файла превышает {os.getenv('MAX_FILE_SIZE_GB', 3)}GB. Текущий размер: {file_size / (1024*1024*1024):.2f}GB"
        )
    
    return file_ext, file_size

@app.post("/upload-video/")
async def upload_video(file: UploadFile = File(...)):
    """
    Загрузка видео файла через API
    """
    try:
        logger.info(f"📥 API upload request for file: {file.filename}")
        
        # Валидация файла
        file_ext, file_size = validate_video_file(file)
        
        # Генерируем уникальное имя файла
        unique_filename = f"{uuid.uuid4()}{file_ext}"
        file_path = os.path.join(UPLOAD_DIR, unique_filename)
        
        # Сохраняем файл
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        logger.info(f"💾 File saved: {file_path} ({file_size} bytes)")
        
        # Подготавливаем данные для Kafka
        video_data = {
            "video_id": str(uuid.uuid4()),
            "filename": unique_filename,
            "original_filename": file.filename,
            "file_path": file_path,
            "file_size": file_size,
            "file_extension": file_ext,
            "upload_timestamp": datetime.now().isoformat(),
            "upload_source": "api"
        }
        
        # Отправляем событие в Kafka
        logger.info(f"📤 Sending to Kafka...")
        kafka_success = kafka_producer.send_video_upload_event(video_data)
        
        logger.info(f"✅ Upload completed. Kafka sent: {kafka_success}")
        
        return JSONResponse({
            "status": "success",
            "message": "Video uploaded successfully",
            "video_id": video_data["video_id"],
            "filename": unique_filename,
            "file_size": file_size,
            "kafka_sent": kafka_success,
            "download_url": f"/api/uploads/{unique_filename}"
        })
        
    except HTTPException as e:
        logger.error(f"❌ HTTP error: {e.detail}")
        raise e
    except Exception as e:
        logger.error(f"❌ Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.get("/uploads/{filename}")
async def download_file(filename: str):
    """Скачать загруженный файл"""
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    from fastapi.responses import FileResponse
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/octet-stream'
    )

@app.get("/")
async def api_root():
    return {
        "service": "Video Upload API",
        "version": "1.0.0",
        "endpoints": {
            "upload": "POST /upload-video/",
            "download": "GET /uploads/{filename}"
        }
    }

@app.get("/health")
async def api_health():
    return {
        "status": "healthy", 
        "service": "video-upload-api",
        "kafka_connected": kafka_producer.producer is not None
    }
