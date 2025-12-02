from fastapi import FastAPI, Request, Form, File, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os
import uuid
from datetime import datetime
from kafka_producer import kafka_producer

app = FastAPI(title="Video Upload GUI")

# Настройки
UPLOAD_DIR = "uploads"
MAX_FILE_SIZE = 3 * 1024 * 1024 * 1024  # 3GB
ALLOWED_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.wmv'}

# Создаем директорию для статических файлов и загрузок
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs("static", exist_ok=True)

# Настройка шаблонов
templates = Jinja2Templates(directory="templates")

def validate_video_file_gui(file: UploadFile, file_size: int = None):
    """Проверяет файл видео для GUI"""
    
    # Проверка расширения файла
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in ALLOWED_EXTENSIONS:
        return False, f"Недопустимый формат файла. Разрешенные форматы: {', '.join(ALLOWED_EXTENSIONS)}"
    
    # Если размер не передан, вычисляем его
    if file_size is None:
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
    
    # Проверка размера файла
    if file_size > MAX_FILE_SIZE:
        return False, f"Размер файла превышает 3GB"
    
    return True, ""

@app.get("/", response_class=HTMLResponse)
async def upload_form(request: Request):
    """Отображает форму загрузки"""
    return templates.TemplateResponse("upload.html", {"request": request})

@app.post("/upload-video-gui/")
async def upload_video_gui(request: Request, file: UploadFile = File(...)):
    """
    Обработка загрузки видео через GUI
    """
    try:
        # Валидация файла
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
        
        is_valid, error_message = validate_video_file_gui(file, file_size)
        
        if not is_valid:
            return templates.TemplateResponse("upload.html", {
                "request": request,
                "error": error_message
            })
        
        # Получаем расширение файла
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        # Генерируем уникальное имя файла
        unique_filename = f"{uuid.uuid4()}{file_ext}"
        file_path = os.path.join(UPLOAD_DIR, unique_filename)
        
        # Сохраняем файл
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Подготавливаем данные для Kafka
        video_data = {
            "video_id": str(uuid.uuid4()),
            "filename": unique_filename,
            "original_filename": file.filename,
            "file_path": file_path,
            "file_size": file_size,
            "file_extension": file_ext,
            "upload_timestamp": datetime.now().isoformat(),
            "upload_source": "gui"
        }
        
        # Отправляем событие в Kafka
        kafka_success = kafka_producer.send_video_upload_event(video_data)
        
        return templates.TemplateResponse("upload.html", {
            "request": request,
            "success": "Video uploaded successfully!",
            "video_id": video_data["video_id"],
            "filename": unique_filename
        })
        
    except Exception as e:
        return templates.TemplateResponse("upload.html", {
            "request": request,
            "error": f"Upload failed: {str(e)}"
        })

@app.on_event("shutdown")
def shutdown_event():
    """Закрываем соединение с Kafka при завершении работы"""
    kafka_producer.close()
