from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from api_upload import app as api_app
from gui_upload import app as gui_app
import os

# Создаем основное приложение
app = FastAPI(title="Video Upload Server", version="1.0.0")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Монтируем API и GUI приложения
app.mount("/api", api_app)
app.mount("/", gui_app)

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "video-upload-server"}

if __name__ == "__main__":
    # Создаем необходимые директории
    os.makedirs("uploads", exist_ok=True)
    os.makedirs("templates", exist_ok=True)
    
    # Запускаем сервер
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
