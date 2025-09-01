from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from imagekitio import ImageKit
import base64
import uuid
import uvicorn
import sqlite3
from typing import List, Optional
from contextlib import contextmanager
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt
from rag import RAGSystem

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_PATH = "/data/chatbot.db"  # Path inside Docker container
# Fallback for local development
if not os.path.exists(DATABASE_PATH):
    DATABASE_PATH = "chatbot.db"

# Database connection helper
@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
    try:
        yield conn
    finally:
        conn.close()

# Pydantic models for request/response
class ImageData(BaseModel):
    url: str
    thumbnail_url: Optional[str] = None
    file_id: str
    fileName: str
    width: Optional[int] = None
    height: Optional[int] = None

class MessageCreate(BaseModel):
    text: Optional[str] = ""
    images: List[ImageData] = []

class MessageResponse(BaseModel):
    id: str
    chat_session_id: str
    sender_type: str
    content: Optional[str]
    content_type: str
    status: str
    images: List[ImageData] = []
    created_at: str

class ChatSessionCreate(BaseModel):
    title: Optional[str] = None
    first_message: Optional[MessageCreate] = None

class ChatSessionResponse(BaseModel):
    id: str
    title: Optional[str]
    message_count: int
    created_at: str
    last_message_at: Optional[str]
    messages: List[MessageResponse] = []

class ChatListResponse(BaseModel):
    chats: List[ChatSessionResponse]
    total: int

app = FastAPI(title="Chatbot Backend API", version="2.0.0")

# CORS middleware to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:8001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ImageKit configuration using environment variables
IMAGEKIT_PRIVATE_KEY = os.getenv("REACT_APP_IMAGEKIT_PRIVATE_KEY")
IMAGEKIT_PUBLIC_KEY = os.getenv("REACT_APP_IMAGEKIT_PUBLIC_KEY")
IMAGEKIT_URL_ENDPOINT = os.getenv("REACT_APP_IMAGEKIT_ENDPOINT")

# Validate that all required environment variables are present
if not all([IMAGEKIT_PRIVATE_KEY, IMAGEKIT_PUBLIC_KEY, IMAGEKIT_URL_ENDPOINT]):
    raise ValueError("Missing required ImageKit environment variables. Please check your .env file.")

# Initialize ImageKit
imagekit = ImageKit(
    private_key=IMAGEKIT_PRIVATE_KEY,
    public_key=IMAGEKIT_PUBLIC_KEY,
    url_endpoint=IMAGEKIT_URL_ENDPOINT
)

# LLM / Groq configuration (renamed from Grok)
GROQ_API_KEY = os.getenv("GROQ_API_KEY") or os.getenv("GROK_API_KEY")  # backward compat
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama3-70b-8192")
GROQ_API_BASE = os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1")
GROQ_SYSTEM_PROMPT = os.getenv(
    "GROQ_SYSTEM_PROMPT",
    os.getenv("GROK_SYSTEM_PROMPT", "You are Super Marketer AI, a senior marketing & growth strategist. Provide clear, actionable, data-driven guidance. If images are provided, incorporate them. Be concise and professional.")
)

if not GROQ_API_KEY:
    print("[WARN] GROQ_API_KEY not set – AI responses will use fallback template.")

# Initialize RAG System
rag_system = None
if GROQ_API_KEY:
    try:
        rag_system = RAGSystem(GROQ_API_KEY, GROQ_MODEL)
        print("[INFO] RAG System initialized successfully")
    except Exception as e:
        print(f"[WARN] RAG System initialization failed: {e}")
        rag_system = None

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Chatbot Backend API v2.0 is running"}

# Helper functions for database operations
def get_default_user_id():
    """Get the default user ID (first user in the database)"""
    with get_db() as conn:
        cursor = conn.execute("SELECT id FROM users LIMIT 1")
        row = cursor.fetchone()
        return row[0] if row else None

def create_default_user_if_not_exists():
    """Create a default user if none exists"""
    with get_db() as conn:
        cursor = conn.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        
        if count == 0:
            user_uuid = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO users (uuid, name, email) VALUES (?, ?, ?)",
                (user_uuid, "Default User", "user@example.com")
            )
            conn.commit()

# Initialize default user on startup
@app.on_event("startup")
async def startup_event():
    create_default_user_if_not_exists()

@app.get("/api/chats")
async def get_chats(limit: int = 50, offset: int = 0):
    """
    Get list of chat sessions for the user
    
    Args:
        limit: Maximum number of chats to return (default: 50)
        offset: Number of chats to skip (default: 0)
        
    Returns:
        JSON response with list of chat sessions
    """
    try:
        user_id = get_default_user_id()
        if not user_id:
            raise HTTPException(status_code=404, detail="No user found")

        with get_db() as conn:
            # Get chat sessions with message count and last message time
            cursor = conn.execute("""
                SELECT 
                    cs.uuid,
                    cs.title,
                    cs.message_count,
                    cs.created_at,
                    cs.last_message_at
                FROM chat_sessions cs
                WHERE cs.user_id = ? AND cs.is_active = TRUE
                ORDER BY cs.last_message_at DESC, cs.created_at DESC
                LIMIT ? OFFSET ?
            """, (user_id, limit, offset))
            
            chats = []
            for row in cursor.fetchall():
                chat = {
                    "id": row["uuid"],
                    "title": row["title"] or "New Chat",
                    "message_count": row["message_count"],
                    "created_at": row["created_at"],
                    "last_message_at": row["last_message_at"],
                    "messages": []
                }
                chats.append(chat)

            # Get total count
            total_cursor = conn.execute(
                "SELECT COUNT(*) FROM chat_sessions WHERE user_id = ? AND is_active = TRUE",
                (user_id,)
            )
            total = total_cursor.fetchone()[0]

            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": {
                        "chats": chats,
                        "total": total
                    }
                }
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching chats: {str(e)}")

@app.post("/api/chats")
async def create_chat(chat_data: MessageCreate):
    """
    Create a new chat session with an initial message
    
    Args:
        chat_data: The initial message data
        
    Returns:
        JSON response with the created chat session and AI response
    """
    try:
        user_id = get_default_user_id()
        if not user_id:
            raise HTTPException(status_code=404, detail="No user found")

        with get_db() as conn:
            # Create new chat session
            chat_uuid = str(uuid.uuid4())
            
            # Generate title from first message or use default
            title = chat_data.text[:50] + "..." if chat_data.text and len(chat_data.text) > 50 else chat_data.text or "New Chat"
            
            cursor = conn.execute("""
                INSERT INTO chat_sessions (uuid, user_id, title, created_at, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (chat_uuid, user_id, title))
            
            chat_session_id = cursor.lastrowid
            
            # Create user message
            user_message_uuid = str(uuid.uuid4())
            content_type = "mixed" if chat_data.images else "text"
            
            conn.execute("""
                INSERT INTO messages (uuid, chat_session_id, sender_type, content, content_type, created_at)
                VALUES (?, ?, 'user', ?, ?, CURRENT_TIMESTAMP)
            """, (user_message_uuid, chat_session_id, chat_data.text, content_type))
            
            user_message_id = cursor.lastrowid
            
            # Add images if any
            for image in chat_data.images:
                conn.execute("""
                    INSERT INTO message_images 
                    (message_id, image_url, thumbnail_url, imagekit_file_id, file_name, width, height)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_message_id, image.url, image.thumbnail_url, 
                    image.file_id, image.fileName, image.width, image.height
                ))
            
            # Generate AI response (now async LLM)
            ai_message_uuid = str(uuid.uuid4())
            ai_response = await generate_ai_response(conn, chat_session_id, chat_data.text, chat_data.images)

            conn.execute("""
                INSERT INTO messages (uuid, chat_session_id, sender_type, content, content_type, created_at)
                VALUES (?, ?, 'assistant', ?, 'text', CURRENT_TIMESTAMP)
            """, (ai_message_uuid, chat_session_id, ai_response))
            # Update chat session counters
            conn.execute("""
                UPDATE chat_sessions
                SET message_count = message_count + 2, last_message_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (chat_session_id,))

            conn.commit()
            
            return JSONResponse(
                status_code=201,
                content={
                    "success": True,
                    "data": {
                        "chat_id": chat_uuid,
                        "message": "Chat created successfully",
                        "ai_response": ai_response
                    }
                }
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating chat: {str(e)}")

# Replace simple placeholder with full LLM integration
async def generate_ai_response(conn, chat_session_id: int, user_text: str, images: List[ImageData]) -> str:
    """Generate an AI response using RAG System or fallback to simple LLM.
    
    Args:
        conn: Active sqlite3 connection (for reading prior messages)
        chat_session_id: Internal chat session numeric id
        user_text: Current user message text
        images: List of ImageData objects
    Returns:
        AI assistant response string
    """
    
    # If RAG system is available, use it
    if rag_system and GROQ_API_KEY:
        try:
            # Build conversation history for context
            conversation_history = []
            try:
                cursor = conn.execute("""
                    SELECT sender_type, content FROM messages
                    WHERE chat_session_id = ?
                    ORDER BY created_at ASC, id ASC
                """, (chat_session_id,))
                history_rows = cursor.fetchall()
                
                # Convert to conversation format
                for row in history_rows[-10:]:  # Keep last 10 for context
                    role = "user" if row[0] == "user" else "assistant"
                    content = row[1] or ""
                    if content.strip():
                        conversation_history.append({"role": role, "content": content})
            except Exception as e:
                print(f"[WARN] Could not retrieve conversation history: {e}")
                conversation_history = []
            
            # Prepare user message with image references
            current_message = user_text or ""
            if images:
                img_section = "\nAttached images:\n" + "\n".join([f"Image {i+1}: {img.url}" for i, img in enumerate(images)])
                current_message += img_section
            
            # Use RAG system to process the query
            response = await rag_system.process_query(current_message, conversation_history)
            return response
            
        except Exception as e:
            print(f"[ERROR] RAG system failed: {e}")
            # Fall through to simple LLM fallback
    
    # Original fallback implementation
    # Build conversation history (limit last ~20 messages + current)
    try:
        cursor = conn.execute("""
            SELECT sender_type, content FROM messages
            WHERE chat_session_id = ?
            ORDER BY created_at ASC, id ASC
        """, (chat_session_id,))
        history_rows = cursor.fetchall()
    except Exception:
        history_rows = []

    messages_payload = []

    # System prompt
    messages_payload.append({"role": "system", "content": GROQ_SYSTEM_PROMPT})

    # Convert existing history
    for row in history_rows[-20:]:  # keep last 20 for brevity
        role = "user" if row[0] == "user" else "assistant"
        content = row[1] or ""
        if content.strip():
            messages_payload.append({"role": role, "content": content})

    # Add current user message with inline image references (if not already in history)
    if user_text or images:
        img_section = ""
        if images:
            img_lines = [f"Image {i+1}: {img.url}" for i, img in enumerate(images)]
            img_section = "\nAttached images:\n" + "\n".join(img_lines)
        messages_payload.append({"role": "user", "content": (user_text or "(no text)") + img_section})

    # If no API key, fallback
    if not GROQ_API_KEY:
        if images:
            return f"(Fallback) {len(images)} image(s) noted. You said: '{user_text}'. Provide more details so I can assist with marketing insights."
        return f"(Fallback) Thanks for your message: '{user_text}'. Add more context (goals, audience, channel) for targeted marketing recommendations."

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    async def call_groq():
        url = f"{GROQ_API_BASE.rstrip('/')}/chat/completions"
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": GROQ_MODEL,
            "messages": messages_payload,
            "temperature": 0.7,
        }
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code >= 400:
                raise HTTPException(status_code=500, detail=f"Groq API error {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content")
            if not content:
                content = "I'm unable to generate a response right now. Please try again."
            return content.strip()

    try:
        return await call_groq()
    except HTTPException:
        raise
    except Exception as e:
        print(f"[ERROR] Groq call failed: {e}")
        if images:
            return f"I noted {len(images)} image(s). Based on your input, could you provide any KPIs, target audience details, or campaign objectives so I can craft a stronger strategy?"
        return "Could you provide a bit more strategic context (objective, audience, channel)? I'll tailor a marketing plan once I have that."

@app.get("/api/chats/{chat_id}")
async def get_chat(chat_id: str):
    """
    Get a specific chat session with all messages
    
    Args:
        chat_id: The UUID of the chat session
        
    Returns:
        JSON response with chat details and messages
    """
    try:
        user_id = get_default_user_id()
        if not user_id:
            raise HTTPException(status_code=404, detail="No user found")

        with get_db() as conn:
            # Get chat session
            cursor = conn.execute("""
                SELECT uuid, title, message_count, created_at, last_message_at
                FROM chat_sessions 
                WHERE uuid = ? AND user_id = ? AND is_active = TRUE
            """, (chat_id, user_id))
            
            chat_row = cursor.fetchone()
            if not chat_row:
                raise HTTPException(status_code=404, detail="Chat not found")
            
            # Get messages with images
            messages_cursor = conn.execute("""
                SELECT 
                    m.uuid,
                    m.sender_type,
                    m.content,
                    m.content_type,
                    m.status,
                    m.created_at,
                    GROUP_CONCAT(mi.image_url) as image_urls,
                    GROUP_CONCAT(mi.thumbnail_url) as thumbnail_urls,
                    GROUP_CONCAT(mi.imagekit_file_id) as file_ids,
                    GROUP_CONCAT(mi.file_name) as file_names,
                    GROUP_CONCAT(mi.width) as widths,
                    GROUP_CONCAT(mi.height) as heights
                FROM messages m
                LEFT JOIN message_images mi ON m.id = mi.message_id
                WHERE m.chat_session_id = (
                    SELECT id FROM chat_sessions WHERE uuid = ?
                )
                GROUP BY m.id
                ORDER BY m.created_at
            """, (chat_id,))
            
            messages = []
            for msg_row in messages_cursor.fetchall():
                images = []
                if msg_row["image_urls"]:
                    urls = msg_row["image_urls"].split(",")
                    thumbnails = (msg_row["thumbnail_urls"] or "").split(",")
                    file_ids = (msg_row["file_ids"] or "").split(",")
                    file_names = (msg_row["file_names"] or "").split(",")
                    widths = (msg_row["widths"] or "").split(",")
                    heights = (msg_row["heights"] or "").split(",")
                    
                    for i, url in enumerate(urls):
                        if url:  # Skip empty URLs
                            images.append({
                                "url": url,
                                "thumbnail_url": thumbnails[i] if i < len(thumbnails) else url,
                                "file_id": file_ids[i] if i < len(file_ids) else "",
                                "fileName": file_names[i] if i < len(file_names) else "",
                                "width": int(widths[i]) if i < len(widths) and widths[i].isdigit() else None,
                                "height": int(heights[i]) if i < len(heights) and heights[i].isdigit() else None
                            })
                
                messages.append({
                    "id": msg_row["uuid"],
                    "role": "user" if msg_row["sender_type"] == "user" else "ai",
                    "content": msg_row["content"],
                    "images": images,
                    "created_at": msg_row["created_at"]
                })
            
            chat_data = {
                "id": chat_row["uuid"],
                "title": chat_row["title"],
                "message_count": chat_row["message_count"],
                "created_at": chat_row["created_at"],
                "last_message_at": chat_row["last_message_at"],
                "messages": messages
            }
            
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": chat_data
                }
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving chat: {str(e)}")

@app.post("/api/chats/{chat_id}/messages")
async def add_message_to_chat(chat_id: str, message_data: MessageCreate):
    """
    Add a new message to an existing chat session
    
    Args:
        chat_id: The UUID of the chat session
        message_data: The message data including text and images
        
    Returns:
        JSON response with the new message and AI response
    """
    try:
        user_id = get_default_user_id()
        if not user_id:
            raise HTTPException(status_code=404, detail="No user found")

        with get_db() as conn:
            # Verify chat exists and belongs to user
            cursor = conn.execute("""
                SELECT id FROM chat_sessions 
                WHERE uuid = ? AND user_id = ? AND is_active = TRUE
            """, (chat_id, user_id))
            
            chat_row = cursor.fetchone()
            if not chat_row:
                raise HTTPException(status_code=404, detail="Chat not found")
            
            chat_session_id = chat_row[0]
            
            # Create user message
            user_message_uuid = str(uuid.uuid4())
            content_type = "mixed" if message_data.images else "text"
            
            cursor = conn.execute("""
                INSERT INTO messages (uuid, chat_session_id, sender_type, content, content_type, created_at)
                VALUES (?, ?, 'user', ?, ?, CURRENT_TIMESTAMP)
            """, (user_message_uuid, chat_session_id, message_data.text, content_type))
            
            user_message_id = cursor.lastrowid
            
            # Add images if any
            for image in message_data.images:
                conn.execute("""
                    INSERT INTO message_images 
                    (message_id, image_url, thumbnail_url, imagekit_file_id, file_name, width, height)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_message_id, image.url, image.thumbnail_url, 
                    image.file_id, image.fileName, image.width, image.height
                ))
            
            # Generate AI response (now async LLM)
            ai_message_uuid = str(uuid.uuid4())
            ai_response = await generate_ai_response(conn, chat_session_id, message_data.text, message_data.images)

            conn.execute("""
                INSERT INTO messages (uuid, chat_session_id, sender_type, content, content_type, created_at)
                VALUES (?, ?, 'assistant', ?, 'text', CURRENT_TIMESTAMP)
            """, (ai_message_uuid, chat_session_id, ai_response))
            # Update chat session counters
            conn.execute("""
                UPDATE chat_sessions
                SET message_count = message_count + 2, last_message_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (chat_session_id,))

            conn.commit()
            
            return JSONResponse(
                status_code=201,
                content={
                    "success": True,
                    "data": {
                        "user_message_id": user_message_uuid,
                        "ai_message_id": ai_message_uuid,
                        "ai_response": ai_response
                    }
                }
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error adding message: {str(e)}")
@app.post("/upload-image")
async def upload_image(file: UploadFile = File(...)):
    """
    Upload an image file to ImageKit and return the URL
    
    Args:
        file: The image file to upload
        
    Returns:
        JSON response with the uploaded image URL and metadata
    """
    try:
        # Validate file type
        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="File must be an image")
        
        # Read file content
        file_content = await file.read()
        file_size = len(file_content)
        
        # Validate file size (max 10MB)
        if file_size > 10 * 1024 * 1024:  # 10MB
            raise HTTPException(status_code=400, detail="File size must be less than 10MB")
        
        # Generate unique filename
        file_extension = file.filename.split('.')[-1] if file.filename and '.' in file.filename else 'jpg'
        unique_filename = f"{uuid.uuid4().hex}.{file_extension}"
        
        # Convert file content to base64
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        
        # Upload the actual file to ImageKit
        upload = imagekit.upload_file(
            file=file_base64,
            file_name=unique_filename,
            options={
                "folder": "/chat_images",
                "use_unique_file_name": True,
                "tags": ["chat", "user_upload"]
            }
        )
        
        # Return the upload result
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Image uploaded successfully",
                "data": {
                    "file_id": getattr(upload, 'file_id', ''),
                    "name": getattr(upload, 'name', unique_filename),
                    "url": getattr(upload, 'url', ''),
                    "thumbnail_url": getattr(upload, 'thumbnail_url', getattr(upload, 'url', '')),
                    "file_path": getattr(upload, 'file_path', ''),
                    "size": getattr(upload, 'size', file_size),
                    "file_type": getattr(upload, 'file_type', file.content_type),
                    "width": getattr(upload, 'width', None),
                    "height": getattr(upload, 'height', None)
                }
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error uploading image: {str(e)}")


# Helper to initialize DB from SQL file
def initialize_db_from_sql(sql_path):
    if not os.path.exists(sql_path):
        print(f"[WARN] SQL file not found: {sql_path}")
        return
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()
    with get_db() as conn:
        try:
            conn.executescript(sql_script)
            conn.commit()
            print(f"[INFO] Ran SQL init from {sql_path}")
        except Exception as e:
            print(f"[ERROR] Failed to run SQL init: {e}")


if __name__ == "__main__":
    # Run DB initialization from SQL file before starting server
    sql_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_engineering", "init_chatbot.sql")
    initialize_db_from_sql(sql_path)
    uvicorn.run(app, host="0.0.0.0", port=8001)