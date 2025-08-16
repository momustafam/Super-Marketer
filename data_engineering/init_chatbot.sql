-- Optimized Chatbot Database Schema
-- Version: 2.0
-- Description: Enhanced schema with proper indexing, normalization, and constraints

-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- Create table: users
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE, -- External UUID for API references
    name TEXT NOT NULL CHECK(length(name) >= 2),
    email TEXT NOT NULL UNIQUE CHECK(email LIKE '%@%.%'),
    avatar_url TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table: chat_sessions
CREATE TABLE chat_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE, -- External UUID for API references
    user_id INTEGER NOT NULL,
    title TEXT CHECK(length(title) <= 255),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    message_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_message_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Create table: messages
CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE, -- External UUID for API references
    chat_session_id INTEGER NOT NULL,
    sender_type TEXT NOT NULL CHECK(sender_type IN ('user', 'assistant', 'system')),
    content TEXT,
    content_type TEXT DEFAULT 'text' CHECK(content_type IN ('text', 'image', 'file', 'mixed')),
    token_count INTEGER DEFAULT 0,
    processing_time_ms INTEGER,
    status TEXT DEFAULT 'sent' CHECK(status IN ('draft', 'sending', 'sent', 'delivered', 'failed', 'deleted')),
    metadata TEXT, -- JSON string for additional message metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (chat_session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE
);

-- Create table: message_images (normalized image storage)
CREATE TABLE message_images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    image_url TEXT NOT NULL,
    thumbnail_url TEXT,
    imagekit_file_id TEXT, -- ImageKit file ID for management
    file_name TEXT,
    file_size INTEGER,
    width INTEGER,
    height INTEGER,
    alt_text TEXT,
    upload_status TEXT DEFAULT 'uploaded' CHECK(upload_status IN ('uploading', 'uploaded', 'failed', 'deleted')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);

-- Create table: chat_participants (for future multi-user chats)
CREATE TABLE chat_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_session_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    role TEXT DEFAULT 'member' CHECK(role IN ('owner', 'admin', 'member', 'viewer')),
    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    left_at TIMESTAMP,
    FOREIGN KEY (chat_session_id) REFERENCES chat_sessions(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(chat_session_id, user_id)
);

-- Create indexes for performance optimization
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_uuid ON users(uuid);
CREATE INDEX idx_users_active ON users(is_active, last_active_at);

CREATE INDEX idx_chat_sessions_user_id ON chat_sessions(user_id);
CREATE INDEX idx_chat_sessions_uuid ON chat_sessions(uuid);
CREATE INDEX idx_chat_sessions_active ON chat_sessions(is_active, updated_at);
CREATE INDEX idx_chat_sessions_last_message ON chat_sessions(last_message_at DESC);

CREATE INDEX idx_messages_chat_session ON messages(chat_session_id, created_at);
CREATE INDEX idx_messages_uuid ON messages(uuid);
CREATE INDEX idx_messages_sender_type ON messages(sender_type);
CREATE INDEX idx_messages_status ON messages(status);
CREATE INDEX idx_messages_created_at ON messages(created_at DESC);

CREATE INDEX idx_message_images_message_id ON message_images(message_id);
CREATE INDEX idx_message_images_imagekit_id ON message_images(imagekit_file_id);

CREATE INDEX idx_chat_participants_session ON chat_participants(chat_session_id);
CREATE INDEX idx_chat_participants_user ON chat_participants(user_id);

-- Create triggers for automatic timestamp updates
CREATE TRIGGER update_users_timestamp 
    AFTER UPDATE ON users
    BEGIN
        UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;

CREATE TRIGGER update_chat_sessions_timestamp 
    AFTER UPDATE ON chat_sessions
    BEGIN
        UPDATE chat_sessions SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;

CREATE TRIGGER update_messages_timestamp 
    AFTER UPDATE ON messages
    BEGIN
        UPDATE messages SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
    END;

-- Create trigger to update chat session stats when messages are added
CREATE TRIGGER update_chat_session_stats 
    AFTER INSERT ON messages
    BEGIN
        UPDATE chat_sessions 
        SET 
            message_count = message_count + 1,
            last_message_at = NEW.created_at,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = NEW.chat_session_id;
    END;

-- Create trigger to update chat session stats when messages are deleted
CREATE TRIGGER update_chat_session_stats_delete 
    AFTER DELETE ON messages
    BEGIN
        UPDATE chat_sessions 
        SET 
            message_count = message_count - 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = OLD.chat_session_id;
    END;

-- Create views for common queries
CREATE VIEW v_active_chat_sessions AS
SELECT 
    cs.id,
    cs.uuid,
    cs.title,
    cs.message_count,
    cs.created_at,
    cs.last_message_at,
    u.name as user_name,
    u.email as user_email
FROM chat_sessions cs
JOIN users u ON cs.user_id = u.id
WHERE cs.is_active = TRUE
ORDER BY cs.last_message_at DESC;

CREATE VIEW v_chat_messages_with_images AS
SELECT 
    m.id,
    m.uuid,
    m.chat_session_id,
    m.sender_type,
    m.content,
    m.content_type,
    m.status,
    m.created_at,
    GROUP_CONCAT(mi.image_url) as image_urls,
    COUNT(mi.id) as image_count
FROM messages m
LEFT JOIN message_images mi ON m.id = mi.message_id
GROUP BY m.id
ORDER BY m.created_at;

-- Insert sample data with UUIDs
INSERT INTO users (id, uuid, name, email)
VALUES (1, 'user-550e8400-e29b-41d4-a716-446655440000', 'Mahmoud Afify', 'mahmoudafify@gmail.com');