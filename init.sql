-- Initialize database for Telegram Bot
-- This script will be executed when the PostgreSQL container starts for the first time

-- Create extension for UUID generation (if needed in future)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create users table (will be created automatically by SQLAlchemy, but this ensures it exists)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    employee_number VARCHAR(50) NOT NULL,
    username VARCHAR(100),
    telegram_id BIGINT UNIQUE NOT NULL
);

-- Create index on telegram_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);

-- Create index on employee_number for faster lookups
CREATE INDEX IF NOT EXISTS idx_users_employee_number ON users(employee_number);

-- Insert some sample data for testing (optional - remove in production)
-- INSERT INTO users (employee_number, username, telegram_id) VALUES 
-- ('EMP001', 'john_doe', 123456789),
-- ('EMP002', 'jane_smith', 987654321)
-- ON CONFLICT (telegram_id) DO NOTHING;