import asyncio
import logging
import pandas as pd
from typing import Optional
from pathlib import Path
import os
from contextlib import asynccontextmanager
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, Document
from aiogram.filters import CommandStart
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, String, select
from sqlalchemy.exc import IntegrityError
from aiogram.types import FSInputFile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "8250207332:AAEL-Mo2QYVf-IJocDfLAFhxLV_")
EXCEL_FILE_PATH = "data/data.xlsx"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@localhost:5432/telegram_bot")
ADMIN_ID = int(os.getenv("ADMIN_ID", "5492521311"))

# Database connection pool settings
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))  # Minimum number of connections in pool
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "30"))  # Maximum overflow connections
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))  # Timeout for getting connection from pool
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))  # Recycle connections after 1 hour

# SQLAlchemy Base
class Base(DeclarativeBase):
    pass

# User model for PostgreSQL
class User(Base):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    employee_number: Mapped[str] = mapped_column(String(50))
    username: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True)

# FSM States
class RegistrationStates(StatesGroup):
    waiting_for_employee_number = State()

class AdminStates(StatesGroup):
    waiting_for_excel_file = State()

# Singleton Database Manager with Connection Pool
class DatabaseManager:
    """Manages PostgreSQL database operations with connection pooling."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, database_url: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, database_url: Optional[str] = None):
        if self._initialized:
            return
            
        if database_url is None:
            raise ValueError("Database URL must be provided on first initialization")
            
        # Create async engine with connection pool
        self.engine = create_async_engine(
            database_url,
            echo=False,
            # Connection pool settings
            pool_size=DB_POOL_SIZE,
            max_overflow=DB_MAX_OVERFLOW,
            pool_timeout=DB_POOL_TIMEOUT,
            pool_recycle=DB_POOL_RECYCLE,
            pool_pre_ping=True,  # Validate connections before use
            # Additional asyncpg settings
            connect_args={
                "server_settings": {
                    "application_name": "telegram_bot",
                }
            }
        )
        
        # Create session factory
        self.async_session = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=True,
            autocommit=False
        )
        
        self._initialized = True
        logger.info(f"Database manager initialized with pool (size={DB_POOL_SIZE}, max_overflow={DB_MAX_OVERFLOW})")
    
    @asynccontextmanager
    async def get_session(self):
        """Context manager for database sessions."""
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    async def init_db(self):
        """Create tables if they don't exist."""
        try:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        """Get user by Telegram ID."""
        try:
            async with self.get_session() as session:
                stmt = select(User).where(User.telegram_id == telegram_id)
                result = await session.execute(stmt)
                return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error fetching user by telegram_id {telegram_id}: {e}")
            return None
    
    async def get_all_users(self) -> list[User]:
        """Get all users from database."""
        try:
            async with self.get_session() as session:
                stmt = select(User)
                result = await session.execute(stmt)
                return list(result.scalars().all())
        except Exception as e:
            logger.error(f"Error fetching all users: {e}")
            return []
    
    async def get_employee_telegram_mapping(self) -> pd.DataFrame:
        """Get employee_number and telegram_id pairs from database as DataFrame."""
        try:
            async with self.get_session() as session:
                stmt = select(User.employee_number, User.telegram_id).order_by(User.id)
                result = await session.execute(stmt)
                rows = result.fetchall()
                
                if not rows:
                    logger.warning("No users found in database for mapping")
                    return pd.DataFrame(columns=['employee_id', 'telegram_id'])
                
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=['employee_id', 'telegram_id'])
                
                # Remove duplicates, keeping the last occurrence
                df = df.drop_duplicates(subset='employee_id', keep='last')
                
                logger.info(f"Retrieved {len(df)} unique employee-telegram mappings")
                return df
                
        except Exception as e:
            logger.error(f"Error fetching employee-telegram mapping: {e}")
            return pd.DataFrame(columns=['employee_id', 'telegram_id'])
    
    async def create_user(self, telegram_id: int, employee_number: str, username: Optional[str] = None) -> bool:
        """Create new user record."""
        try:
            async with self.get_session() as session:
                new_user = User(
                    telegram_id=telegram_id,
                    employee_number=employee_number,
                    username=username
                )
                session.add(new_user)
                # Session will be committed by context manager
                
            logger.info(f"Created new user: telegram_id={telegram_id}, employee_number={employee_number}")
            return True
            
        except IntegrityError:
            logger.warning(f"User with telegram_id {telegram_id} already exists")
            return False
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            return False
    
    async def close(self):
        """Close all connections and dispose engine."""
        try:
            await self.engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")

# Excel Data Manager (optimized)
class ExcelDataManager:
    """Manages Excel data loading and querying operations."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
        self._data_loaded = False
        
    async def load_data(self) -> bool:
        """Load Excel file into memory asynchronously."""
        try:
            # Run pandas operations in executor to avoid blocking
            loop = asyncio.get_event_loop()
            self.df = await loop.run_in_executor(
                None, 
                self._read_excel_sync
            )
            
            # Validate required column exists
            if 'ID' not in self.df.columns:
                raise ValueError("Column 'ID' not found in Excel file")
                
            self._data_loaded = True
            logger.info(f"Successfully loaded {len(self.df)} rows from {self.file_path}")
            return True
            
        except FileNotFoundError:
            logger.error(f"Excel file not found: {self.file_path}")
            return False
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            return False
    
    def _read_excel_sync(self) -> pd.DataFrame:
        """Synchronous Excel reading operation."""
        return pd.read_excel(self.file_path)
    
    async def reload_data(self) -> bool:
        """Reload Excel data from file."""
        self._data_loaded = False
        return await self.load_data()
    
    async def find_user_data(self, telegram_id: int) -> Optional[dict]:
        """Find user data by Telegram ID."""
        if not self._data_loaded or self.df is None:
            logger.warning("Excel data not loaded")
            return None
            
        try:
            # Find row with matching Telegram ID
            user_row = self.df[self.df['ID'] == telegram_id]
            
            if user_row.empty:
                return None
                
            # Convert to dictionary and exclude Telegram ID column
            user_data = user_row.iloc[0].to_dict()
            user_data.pop('ID', None)
            
            return user_data
            
        except Exception as e:
            logger.error(f"Error querying user data: {e}")
            return None
    
    async def merge_with_telegram_data(self, db_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Merge Excel data with database telegram mappings."""
        if not self._data_loaded or self.df is None:
            logger.error("Excel data not loaded for merge")
            return None
        
        try:
            # Run merge operation in executor to avoid blocking
            loop = asyncio.get_event_loop()
            merged_df = await loop.run_in_executor(
                None,
                self._perform_merge_sync,
                self.df.copy(),
                db_df
            )
            
            logger.info(f"Merge completed: {len(merged_df)} rows in result")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error during merge operation: {e}")
            return None
    
    def _perform_merge_sync(self, excel_df: pd.DataFrame, db_df: pd.DataFrame) -> pd.DataFrame:
        """Синхронное объединение: записывает telegram_id из базы в колонку ID Excel."""
        
        if 'Номер сотрудника' not in excel_df.columns:
            raise ValueError("Column 'Номер сотрудника' not found in Excel data")
        
        excel_merge_df = excel_df.copy()
        db_merge_df = db_df.copy()

        # Переименовываем для join
        excel_merge_df.rename(columns={'Номер сотрудника': 'employee_id'}, inplace=True)
        
        # Приводим типы к строке
        excel_merge_df['employee_id'] = excel_merge_df['employee_id'].astype(str).str.strip()
        db_merge_df['employee_id'] = db_merge_df['employee_id'].astype(str).str.strip()

        # Логирование для отладки
        logger.info(f"Excel employee_id dtype: {excel_merge_df['employee_id'].dtype}")
        logger.info(f"DB employee_id dtype: {db_merge_df['employee_id'].dtype}")

        # Джойним, подставляем telegram_id
        merged_df = pd.merge(
            excel_merge_df,
            db_merge_df[['employee_id', 'telegram_id']],
            on='employee_id',
            how='left',
            sort=False
        )

        # Переименовываем обратно
        merged_df.rename(columns={'employee_id': 'Номер сотрудника'}, inplace=True)

        # Перезаписываем колонку ID данными из telegram_id (если они есть)
        if 'ID' in merged_df.columns:
            merged_df['ID'] = merged_df['telegram_id'].combine_first(merged_df['ID'])
            merged_df.drop(columns=['telegram_id'], inplace=True)
        else:
            merged_df.rename(columns={'telegram_id': 'ID'}, inplace=True)

        # Лог статистики
        matched_count = merged_df['ID'].notna().sum()
        total_count = len(merged_df)
        logger.info(f"Merge statistics: {matched_count}/{total_count} records matched with Telegram IDs")

        return merged_df


# Global managers (initialized once)
db_manager = None
data_manager = ExcelDataManager(EXCEL_FILE_PATH)

def is_admin(user_id: int) -> bool:
    """Check if user is admin."""
    return user_id == ADMIN_ID

def get_main_keyboard(is_admin_user: bool = False) -> InlineKeyboardMarkup:
    """Create main inline keyboard with 'My Info' button and admin buttons if applicable."""
    buttons = [
        [InlineKeyboardButton(text="📊 Получить информацию", callback_data="get_my_info")]
    ]
    
    if is_admin_user:
        buttons.append([
            InlineKeyboardButton(text="📥 Upload Data", callback_data="admin_upload_data"),
            InlineKeyboardButton(text="📤 Download Data", callback_data="admin_download_data")
        ])
        buttons.append([
            InlineKeyboardButton(text="🔄 Merge Data", callback_data="admin_merge_data")
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_registration_keyboard(is_admin_user: bool = False) -> InlineKeyboardMarkup:
    """Create registration keyboard with admin buttons if applicable."""
    buttons = [
        [InlineKeyboardButton(text="📝 Зарегистрироваться", callback_data="start_registration")]
    ]
    
    if is_admin_user:
        buttons.append([
            InlineKeyboardButton(text="📥 Upload Data", callback_data="admin_upload_data"),
            InlineKeyboardButton(text="📤 Download Data", callback_data="admin_download_data")
        ])
        buttons.append([
            InlineKeyboardButton(text="🔄 Merge Data", callback_data="admin_merge_data")
        ])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_cancel_keyboard() -> InlineKeyboardMarkup:
    """Create cancel keyboard for admin operations."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_cancel")]
    ])

def format_user_info(user_data: dict) -> str:
    """Format user data for display."""
    if not user_data:
        return "❌ Данные не найдены"
    
    # Column name mappings for better display
    column_mapping = {
        'Номер сотрудника': '👤 Номер сотрудника',
        'ФИО': '📝 ФИО',
        'Игра': '🎮 Игра',
        'Здание': '🏢 Здание',
        'Этаж': '🏗️ Этаж',
        'Кабинет': '🚪 Кабинет',
        'Сессия': '📅 Сессия',
        'Стол': '🪑 Стол',
        'Место': '📍 Место'
    }
    
    formatted_lines = ["✅ <b>Ваша информация:</b>\n"]
    
    for key, value in user_data.items():
        if pd.notna(value):  # Skip NaN values
            display_name = column_mapping.get(key, key)
            formatted_lines.append(f"{display_name}: <code>{value}</code>")
    
    return "\n".join(formatted_lines)

async def create_users_excel() -> str:
    """Create Excel file with all users data and return file path."""
    try:
        # Get all users from database
        users = await db_manager.get_all_users()
        
        if not users:
            logger.warning("No users found in database")
            return None
        
        # Convert to DataFrame
        users_data = []
        for user in users:
            users_data.append({
                'ID': user.id,
                'Employee Number': user.employee_number,
                'Username': user.username or 'N/A',
                'Telegram ID': user.telegram_id
            })
        
        df = pd.DataFrame(users_data)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"users_data_{timestamp}.xlsx"
        
        # Run pandas operations in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: df.to_excel(filename, index=False)
        )
        
        logger.info(f"Created users Excel file: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Error creating users Excel file: {e}")
        return None

async def create_merged_excel() -> Optional[str]:
    """Create merged Excel file with employee data and telegram IDs."""
    try:
        # Get employee-telegram mapping from database
        db_df = await db_manager.get_employee_telegram_mapping()
        
        if db_df.empty:
            logger.warning("No employee-telegram mapping found in database")
            return None
        
        # Merge with Excel data
        merged_df = await data_manager.merge_with_telegram_data(db_df)
        
        if merged_df is None:
            logger.error("Merge operation failed")
            return None
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"merged_data_{timestamp}.xlsx"
        
        # Run pandas operations in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: merged_df.to_excel(filename, index=False)
        )
        
        logger.info(f"Created merged Excel file: {filename} with {len(merged_df)} rows")
        return filename
        
    except Exception as e:
        logger.error(f"Error creating merged Excel file: {e}")
        return None

async def startup_handler():
    """Initialize bot on startup."""
    global db_manager
    
    logger.info("Starting bot initialization...")
    
    # Initialize database manager (singleton)
    try:
        db_manager = DatabaseManager(DATABASE_URL)
        await db_manager.init_db()
        logger.info("Database manager initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database manager: {e}")
        return False
    
    # Load Excel data
    success = await data_manager.load_data()
    if not success:
        logger.error("Failed to load Excel data. Bot may not function properly.")
        return False
    
    logger.info("Bot initialized successfully!")
    return True

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

@dp.message(CommandStart())
async def start_handler(message: Message, state: FSMContext):
    """Handle /start command."""
    # Clear any existing state
    await state.clear()
    
    user_id = message.from_user.id
    username = message.from_user.username
    is_admin_user = is_admin(user_id)
    
    # Check if user exists in PostgreSQL
    existing_user = await db_manager.get_user_by_telegram_id(user_id)
    
    if existing_user:
        # User is registered, show main menu
        welcome_text = (
            f"👋 Добро пожаловать, <b>{message.from_user.first_name}</b>!\n\n"
            f"✅ Вы зарегистрированы как сотрудник №{existing_user.employee_number}\n\n"
            "🤖 Я бот для получения информации о сотрудниках.\n"
            "📊 Нажмите кнопку ниже, чтобы получить свою информацию."
        )
        
        if is_admin_user:
            welcome_text += "\n\n🔧 <b>Административные функции доступны</b>"
        
        await message.answer(
            text=welcome_text,
            reply_markup=get_main_keyboard(is_admin_user)
        )
    else:
        # User needs to register
        welcome_text = (
            f"👋 Добро пожаловать, <b>{message.from_user.first_name}</b>!\n\n"
            "🤖 Я бот для получения информации о сотрудниках.\n\n"
            "❗️ Для начала работы необходимо пройти регистрацию.\n"
            "Нажмите кнопку ниже для регистрации."
        )
        
        if is_admin_user:
            welcome_text += "\n\n🔧 <b>Административные функции доступны</b>"
        
        await message.answer(
            text=welcome_text,
            reply_markup=get_registration_keyboard(is_admin_user)
        )

@dp.callback_query(F.data == "start_registration")
async def start_registration_handler(callback: CallbackQuery, state: FSMContext):
    """Start registration process."""
    await callback.answer()
    
    registration_text = (
        "📝 <b>Регистрация</b>\n\n"
        "👤 Пожалуйста, введите ваш номер сотрудника:"
    )
    
    await callback.message.edit_text(
        text=registration_text
    )
    
    # Set FSM state to wait for employee number
    await state.set_state(RegistrationStates.waiting_for_employee_number)

@dp.message(RegistrationStates.waiting_for_employee_number)
async def process_employee_number(message: Message, state: FSMContext):
    """Process employee number input."""
    employee_number = message.text.strip()
    
    # Basic validation
    if not employee_number or len(employee_number) < 1:
        await message.answer(
            "❌ Пожалуйста, введите корректный номер сотрудника:"
        )
        return
    
    user_id = message.from_user.id
    username = message.from_user.username
    is_admin_user = is_admin(user_id)
    
    # Create user in database
    success = await db_manager.create_user(
        telegram_id=user_id,
        employee_number=employee_number,
        username=username
    )
    
    if success:
        success_text = (
            "✅ <b>Регистрация успешно завершена!</b>\n\n"
            f"👤 Номер сотрудника: <code>{employee_number}</code>\n"
            "Теперь вы можете получить свою информацию, нажав на кнопку ниже."
        )
        
        await message.answer(
            text=success_text,
            reply_markup=get_main_keyboard(is_admin_user)
        )
    else:
        error_text = (
            "❌ <b>Ошибка регистрации</b>\n\n"
            "Возможно, вы уже зарегистрированы. Используйте /start для проверки."
        )
        
        await message.answer(
            text=error_text,
            reply_markup=get_registration_keyboard(is_admin_user)
        )
    
    # Clear FSM state
    await state.clear()

@dp.callback_query(F.data == "get_my_info")
async def get_info_handler(callback: CallbackQuery):
    """Handle 'My Info' button press."""
    await callback.answer()
    
    user_id = callback.from_user.id
    is_admin_user = is_admin(user_id)
    logger.info(f"User {user_id} requested their info")
    
    # Check if user is registered in PostgreSQL
    existing_user = await db_manager.get_user_by_telegram_id(user_id)
    
    if not existing_user:
        response_text = (
            "❌ <b>Вы не зарегистрированы</b>\n\n"
            "Пожалуйста, пройдите регистрацию для получения информации."
        )
        
        await callback.message.edit_text(
            text=response_text,
            reply_markup=get_registration_keyboard(is_admin_user)
        )
        return
    
    # Search for user data in Excel
    user_data = await data_manager.find_user_data(user_id)
    
    if user_data:
        response_text = format_user_info(user_data)
    else:
        response_text = (
            "❌ <b>Информация не найдена</b>\n\n"
            f"Номер сотрудника: <code>{existing_user.employee_number}</code>\n\n"
            "Обратитесь к администратору для уточнения информации."
        )
    
    # Check if the content is different before editing
    try:
        await callback.message.edit_text(
            text=response_text,
            reply_markup=get_main_keyboard(is_admin_user)
        )
    except Exception as e:
        # If editing fails (message not modified), just acknowledge the callback
        logger.debug(f"Message edit failed (content unchanged): {e}")
        # Show a brief popup to acknowledge the button press
        await callback.answer("🔄 Информация обновлена", show_alert=False)

# Admin handlers
@dp.callback_query(F.data == "admin_upload_data")
async def admin_upload_data_handler(callback: CallbackQuery, state: FSMContext):
    """Handle admin upload data button."""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    await callback.answer()
    
    upload_text = (
        "📥 <b>Загрузка данных</b>\n\n"
        "Пожалуйста, отправьте Excel файл (.xlsx) с данными сотрудников.\n\n"
        "⚠️ Убедитесь, что файл содержит следующие колонки:\n"
        "• Номер сотрудника\n"
        "• ФИО\n" 
        "• Телеграм\n"
        "• ID (Telegram ID пользователей)\n"
        "• Игра\n"
        "• Здание\n"
        "• Этаж\n"
        "• Кабинет\n"
        "• Сессия\n"
        "• Стол\n"
        "• Место\n\n"
        "Все колонки должны быть заполнены корректно."
    )
    
    await callback.message.edit_text(
        text=upload_text,
        reply_markup=get_admin_cancel_keyboard()
    )
    
    await state.set_state(AdminStates.waiting_for_excel_file)

@dp.callback_query(F.data == "admin_download_data")
async def admin_download_data_handler(callback: CallbackQuery):
    """Handle admin download data button."""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    await callback.answer("📤 Создание файла...", show_alert=False)
    
    # Create Excel file with users data
    excel_file = await create_users_excel()
    
    if excel_file:
        try:
            # Create InputFile object for the local file
            input_file = FSInputFile(excel_file)
            
            # Send the file
            await bot.send_document(
                chat_id=user_id,
                document=input_file,
                caption="📤 <b>Данные пользователей</b>\n\nВсе зарегистрированные пользователи из базы данных."
            )
            
            # Clean up the file
            os.remove(excel_file)
            logger.info(f"Sent users data file to admin {user_id}")
            
        except Exception as e:
            logger.error(f"Error sending file to admin: {e}")
            await callback.message.answer(
                "❌ Ошибка при отправке файла. Попробуйте позже."
            )
            # Clean up the file even if sending failed
            if os.path.exists(excel_file):
                try:
                    os.remove(excel_file)
                except:
                    pass
    else:
        await callback.message.answer(
            "❌ Не удалось создать файл с данными пользователей."
        )

@dp.callback_query(F.data == "admin_merge_data")
async def admin_merge_data_handler(callback: CallbackQuery):
    """Handle admin merge data button."""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    await callback.answer("🔄 Выполнение слияния данных...", show_alert=False)
    
    try:
        # Show initial message
        await callback.message.edit_text(
            "🔄 <b>Слияние данных</b>\n\n"
            "⏳ Получение данных из базы данных...",
            reply_markup=None
        )
        
        # Create merged Excel file
        merged_file = await create_merged_excel()
        
        if merged_file:
            try:
                # Update message
                await callback.message.edit_text(
                    "🔄 <b>Слияние данных</b>\n\n"
                    "📤 Отправка файла...",
                    reply_markup=None
                )
                
                # Create InputFile object for the local file
                input_file = FSInputFile(merged_file)
                
                # Send the file
                await bot.send_document(
                    chat_id=user_id,
                    document=input_file,
                    caption=(
                        "🔄 <b>Результат слияния данных</b>\n\n"
                        "📊 Excel файл с объединенными данными:\n"
                        "• Все строки из исходного Excel файла\n"
                        "• Telegram ID добавлены для зарегистрированных пользователей\n"
                        "• Сохранен исходный порядок строк\n"
                        "• Дубликаты employee_id обработаны корректно"
                    )
                )
                
                # Update final message
                await callback.message.edit_text(
                    "✅ <b>Слияние завершено!</b>\n\n"
                    "📄 Файл с объединенными данными отправлен выше.",
                    reply_markup=get_main_keyboard(True)
                )
                
                # Clean up the file
                os.remove(merged_file)
                logger.info(f"Sent merged data file to admin {user_id}")
                
            except Exception as e:
                logger.error(f"Error sending merged file to admin: {e}")
                await callback.message.edit_text(
                    "❌ <b>Ошибка при отправке файла</b>\n\n"
                    "Файл создан, но произошла ошибка при отправке. Попробуйте позже.",
                    reply_markup=get_main_keyboard(True)
                )
                # Clean up the file even if sending failed
                if os.path.exists(merged_file):
                    try:
                        os.remove(merged_file)
                    except:
                        pass
        else:
            await callback.message.edit_text(
                "❌ <b>Ошибка слияния данных</b>\n\n"
                "Возможные причины:\n"
                "• Нет зарегистрированных пользователей\n"
                "• Ошибка доступа к Excel файлу\n"
                "• Некорректная структура данных\n\n"
                "Проверьте логи для получения подробной информации.",
                reply_markup=get_main_keyboard(True)
            )
            
    except Exception as e:
        logger.error(f"Unexpected error during merge operation: {e}")
        await callback.message.edit_text(
            "❌ <b>Неожиданная ошибка</b>\n\n"
            "Произошла ошибка при выполнении слияния данных. "
            "Попробуйте позже или обратитесь к разработчику.",
            reply_markup=get_main_keyboard(True)
        )

@dp.callback_query(F.data == "admin_cancel")
async def admin_cancel_handler(callback: CallbackQuery, state: FSMContext):
    """Handle admin cancel button."""
    user_id = callback.from_user.id
    
    if not is_admin(user_id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    await callback.answer()
    await state.clear()
    
    # Check if user is registered to show appropriate keyboard
    existing_user = await db_manager.get_user_by_telegram_id(user_id)
    
    cancel_text = "❌ Операция отменена."
    
    await callback.message.edit_text(
        text=cancel_text,
        reply_markup=get_main_keyboard(True) if existing_user else get_registration_keyboard(True)
    )

@dp.message(AdminStates.waiting_for_excel_file)
async def process_admin_excel_file(message: Message, state: FSMContext):
    """Process Excel file upload from admin."""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.answer("❌ Недостаточно прав")
        await state.clear()
        return
    
    # Check if message contains a document
    if not message.document:
        await message.answer(
            "❌ Пожалуйста, отправьте файл.\n\nОтправьте Excel файл (.xlsx) или нажмите 'Cancel' для отмены.",
            reply_markup=get_admin_cancel_keyboard()
        )
        return
    
    document: Document = message.document
    
    # Check file extension
    if not document.file_name or not document.file_name.lower().endswith('.xlsx'):
        await message.answer(
            "❌ Неверный формат файла.\n\nПожалуйста, отправьте файл в формате .xlsx",
            reply_markup=get_admin_cancel_keyboard()
        )
        return
    
    try:
        # Download the file
        file = await bot.get_file(document.file_id)
        
        # Create backup of current file if it exists
        if os.path.exists(EXCEL_FILE_PATH):
            backup_path = f"{EXCEL_FILE_PATH}.backup"
            os.rename(EXCEL_FILE_PATH, backup_path)
            logger.info(f"Created backup: {backup_path}")
        
        # Save the new file
        await bot.download_file(file.file_path, EXCEL_FILE_PATH)
        
        # Try to reload the data
        success = await data_manager.reload_data()
        
        if success:
            success_text = (
                "✅ <b>Данные успешно обновлены!</b>\n\n"
                f"📄 Файл: <code>{document.file_name}</code>\n"
                f"📊 Загружено записей: <code>{len(data_manager.df)}</code>\n\n"
                "Все пользователи теперь будут получать данные из нового файла."
            )
            
            await message.answer(
                text=success_text,
                reply_markup=get_main_keyboard(True)
            )
            
            logger.info(f"Admin {user_id} successfully updated Excel data")
            
        else:
            # Restore backup if reload failed
            backup_path = f"{EXCEL_FILE_PATH}.backup"
            if os.path.exists(backup_path):
                os.rename(backup_path, EXCEL_FILE_PATH)
                await data_manager.reload_data()  # Reload old data
                logger.info("Restored backup due to reload failure")
            
            error_text = (
                "❌ <b>Ошибка загрузки файла</b>\n\n"
                "Файл не удалось обработать. Возможные причины:\n"
                "• Повреждённый файл\n"
                "• Неправильный формат данных\n\n"
                "Старые данные сохранены."
            )
            
            await message.answer(
                text=error_text,
                reply_markup=get_main_keyboard(True)
            )
    
    except Exception as e:
        logger.error(f"Error processing admin Excel file: {e}")
        
        # Try to restore backup
        backup_path = f"{EXCEL_FILE_PATH}.backup"
        if os.path.exists(backup_path):
            if os.path.exists(EXCEL_FILE_PATH):
                os.remove(EXCEL_FILE_PATH)
            os.rename(backup_path, EXCEL_FILE_PATH)
            await data_manager.reload_data()
        
        await message.answer(
            "❌ Произошла ошибка при обработке файла. Старые данные сохранены.",
            reply_markup=get_main_keyboard(True)
        )
    
    finally:
        # Clean up backup if everything is OK
        backup_path = f"{EXCEL_FILE_PATH}.backup"
        if os.path.exists(backup_path) and os.path.exists(EXCEL_FILE_PATH):
            try:
                os.remove(backup_path)
            except:
                pass
    
    await state.clear()

@dp.message()
async def unknown_handler(message: Message, state: FSMContext):
    """Handle all other messages."""
    # Check current state
    current_state = await state.get_state()
    
    if current_state == RegistrationStates.waiting_for_employee_number:
        # User is in registration process, this is handled by process_employee_number
        return
    elif current_state == AdminStates.waiting_for_excel_file:
        # Admin is uploading file, this is handled by process_admin_excel_file
        return
    
    # Check if user is registered
    user_id = message.from_user.id
    is_admin_user = is_admin(user_id)
    existing_user = await db_manager.get_user_by_telegram_id(user_id)
    
    if existing_user:
        keyboard = get_main_keyboard(is_admin_user)
        text = ("🤔 Я не понимаю эту команду.\n"
                "Используйте кнопку ниже для получения информации.")
    else:
        keyboard = get_registration_keyboard(is_admin_user)
        text = ("🤔 Я не понимаю эту команду.\n"
                "Пожалуйста, пройдите регистрацию для начала работы.")
    
    await message.answer(
        text=text,
        reply_markup=keyboard
    )

async def main():
    """Main function to run the bot."""
    try:
        # Initialize bot
        if not await startup_handler():
            logger.error("Failed to initialize bot. Exiting...")
            return
        
        # Start polling
        logger.info("Starting bot polling...")
        await dp.start_polling(bot)
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        await bot.session.close()
        if db_manager:
            await db_manager.close()

if __name__ == "__main__":
    asyncio.run(main())