
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

#  Cargar las variables del archivo .env
load_dotenv()

# Obtener las variables
password = os.getenv('PASSWORD')
user_db = os.getenv('USER_DB')
db_name = os.getenv('DB_NAME')


# Configuración de conexión a PostgreSQL
DATABASE_URI = f'postgresql+psycopg2://{user_db}:{password}@localhost:5432/{db_name}'
engine = create_engine(DATABASE_URI)

