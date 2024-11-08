from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine

def extract(engine_bodega):
    """Extraer datos de la base de datos 'bodega_datos'."""
    with engine_bodega.connect() as conn_bodega:
        query_bodega = text("""
            SELECT tipo_cliente_id, nombre, COALESCE(descripcion, 'NaN') AS descripcion 
            FROM tipo_cliente 
            WHERE tipo_cliente_id IS NOT NULL AND nombre IS NOT NULL
        """)
        result_bodega = conn_bodega.execute(query_bodega)
        tipos_cliente = result_bodega.fetchall()
    return tipos_cliente

def transform(tipos_cliente, engine_etl):
    """Transformar los datos y validar existencia en la base de datos de destino."""
    valid_tipos_cliente = []
    
    with engine_etl.connect() as conn_etl:
        for tipo_cliente in tipos_cliente:
            tipo_cliente_id, nombre, descripcion = tipo_cliente
            
            # Verificar si el registro ya existe en la base de datos de destino
            query_existente = text("SELECT 1 FROM tipo_cliente WHERE id_tipo_cliente = :tipo_cliente_id")
            result_existente = conn_etl.execute(query_existente, {"tipo_cliente_id": tipo_cliente_id}).fetchone()

            if result_existente:
                print(f"El tipo de cliente con ID {tipo_cliente_id} ya existe en etl_rapidos_furiosos, no se insertará.")
            else:
                valid_tipos_cliente.append((tipo_cliente_id, nombre, descripcion))  # Agregar a la lista de válidos
    
    return valid_tipos_cliente

def load(valid_tipos_cliente, engine_etl):
    """Cargar los datos transformados en la base de datos 'etl_rapidos_furiosos'."""
    with engine_etl.connect() as conn_etl:
        for tipo_cliente in valid_tipos_cliente:
            tipo_cliente_id, nombre, descripcion = tipo_cliente
            
            # Insertar en la base de datos
            query_insert = text("""
                INSERT INTO tipo_cliente (id_tipo_cliente, nombre, descripcion) 
                VALUES (:tipo_cliente_id, :nombre, :descripcion)
            """)
            conn_etl.execute(query_insert, {"tipo_cliente_id": tipo_cliente_id, "nombre": nombre, "descripcion": descripcion})
            #print(f"Tipo de cliente {tipo_cliente_id} insertado correctamente.")
        
        # Confirmar los cambios (commit de la transacción)
        conn_etl.commit()

def transferir_tipo_cliente():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')

    # Crear una sesión para la base de datos ETL
    Session = sessionmaker(bind=engine_etl)
    session = Session()

    try:
        # Proceso ETL
        tipos_cliente = extract(engine_bodega)
        valid_tipos_cliente = transform(tipos_cliente, engine_etl)
        load(valid_tipos_cliente, engine_etl)

        # Mostrar los registros en la base de datos 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_tipo_cliente, nombre, descripcion FROM tipo_cliente")
            result_etl = conn_etl.execute(query_etl)
            tipos_cliente_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(tipos_cliente_etl, columns=["ID Tipo Cliente", "Nombre", "Descripción"])

            # Ordenar por 'ID Tipo Cliente' y restablecer el índice
            df.sort_values(by='ID Tipo Cliente', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos' (tipo_cliente):")
            print(df)

    except Exception as e:
        session.rollback()  # Si ocurre un error, hacer rollback
        print(f"Error al transferir los tipos de cliente: {e}")

# Llamar a la función principal
if __name__ == "__main__":
    transferir_tipo_cliente()