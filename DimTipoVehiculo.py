from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine

def extract(engine_bodega):
    """Extraer datos de la base de datos 'bodega_datos'."""
    with engine_bodega.connect() as conn_bodega:
        query_bodega = text("SELECT id, nombre, descripcion FROM mensajeria_tipovehiculo")
        result_bodega = conn_bodega.execute(query_bodega)
        tipo_vehiculos = result_bodega.fetchall()
    return tipo_vehiculos

def transform(tipo_vehiculos):
    """Transformar los datos y rellenar descripciones nulas."""
    transformed_data = []
    
    for tipo_vehiculo in tipo_vehiculos:
        id, nombre, descripcion = tipo_vehiculo
        
        # Rellenar 'descripcion' con "Sin información" si está NULL
        if descripcion is None:
            descripcion = "Sin información"
        
        transformed_data.append((id, nombre, descripcion))
    
    return transformed_data

def load(transformed_data, engine_etl):
    """Cargar los datos transformados en la base de datos 'etl_rapidos_furiosos'."""
    with engine_etl.connect() as conn_etl:
        for id, nombre, descripcion in transformed_data:
            # Verificar si el tipo de vehículo ya existe en la base de datos de destino
            query_existente = text("SELECT 1 FROM tipo_vehiculo WHERE id_tipo_vehiculo = :id")
            tipo_vehiculo_existente = conn_etl.execute(query_existente, {"id": id}).fetchone()

            if tipo_vehiculo_existente:
                print(f"El tipo de vehículo con ID {id} ya existe en etl_rapidos_furiosos, no se insertará.")
            else:
                # Si no existe, insertarlo en la base de datos de destino
                query_insert = text("""
                    INSERT INTO tipo_vehiculo (id_tipo_vehiculo, nombre, descripcion) 
                    VALUES (:id, :nombre, :descripcion)
                """)
                conn_etl.execute(query_insert, {"id": id, "nombre": nombre, "descripcion": descripcion})
                print(f"Tipo de vehículo {id} insertado correctamente.")

        # Confirmar los cambios (commit de la transacción)
        conn_etl.commit()

def transferir_tipo_vehiculo():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')

    # Crear una sesión para la base de datos ETL
    Session = sessionmaker(bind=engine_etl)
    session = Session()

    try:
        # Proceso ETL
        tipo_vehiculos = extract(engine_bodega)
        transformed_data = transform(tipo_vehiculos)
        load(transformed_data, engine_etl)

        # Mostrar los registros en la base de datos 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_tipo_vehiculo, nombre, descripcion FROM tipo_vehiculo")
            result_etl = conn_etl.execute(query_etl)
            tipo_vehiculos_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(tipo_vehiculos_etl, columns=["ID Tipo Vehículo", "Nombre", "Descripción"])

            # Ordenar por 'ID Tipo Vehículo' y restablecer el índice
            df.sort_values(by='ID Tipo Vehículo', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos' (tipo_vehiculo):")
            print(df)

    except Exception as e:
        session.rollback()  # Si ocurre un error hacer rollback
        print(f"Error al transferir los tipos de vehículos: {e}")

# Llamar a la función principal
if __name__ == "__main__":
    transferir_tipo_vehiculo()