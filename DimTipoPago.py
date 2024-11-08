from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine

def extract(engine_bodega):
    """Extraer datos de la base de datos 'bodega_datos'."""
    with engine_bodega.connect() as conn_bodega:
        query_bodega = text("SELECT id, nombre, descripcion FROM mensajeria_tipopago")
        result_bodega = conn_bodega.execute(query_bodega)
        tipo_pagos = result_bodega.fetchall()
    return tipo_pagos

def transform(tipo_pagos):
    """Transformar los datos y rellenar descripciones nulas."""
    transformed_data = []
    
    for tipo_pago in tipo_pagos:
        id, nombre, descripcion = tipo_pago
        
        # Rellenar 'descripcion' con "Sin información" si está NULL
        if descripcion is None:
            descripcion = "Sin información"
        
        transformed_data.append((id, nombre, descripcion))
    
    return transformed_data

def load(transformed_data, engine_etl):
    """Cargar los datos transformados en la base de datos 'etl_rapidos_furiosos'."""
    with engine_etl.connect() as conn_etl:
        for id, nombre, descripcion in transformed_data:
            # Verificar si el tipo de pago ya existe en la base de datos de destino
            query_existente = text("SELECT 1 FROM tipo_pago WHERE id_tipo_pago = :id")
            tipo_pago_existente = conn_etl.execute(query_existente, {"id": id}).fetchone()

            if tipo_pago_existente:
                print(f"El tipo de pago con ID {id} ya existe en etl_rapidos_furiosos, no se insertará.")
            else:
                # Si no existe, insertarlo en la base de datos de destino
                query_insert = text("""
                    INSERT INTO tipo_pago (id_tipo_pago, nombre, descripcion) 
                    VALUES (:id, :nombre, :descripcion)
                """)
                conn_etl.execute(query_insert, {"id": id, "nombre": nombre, "descripcion": descripcion})
                print(f"Tipo de pago {id} insertado correctamente.")

        # Confirmar los cambios (commit de la transacción)
        conn_etl.commit()

def transferir_tipo_pago():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')

    # Crear una sesión para la base de datos ETL
    Session = sessionmaker(bind=engine_etl)
    session = Session()

    try:
        # Proceso ETL
        tipo_pagos = extract(engine_bodega)
        transformed_data = transform(tipo_pagos)
        load(transformed_data, engine_etl)

        # Mostrar los registros en la base de datos 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_tipo_pago, nombre, descripcion FROM tipo_pago")
            result_etl = conn_etl.execute(query_etl)
            tipo_pagos_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(tipo_pagos_etl, columns=["ID Tipo Pago", "Nombre", "Descripción"])

            # Ordenar por 'ID Tipo Pago' y restablecer el índice
            df.sort_values(by='ID Tipo Pago', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos' (tipo_pago):")
            print(df)

    except Exception as e:
        session.rollback()  # Si ocurre un error hacer rollback
        print(f"Error al transferir los tipos de pago: {e}")

# Llamar a la función principal
if __name__ == "__main__":
    transferir_tipo_pago()