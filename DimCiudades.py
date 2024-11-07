from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine

def transferir_ciudades():
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')

    # Crear una sesión para la base de datos ETL
    Session = sessionmaker(bind=engine_etl)
    session = Session()

    try:
        # Conectar a la base de datos 'bodega_datos' y obtener los registros de la tabla 'ciudad'
        with engine_bodega.connect() as conn_bodega:
            # Consulta para obtener los registros válidos de 'ciudad' (sin NULL)
            query_bodega = text("""
                SELECT ciudad_id, nombre, departamento_id 
                FROM ciudad 
                WHERE ciudad_id IS NOT NULL AND nombre IS NOT NULL AND departamento_id IS NOT NULL
            """)
            result_bodega = conn_bodega.execute(query_bodega)
            ciudades = result_bodega.fetchall()  # Traer todos los resultados

        # Conectar a la base de datos 'etl_rapidos_furiosos' para insertar los registros
        with engine_etl.connect() as conn_etl:
            for ciudad in ciudades:
                ciudad_id, nombre, departamento_id = ciudad
                
                # Validar si el departamento_id existe en la tabla 'departamentos' en 'etl_rapidos_furiosos'
                query_departamento = text("SELECT 1 FROM departamentos WHERE id_departamento = :departamento_id")
                result_departamento = conn_etl.execute(query_departamento, {"departamento_id": departamento_id}).fetchone()

                if not result_departamento:
                    print(f"El departamento con ID {departamento_id} no existe en etl_rapidos_furiosos. Ciudad {ciudad_id} no será insertada.")
                    continue  # Saltar la inserción de esta ciudad

                # Verificar si el registro ya existe en la base de datos de destino 'etl_rapidos_furiosos'
                query_existente = text("SELECT 1 FROM ciudades WHERE id_ciudad = :ciudad_id")
                result_existente = conn_etl.execute(query_existente, {"ciudad_id": ciudad_id}).fetchone()

                if result_existente:
                    print(f"La ciudad con ID {ciudad_id} ya existe en etl_rapidos_furiosos, no se insertará.")
                else:
                    # Si no existe, insertarlo en la base de datos de destino
                    query_insert = text("""
                        INSERT INTO ciudades (id_ciudad, nombre_ciudad, id_departamento) 
                        VALUES (:ciudad_id, :nombre, :departamento_id)
                    """)
                    conn_etl.execute(query_insert, {"ciudad_id": ciudad_id, "nombre": nombre, "departamento_id": departamento_id})
                    #print(f"Ciudad {ciudad_id} insertada correctamente.")
                    # Confirmar los cambios
                    conn_etl.commit()

        # Mostrar los registros de la tabla 'ciudades' en la base de datos de destino 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_ciudad, nombre_ciudad, id_departamento FROM ciudades")
            result_etl = conn_etl.execute(query_etl)
            ciudades_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(ciudades_etl, columns=["ID Ciudad", "Nombre Ciudad", "ID Departamento"])

            # Ordenar por 'ID Ciudad' y restablecer el índice
            df.sort_values(by='ID Ciudad', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos' (ciudades):")
            print(df)

    except Exception as e:
        session.rollback()  # Si ocurre un error, hacer rollback
        print(f"Error al transferir las ciudades: {e}")

# Llamar a la función
transferir_ciudades()
