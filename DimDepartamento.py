from sqlalchemy import create_engine, text
from connection_script import get_engine
import pandas as pd


def transferir_departamentos():
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')
    
    try:
        # Conectar a la base de datos 'bodega_datos' y obtener los registros de la tabla 'departamento'
        with engine_bodega.connect() as conn_bodega:
            # Consulta para obtener los registros válidos de 'departamento' (sin NULL)
            query_bodega = text("SELECT departamento_id, nombre FROM departamento WHERE departamento_id IS NOT NULL AND nombre IS NOT NULL")
            result_bodega = conn_bodega.execute(query_bodega)
            departamentos = result_bodega.fetchall()  # Traer todos los resultados

        # Conectar a la base de datos 'etl_rapidos_furiosos' para insertar los registros
        with engine_etl.connect() as conn_etl:
            for departamento in departamentos:
                departamento_id, nombre = departamento
                
                # Verificar si el registro ya existe en la base de datos de destino 'etl_rapidos_furiosos'
                query_existente = text("SELECT 1 FROM departamentos WHERE id_departamento = :departamento_id")
                result_existente = conn_etl.execute(query_existente, {"departamento_id": departamento_id}).fetchone()
                
                if result_existente:
                    print(f"El departamento con ID {departamento_id} ya existe en etl_rapidos_furiosos, no se insertará.")
                else:
                    # Si no existe, insertarlo en la base de datos de destino
                    query_insert = text("""
                    INSERT INTO departamentos (id_departamento, nombre_departamento) 
                    VALUES (:departamento_id, :nombre)
                    """)
                    conn_etl.execute(query_insert, {"departamento_id": departamento_id, "nombre": nombre})
                    print(f"Departamento {departamento_id} insertado correctamente.")
                    
            # Confirmar los cambios (commit de la transacción)
            conn_etl.commit()
            
        # Mostrar los registros de la tabla 'departamentos' en la base de datos de destino 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_departamento, nombre_departamento FROM departamentos")
            result_etl = conn_etl.execute(query_etl)
            departamentos_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(departamentos_etl, columns=["ID Departamento", "Nombre Departamento"])

            # Ordenar por 'ID Departamento' y restablecer el índice
            df.sort_values(by='ID Departamento', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos':")
            print(df)

    except Exception as e:
        print(f"Error al transferir los departamentos: {e}")

# Llamar a la función
transferir_departamentos()
