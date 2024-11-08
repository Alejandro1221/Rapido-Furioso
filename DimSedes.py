from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine

def extract(engine_bodega):
    """Extraer datos de la base de datos 'bodega_datos'."""
    with engine_bodega.connect() as conn_bodega:
        query_bodega = text("""
            SELECT sede_id, nombre, direccion, telefono, nombre_contacto, ciudad_id, cliente_id
            FROM sede
            WHERE sede_id IS NOT NULL AND nombre IS NOT NULL
        """)
        result_bodega = conn_bodega.execute(query_bodega)
        sedes = result_bodega.fetchall()  # Traer todos los resultados
    return sedes

def transform(sedes, engine_etl):
    """Validar la existencia de ciudades y clientes en la base de datos destino."""
    valid_sedes = []
    
    with engine_etl.connect() as conn_etl:
        for sede in sedes:
            sede_id, nombre, direccion, telefono, nombre_contacto, ciudad_id, cliente_id = sede
            
            # Validar que `ciudad_id` exista en la base de datos destino
            query_ciudad = text("SELECT 1 FROM ciudades WHERE id_ciudad = :ciudad_id")
            ciudad_existente = conn_etl.execute(query_ciudad, {"ciudad_id": ciudad_id}).fetchone()
            
            # Validar que `cliente_id` exista en la base de datos destino
            query_cliente = text("SELECT 1 FROM cliente WHERE id_cliente = :cliente_id")
            cliente_existente = conn_etl.execute(query_cliente, {"cliente_id": cliente_id}).fetchone()
            
            # Si no existe la ciudad o el cliente, omitir el registro
            if not ciudad_existente or not cliente_existente:
                print(f"Sede con ID {sede_id} omitida: Ciudad o Cliente no existe en etl_rapidos_furiosos.")
                continue
            
            valid_sedes.append(sede)  # Agregar a la lista de sedes válidas
    
    return valid_sedes

def load(valid_sedes, engine_etl):
    """Cargar los datos transformados en la base de datos 'etl_rapidos_furiosos'."""
    with engine_etl.connect() as conn_etl:
        for sede in valid_sedes:
            sede_id, nombre, direccion, telefono, nombre_contacto, ciudad_id, cliente_id = sede
            
            # Verificar si el registro ya existe en la base de datos de destino 'etl_rapidos_furiosos'
            query_existente = text("SELECT 1 FROM sedes WHERE id_sede = :sede_id")
            sede_existente = conn_etl.execute(query_existente, {"sede_id": sede_id}).fetchone()
            
            if sede_existente:
                print(f"La sede con ID {sede_id} ya existe en etl_rapidos_furiosos, no se insertará.")
            else:
                # Insertar el registro si no existe
                query_insert = text("""
                    INSERT INTO sedes (id_sede, nombre_sede, id_ciudad, direccion, telefono_sede, id_cliente)
                    VALUES (:sede_id, :nombre_sede, :id_ciudad, :direccion, :telefono_sede, :id_cliente)
                """)
                conn_etl.execute(query_insert, {
                    "sede_id": sede_id,
                    "nombre_sede": nombre,
                    "id_ciudad": ciudad_id,
                    "direccion": direccion,
                    "telefono_sede": telefono,
                    "id_cliente": cliente_id
                })
                print(f"Sede {sede_id} insertada correctamente.")
        
        # Confirmar los cambios (commit de la transacción)
        conn_etl.commit()

def transferir_sedes():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')
    
    # Crear una sesión para manejar transacciones
    Session = sessionmaker(bind=engine_etl)
    session = Session()

    try:
        # Proceso ETL
        sedes = extract(engine_bodega)
        valid_sedes = transform(sedes, engine_etl)
        load(valid_sedes, engine_etl)
        
        # Mostrar los registros en la base de datos 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_sede, nombre_sede, id_ciudad FROM sedes")
            result_etl = conn_etl.execute(query_etl)
            sedes_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(sedes_etl, columns=["ID Sede", "Nombre Sede", "ID Ciudad"])

            # Ordenar por 'ID Sede' y restablecer el índice
            df.sort_values(by='ID Sede', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos' (sedes):")
            print(df)

    except Exception as e:
        session.rollback()  # Si ocurre un error hacer rollback
        print(f"Error al transferir las sedes: {e}")
    
    finally:
        session.close()  # Cerrar la sesión

# Llamar a la función principal
if __name__ == "__main__":
    transferir_sedes()