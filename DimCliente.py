from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
from connection_script import get_engine

def extract(engine_bodega):
    """Extraer datos de la base de datos 'bodega_datos'."""
    with engine_bodega.connect() as conn_bodega:
        query_bodega = text("""
            SELECT cliente_id, nit_cliente, nombre, email, direccion, telefono, nombre_contacto, 
                   ciudad_id, tipo_cliente_id, activo, sector
            FROM cliente
            WHERE cliente_id IS NOT NULL AND nombre IS NOT NULL
        """)
        result_bodega = conn_bodega.execute(query_bodega)
        clientes = result_bodega.fetchall()
    return clientes

def transform(clientes, engine_etl):
    """Transformar los datos y validar existencia en la base de datos de destino."""
    valid_clientes = []
    
    with engine_etl.connect() as conn_etl:
        for cliente in clientes:
            (cliente_id, nit_cliente, nombre, email, direccion, telefono, 
             nombre_contacto, ciudad_id, tipo_cliente_id, activo, sector) = cliente

            # Validar que el tipo_cliente_id existe en la tabla tipo_cliente
            query_tipo_cliente = text("SELECT 1 FROM tipo_cliente WHERE id_tipo_cliente = :tipo_cliente_id")
            tipo_cliente_existente = conn_etl.execute(query_tipo_cliente, {"tipo_cliente_id": tipo_cliente_id}).fetchone()

            # Validar que el ciudad_id existe en la tabla ciudad
            query_ciudad = text("SELECT 1 FROM ciudades WHERE id_ciudad = :ciudad_id")
            ciudad_existente = conn_etl.execute(query_ciudad, {"ciudad_id": ciudad_id}).fetchone()

            if not tipo_cliente_existente:
                print(f"El tipo de cliente con ID {tipo_cliente_id} no existe, el cliente {cliente_id} no será insertado.")
                continue

            if not ciudad_existente:
                print(f"La ciudad con ID {ciudad_id} no existe, el cliente {cliente_id} no será insertado.")
                continue

            # Verificar si el cliente ya existe en la base de datos de destino
            query_existente = text("SELECT 1 FROM cliente WHERE id_cliente = :cliente_id")
            cliente_existente = conn_etl.execute(query_existente, {"cliente_id": cliente_id}).fetchone()

            if cliente_existente:
                print(f"El cliente con ID {cliente_id} ya existe en etl_rapidos_furiosos, no se insertará.")
            else:
                valid_clientes.append(cliente)  # Agregar a la lista de válidos
    
    return valid_clientes

def load(valid_clientes, engine_etl):
    """Cargar los datos transformados en la base de datos 'etl_rapidos_furiosos'."""
    with engine_etl.connect() as conn_etl:
        for cliente in valid_clientes:
            (cliente_id, nit_cliente, nombre, email, direccion, telefono,
             nombre_contacto, ciudad_id, tipo_cliente_id, activo, sector) = cliente
            
            # Insertar en la base de datos
            query_insert = text("""
                INSERT INTO cliente (id_cliente, nit_cliente, nombre_cliente, email,
                                     direccion, telefono_cliente, nombre_contacto,
                                     id_ciudad, id_tipo_cliente, activo, sector)
                VALUES (:cliente_id, :nit_cliente, :nombre, :email,
                        :direccion, :telefono, :nombre_contacto,
                        :ciudad_id, :tipo_cliente_id, :activo,
                        :sector)
            """)
            conn_etl.execute(query_insert,{
                "cliente_id": cliente_id,
                "nit_cliente": nit_cliente,
                "nombre": nombre,
                "email": email,
                "direccion": direccion,
                "telefono": telefono,
                "nombre_contacto": nombre_contacto,
                "ciudad_id": ciudad_id,
                "tipo_cliente_id": tipo_cliente_id,
                "activo": activo,
                "sector": sector
            })
            #print(f"Cliente {cliente_id} insertado correctamente.")
        
        # Confirmar los cambios (commit de la transacción)
        conn_etl.commit()

def transferir_clientes():
    """Función principal para ejecutar el proceso ETL."""
    # Obtener los engines de las bases de datos
    engine_bodega = get_engine('bodega_datos')
    engine_etl = get_engine('etl_rapidos_furiosos')

    # Crear una sesión para la base de datos ETL
    Session = sessionmaker(bind=engine_etl)
    session = Session()

    try:
        # Proceso ETL
        clientes = extract(engine_bodega)
        valid_clientes = transform(clientes, engine_etl)
        load(valid_clientes, engine_etl)

        # Mostrar los registros en la base de datos 'etl_rapidos_furiosos'
        with engine_etl.connect() as conn_etl:
            query_etl = text("SELECT id_cliente, nombre_cliente, id_tipo_cliente, id_ciudad FROM cliente")
            result_etl = conn_etl.execute(query_etl)
            clientes_etl = result_etl.fetchall()

            # Convertir el resultado en un DataFrame de pandas
            df = pd.DataFrame(clientes_etl, columns=["ID Cliente", "Nombre Cliente", "ID Tipo Cliente", "ID Ciudad"])

            # Ordenar por 'ID Cliente' y restablecer el índice
            df.sort_values(by='ID Cliente', inplace=True)
            df.reset_index(drop=True, inplace=True)

            # Mostrar la tabla ordenada
            print("Registros en la base de datos 'etl_rapidos_furiosos' (cliente):")
            print(df)

    except Exception as e:
        session.rollback()  # Si ocurre un error hacer rollback
        print(f"Error al transferir los clientes: {e}")

# Llamar a la función principal
if __name__ == "__main__":
    transferir_clientes()