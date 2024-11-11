--restaurar la base de datos del profesor 

psql -h localhost -p 5433 -U postgres -d bodega_datos -f "/Users/jhonfreddypopomoreno/Downloads/bd_proyecto_2024 2/copia-BD.sql"

-- crear tablas para etl de analisis
CREATE TABLE fechas (
    id_fecha SERIAL PRIMARY KEY,
    fecha date NOT NULL,
    dia integer NOT NULL,
    mes integer NOT NULL,
    "año" integer NOT NULL,
    nombre_dia character varying(10) COLLATE pg_catalog."default" NOT NULL,
    nombre_mes character varying(15) COLLATE pg_catalog."default" NOT NULL,
    es_fin_de_semana boolean NOT NULL
);

CREATE TABLE departamentos (
    id_departamento SERIAL PRIMARY KEY,
    nombre_departamento VARCHAR(100) NOT NULL
);

CREATE TABLE ciudades (
    id_ciudad SERIAL PRIMARY KEY,
    nombre_ciudad VARCHAR(100) NOT NULL,
    id_departamento INT NOT NULL,
    FOREIGN KEY (id_departamento) REFERENCES departamentos(id_departamento)
);

CREATE TABLE tipo_cliente (
    id_tipo_cliente SERIAL PRIMARY KEY,
    nombre VARCHAR(120) NOT NULL,
    descripcion TEXT
);

CREATE TABLE cliente (
    id_cliente SERIAL PRIMARY KEY,
    nit_cliente VARCHAR(50) NOT NULL,
    nombre_cliente VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    id_tipo_cliente INT,
    telefono_cliente VARCHAR(20),
    direccion VARCHAR(255),
    id_ciudad INT,
    fecha_nacimiento DATE,
    nombre_contacto VARCHAR(120),
    activo BOOLEAN DEFAULT true,
    sector VARCHAR(50),
    FOREIGN KEY (id_tipo_cliente) REFERENCES tipo_cliente(id_tipo_cliente),
    FOREIGN KEY (id_ciudad) REFERENCES ciudades(id_ciudad)
);

CREATE TABLE sedes (
    id_sede SERIAL PRIMARY KEY,
    nombre_sede VARCHAR(120) NOT NULL,
    id_ciudad INT,
    id_cliente INT,
    direccion VARCHAR(255),
    telefono_sede VARCHAR(20),
    FOREIGN KEY (id_cliente) REFERENCES cliente(id_cliente),
    FOREIGN KEY (id_ciudad) REFERENCES ciudades(id_ciudad)
);

CREATE TABLE tipo_vehiculo (
    id_tipo_vehiculo SERIAL PRIMARY KEY,
    nombre VARCHAR(120) NOT NULL,
    descripcion VARCHAR(120)
);

CREATE TABLE tipo_pago (
    id_tipo_pago SERIAL PRIMARY KEY,
    nombre VARCHAR(120) NOT NULL,
    descripcion TEXT
);

CREATE TABLE mensajero (
    id_mensajero SERIAL PRIMARY KEY,
    telefono_mensajero VARCHAR(20),
    activo BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE tipo_servicio (
    id_tipo_servicio SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL
);

CREATE TABLE origen_servicio (
    id_origen SERIAL PRIMARY KEY,
    direccion VARCHAR(120) NOT NULL,
    id_cliente INT REFERENCES cliente(id_cliente),
    id_ciudad INT REFERENCES ciudades(id_ciudad)
);

CREATE TABLE destino_servicio (
    id_destino SERIAL PRIMARY KEY,
    direccion VARCHAR(120) NOT NULL,
    id_cliente INT REFERENCES cliente(id_cliente),
    id_ciudad INT REFERENCES ciudades(id_ciudad)
);

CREATE TABLE estados (
    id_estado SERIAL PRIMARY KEY,
    nombre_estado VARCHAR(100) NOT NULL,
    descripcion_estado VARCHAR(500) NOT NULL
);

-----------

CREATE TABLE servicios (
    id_servicio SERIAL PRIMARY KEY,
    id_cliente INT REFERENCES cliente(id_cliente) ON DELETE CASCADE,
    id_mensajero INT REFERENCES mensajero(id_mensajero) ON DELETE CASCADE,


    id_fecha_solicitud INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,
    id_fecha_mensajero_asignado INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,
    id_fecha_recogida INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,
    id_fecha_entrega INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,
    id_fecha_cerrado INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,

    hora_solicitud TIME,
    hora_mensajero_asignado TIME, --mensajero asignado
    hora_recogida TIME,
    hora_entrega TIME,
    hora_cerrado TIME,

    fecha_solicitud DATE,
    fecha_entrega DATE,
    id_estado INT REFERENCES estados(id_estado) ON DELETE CASCADE,
    duracion_total INT,
    tiempo_espera DECIMAL,
    tiempo_mensajero_recogida DECIMAL,
    tiempo_recogida_entrega DECIMAL,
    tiempo_entrega_cerrado DECIMAL,

    id_origen_servicio INT REFERENCES origen_servicio(id_origen) ON DELETE CASCADE,
    id_destino_servicio INT REFERENCES destino_servicio(id_destino) ON DELETE CASCADE,
    descripcion_pago TEXT,
    id_tiposervicio INT REFERENCES tipo_servicio(id_tipo_servicio) ON DELETE CASCADE,
    prioridad VARCHAR(100),
    -- Puede ser VARCHAR o INT dependiendo de cómo quieras manejar la prioridad
    id_origen_ciudad INT REFERENCES ciudades(id_ciudad) ON DELETE CASCADE,
    id_destino_ciudad INT REFERENCES ciudades(id_ciudad) ON DELETE CASCADE,
    descripcion_cancelado VARCHAR(2000),

    id_tipo_vehiculo INT REFERENCES tipo_vehiculo(id_tipo_vehiculo) ON DELETE CASCADE,
    id_tipo_pago INT REFERENCES  tipo_pago(id_tipo_pago) ON DELETE CASCADE
);

CREATE TABLE novedades (
    id_novedad SERIAL PRIMARY KEY,
    id_servicio INT REFERENCES servicios(id_servicio) ON DELETE CASCADE,
    descripcion TEXT NOT NULL,
    id_tipo_novedad INT REFERENCES tipo_servicio(id_tipo_servicio) ON DELETE CASCADE,
    id_mensajero INT REFERENCES mensajero(id_mensajero) ON DELETE CASCADE,
    fecha_novedad DATE NOT NULL
);


CREATE TABLE eficiencia_dia (
    id_estado SERIAL PRIMARY KEY,
    id_mensajero INT REFERENCES mensajero(id_mensajero) ON DELETE CASCADE,
    id_fecha INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,
    cantidad_servicios INT NOT NULL,
    novedades_reportadas TEXT,
    tiempo_promedio INT NOT NULL
);

CREATE TABLE eficiencia_hora (
    id_estado SERIAL PRIMARY KEY,
    id_mensajero INT REFERENCES mensajero(id_mensajero) ON DELETE CASCADE,
    id_fecha INT REFERENCES fechas(id_fecha) ON DELETE CASCADE,
    cantidad_servicios INT NOT NULL,
    novedades_reportadas TEXT,
    tiempo_promedio INT NOT NULL
);



---- eliminar todos los registros

DO
$$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE;';
    END LOOP;
END
$$;

