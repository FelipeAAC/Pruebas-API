-- Eliminar tablas en orden correcto (por relaciones FK)
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE Transaccion CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Factura CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Detalle_Pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Inventario CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Productos CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Categoria CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Cliente CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Empleado CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Cargo CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Sucursal CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Ciudad CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Tipo_Transaccion CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Estado_Pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Reporte_Ventas CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE Reporte_Desempenio CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN
    NULL;
END;
/

-- CREACIÓN DE TABLAS

CREATE TABLE Ciudad (
    id_ciudad NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100)
);

CREATE TABLE Sucursal (
    id_sucursal NUMBER PRIMARY KEY,
    nombre_sucursal VARCHAR2(100),
    direccion VARCHAR2(200),
    id_ciudad NUMBER,
    FOREIGN KEY (id_ciudad) REFERENCES Ciudad(id_ciudad)
);

CREATE TABLE Empleado (
    id_empleado NUMBER PRIMARY KEY,
    p_nombre VARCHAR2(50),
    s_nombre VARCHAR2(50),
    p_apellido VARCHAR2(50),
    s_apellido VARCHAR2(50),
    salario NUMBER
);

CREATE TABLE Cargo (
    id_cargo NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100)
);

CREATE TABLE Cliente (
    id_cliente NUMBER PRIMARY KEY,
    rut VARCHAR2(20),
    p_nombre VARCHAR2(50),
    s_nombre VARCHAR2(50),
    p_apellido VARCHAR2(50),
    s_apellido VARCHAR2(50),
    correo VARCHAR2(100),
    telefono VARCHAR2(20)
);

CREATE TABLE Categoria (
    id_categoria NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100)
);

CREATE TABLE Productos (
    id_producto NUMBER PRIMARY KEY,
    nombre VARCHAR2(100),
    marca VARCHAR2(50),
    precio NUMBER,
    stock NUMBER,
    id_categoria NUMBER,
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria)
);

CREATE TABLE Inventario (
    id_inventario NUMBER PRIMARY KEY,
    fecha_actualizacion DATE,
    id_sucursal NUMBER,
    FOREIGN KEY (id_sucursal) REFERENCES Sucursal(id_sucursal)
);

CREATE TABLE Pedido (
    id_pedido NUMBER PRIMARY KEY,
    fecha_pedido DATE,
    id_cliente NUMBER,
    id_empleado NUMBER,
    FOREIGN KEY (id_cliente) REFERENCES Cliente(id_cliente),
    FOREIGN KEY (id_empleado) REFERENCES Empleado(id_empleado)
);

CREATE TABLE Estado_Pedido (
    id_estado_pedido NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100)
);

CREATE TABLE Detalle_Pedido (
    id_detalle_pedido NUMBER PRIMARY KEY,
    id_pedido NUMBER,
    id_producto NUMBER,
    cantidad NUMBER,
    subtotal NUMBER,
    FOREIGN KEY (id_pedido) REFERENCES Pedido(id_pedido),
    FOREIGN KEY (id_producto) REFERENCES Productos(id_producto)
);

CREATE TABLE Factura (
    id_factura NUMBER PRIMARY KEY,
    total NUMBER,
    fecha_emision DATE,
    id_pedido NUMBER,
    FOREIGN KEY (id_pedido) REFERENCES Pedido(id_pedido)
);

CREATE TABLE Transaccion (
    id_transaccion NUMBER PRIMARY KEY,
    monto NUMBER,
    fecha_transaccion DATE,
    id_factura NUMBER,
    FOREIGN KEY (id_factura) REFERENCES Factura(id_factura)
);

CREATE TABLE Tipo_Transaccion (
    id_tipo_transaccion NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100)
);

CREATE TABLE Reporte_Ventas (
    id_reportes NUMBER PRIMARY KEY,
    fecha_generacion DATE,
    total_ventas NUMBER
);

CREATE TABLE Reporte_Desempenio (
    id_reporte_desempenio NUMBER PRIMARY KEY,
    fecha_generacion DATE,
    datos_evaluacion VARCHAR2(500)
);

-- INSERTS

-- Categorías
INSERT INTO Categoria (id_categoria, descripcion) VALUES (1, 'Herramientas Manuales');
INSERT INTO Categoria (id_categoria, descripcion) VALUES (2, 'Herramientas Eléctricas');
INSERT INTO Categoria (id_categoria, descripcion) VALUES (3, 'Materiales Básicos');
INSERT INTO Categoria (id_categoria, descripcion) VALUES (4, 'Acabados');
INSERT INTO Categoria (id_categoria, descripcion) VALUES (5, 'Equipos de Seguridad');
INSERT INTO Categoria (id_categoria, descripcion) VALUES (6, 'Accesorios Varios');

-- Productos
-- Herramientas Manuales
INSERT INTO Productos VALUES (1, 'Martillo', 'Tramontina', 7500, 50, 1);
INSERT INTO Productos VALUES (2, 'Destornillador', 'Stanley', 3500, 60, 1);
INSERT INTO Productos VALUES (3, 'Llave', 'Bahco', 4800, 40, 1);

-- Herramientas Eléctricas
INSERT INTO Productos VALUES (4, 'Taladro', 'Bosch', 45000, 20, 2);
INSERT INTO Productos VALUES (5, 'Sierra', 'Makita', 62000, 15, 2);
INSERT INTO Productos VALUES (6, 'Lijadora', 'Dewalt', 39000, 25, 2);

-- Materiales Básicos
INSERT INTO Productos VALUES (7, 'Cemento', 'Melón', 5500, 200, 3);
INSERT INTO Productos VALUES (8, 'Arena', 'Granel', 3000, 300, 3);
INSERT INTO Productos VALUES (9, 'Ladrillos', 'Ladrillera Sur', 120, 1000, 3);

-- Acabados
INSERT INTO Productos VALUES (10, 'Pintura', 'Ceresita', 15000, 80, 4);
INSERT INTO Productos VALUES (11, 'Barniz', 'Sipa', 9500, 60, 4);
INSERT INTO Productos VALUES (12, 'Cerámico', 'Cordillera', 10000, 100, 4);

-- Equipos de Seguridad
INSERT INTO Productos VALUES (13, 'Casco', '3M', 7500, 50, 5);
INSERT INTO Productos VALUES (14, 'Guantes', 'Ansell', 2500, 100, 5);
INSERT INTO Productos VALUES (15, 'Lentes de Seguridad', '3M', 3200, 70, 5);

-- Accesorios Varios
INSERT INTO Productos VALUES (16, 'Tornillos', 'Fixser', 500, 1000, 6);
INSERT INTO Productos VALUES (17, 'Anclajes', 'Hilti', 900, 500, 6);
INSERT INTO Productos VALUES (18, 'Fijaciones', 'Sika', 700, 400, 6);
INSERT INTO Productos VALUES (19, 'Adhesivos', 'Sika', 3000, 250, 6);
INSERT INTO Productos VALUES (20, 'Equipos de Medición', 'Stanley', 22000, 30, 6);
