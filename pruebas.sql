BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE transaccion CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE factura CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE detalle_pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE inventario CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE productos CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE categoria CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE cliente CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE empleado CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE cargo CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE sucursal CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE ciudad CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE tipo_transaccion CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE estado_pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE reporte_ventas CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE reporte_desempenio CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN
    NULL;
END;
/

-- CREACIÓN DE TABLAS

CREATE TABLE ciudad (
    id_ciudad NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL
);

CREATE TABLE sucursal (
    id_sucursal NUMBER PRIMARY KEY,
    nombre_sucursal VARCHAR2(100) NOT NULL,
    direccion VARCHAR2(200),
    id_ciudad NUMBER,
    FOREIGN KEY (id_ciudad) REFERENCES ciudad(id_ciudad) ON DELETE SET NULL
);

CREATE TABLE empleado (
    id_empleado NUMBER PRIMARY KEY,
    p_nombre VARCHAR2(50) NOT NULL,
    s_nombre VARCHAR2(50),
    p_apellido VARCHAR2(50) NOT NULL,
    s_apellido VARCHAR2(50),
    salario NUMBER(10, 2) NOT NULL
);

CREATE TABLE cargo (
    id_cargo NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL
);

CREATE TABLE cliente (
    id_cliente NUMBER PRIMARY KEY,
    rut VARCHAR2(20) NOT NULL,
    p_nombre VARCHAR2(50) NOT NULL,
    s_nombre VARCHAR2(50),
    p_apellido VARCHAR2(50) NOT NULL,
    s_apellido VARCHAR2(50),
    correo VARCHAR2(100),
    telefono VARCHAR2(20)
);

CREATE TABLE categoria (
    id_categoria NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL
);

CREATE TABLE productos (
    id_producto NUMBER PRIMARY KEY,
    nombre VARCHAR2(100) NOT NULL,
    marca VARCHAR2(50),
    precio NUMBER(10, 2) NOT NULL,
    stock NUMBER NOT NULL,
    id_categoria NUMBER,
    FOREIGN KEY (id_categoria) REFERENCES categoria(id_categoria) ON DELETE SET NULL
);

CREATE TABLE inventario (
    id_inventario NUMBER PRIMARY KEY,
    fecha_actualizacion DATE NOT NULL,
    id_sucursal NUMBER,
    FOREIGN KEY (id_sucursal) REFERENCES sucursal(id_sucursal) ON DELETE CASCADE
);

CREATE TABLE pedido (
    id_pedido NUMBER PRIMARY KEY,
    fecha_pedido DATE NOT NULL,
    id_cliente NUMBER,
    id_empleado NUMBER,
    FOREIGN KEY (id_cliente) REFERENCES cliente(id_cliente) ON DELETE CASCADE,
    FOREIGN KEY (id_empleado) REFERENCES empleado(id_empleado) ON DELETE SET NULL
);

CREATE TABLE estado_pedido (
    id_estado_pedido NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL
);

CREATE TABLE detalle_pedido (
    id_detalle_pedido NUMBER PRIMARY KEY,
    id_pedido NUMBER,
    id_producto NUMBER,
    cantidad NUMBER NOT NULL,
    subtotal NUMBER(10, 2) NOT NULL,
    FOREIGN KEY (id_pedido) REFERENCES pedido(id_pedido) ON DELETE CASCADE,
    FOREIGN KEY (id_producto) REFERENCES productos(id_producto) ON DELETE CASCADE
);

CREATE TABLE factura (
    id_factura NUMBER PRIMARY KEY,
    total NUMBER(10, 2) NOT NULL,
    fecha_emision DATE NOT NULL,
    id_pedido NUMBER,
    FOREIGN KEY (id_pedido) REFERENCES pedido(id_pedido) ON DELETE CASCADE
);

CREATE TABLE transaccion (
    id_transaccion NUMBER PRIMARY KEY,
    monto NUMBER(10, 2) NOT NULL,
    fecha_transaccion DATE NOT NULL,
    id_factura NUMBER,
    FOREIGN KEY (id_factura) REFERENCES factura(id_factura) ON DELETE CASCADE
);

CREATE TABLE tipo_transaccion (
    id_tipo_transaccion NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL
);

CREATE TABLE reporte_ventas (
    id_reportes NUMBER PRIMARY KEY,
    fecha_generacion DATE NOT NULL,
    total_ventas NUMBER(10, 2) NOT NULL
);

CREATE TABLE reporte_desempenio (
    id_reporte_desempenio NUMBER PRIMARY KEY,
    fecha_generacion DATE NOT NULL,
    datos_evaluacion VARCHAR2(500) NOT NULL
);

-- Categorías
INSERT INTO categoria (id_categoria, descripcion) VALUES (1, 'Herramientas Manuales');
INSERT INTO categoria (id_categoria, descripcion) VALUES (2, 'Herramientas Eléctricas');
INSERT INTO categoria (id_categoria, descripcion) VALUES (3, 'Materiales Básicos');
INSERT INTO categoria (id_categoria, descripcion) VALUES (4, 'Acabados');
INSERT INTO categoria (id_categoria, descripcion) VALUES (5, 'Equipos de Seguridad');
INSERT INTO categoria (id_categoria, descripcion) VALUES (6, 'Accesorios Varios');

-- Productos
-- Herramientas Manuales
INSERT INTO productos VALUES (1, 'Martillo', 'Tramontina', 7500, 50, 1);
INSERT INTO productos VALUES (2, 'Destornillador', 'Stanley', 3500, 60, 1);
INSERT INTO productos VALUES (3, 'Llave', 'Bahco', 4800, 40, 1);

-- Herramientas Eléctricas
INSERT INTO productos VALUES (4, 'Taladro', 'Bosch', 45000, 20, 2);
INSERT INTO productos VALUES (5, 'Sierra', 'Makita', 62000, 15, 2);
INSERT INTO productos VALUES (6, 'Lijadora', 'Dewalt', 39000, 25, 2);

-- Materiales Básicos
INSERT INTO productos VALUES (7, 'Cemento', 'Melón', 5500, 200, 3);
INSERT INTO productos VALUES (8, 'Arena', 'Granel', 3000, 300, 3);
INSERT INTO productos VALUES (9, 'Ladrillos', 'Ladrillera Sur', 120, 1000, 3);

-- Acabados
INSERT INTO productos VALUES (10, 'Pintura', 'Ceresita', 15000, 80, 4);
INSERT INTO productos VALUES (11, 'Barniz', 'Sipa', 9500, 60, 4);
INSERT INTO productos VALUES (12, 'Cerámico', 'Cordillera', 10000, 100, 4);

-- Equipos de Seguridad
INSERT INTO productos VALUES (13, 'Casco', '3M', 7500, 50, 5);
INSERT INTO productos VALUES (14, 'Guantes', 'Ansell', 2500, 100, 5);
INSERT INTO productos VALUES (15, 'Lentes de Seguridad', '3M', 3200, 70, 5);

-- Accesorios Varios
INSERT INTO productos VALUES (16, 'Tornillos', 'Fixser', 500, 1000, 6);
INSERT INTO productos VALUES (17, 'Anclajes', 'Hilti', 900, 500, 6);
INSERT INTO productos VALUES (18, 'Fijaciones', 'Sika', 700, 400, 6);
INSERT INTO productos VALUES (19, 'Adhesivos', 'Sika', 3000, 250, 6);
INSERT INTO productos VALUES (20, 'Equipos de Medición', 'Stanley', 22000, 30, 6);

-- Ciudades
INSERT INTO ciudad VALUES (1, 'Santiago');
INSERT INTO ciudad VALUES (2, 'Valparaíso');
INSERT INTO ciudad VALUES (3, 'Concepción');
INSERT INTO ciudad VALUES (4, 'La Serena');
INSERT INTO ciudad VALUES (5, 'Antofagasta');

-- Sucursales
INSERT INTO sucursal VALUES (1, 'Sucursal Central', 'Av. Libertador 1001', 1);
INSERT INTO sucursal VALUES (2, 'Sucursal Puerto', 'Calle Bellavista 203', 2);
INSERT INTO sucursal VALUES (3, 'Sucursal Sur', 'Camino a Penco 455', 3);

-- Cargos
INSERT INTO cargo VALUES (1, 'Vendedor');
INSERT INTO cargo VALUES (2, 'Bodeguero');
INSERT INTO cargo VALUES (3, 'Administrador');

-- Empleados
INSERT INTO empleado VALUES (1, 'Pedro', 'Luis', 'Gómez', 'Pérez', 750000);
INSERT INTO empleado VALUES (2, 'Ana', 'María', 'Rojas', 'Soto', 820000);
INSERT INTO empleado VALUES (3, 'Carlos', NULL, 'Muñoz', 'Fuentes', 910000);

-- Clientes
INSERT INTO cliente VALUES (1, '12.345.678-9', 'Juan', NULL, 'Pérez', 'López', 'juanperez@mail.com', '+56912345678');
INSERT INTO cliente VALUES (2, '23.456.789-0', 'Claudia', 'Andrea', 'Gutiérrez', 'Ramírez', 'claudia@mail.com', '+56987654321');
INSERT INTO cliente VALUES (3, '34.567.890-1', 'Marco', NULL, 'Salinas', 'Torres', 'msalinas@mail.com', '+56910293847');

-- Inventarios
INSERT INTO inventario VALUES (1, TO_DATE('2024-06-10', 'YYYY-MM-DD'), 1);
INSERT INTO inventario VALUES (2, TO_DATE('2024-06-10', 'YYYY-MM-DD'), 2);

-- Estado Pedido
INSERT INTO estado_pedido VALUES (1, 'Pendiente');
INSERT INTO estado_pedido VALUES (2, 'En proceso');
INSERT INTO estado_pedido VALUES (3, 'Enviado');
INSERT INTO estado_pedido VALUES (4, 'Completado');

-- Pedidos
INSERT INTO pedido VALUES (1, TO_DATE('2024-06-05', 'YYYY-MM-DD'), 1, 1);
INSERT INTO pedido VALUES (2, TO_DATE('2024-06-06', 'YYYY-MM-DD'), 2, 2);
INSERT INTO pedido VALUES (3, TO_DATE('2024-06-07', 'YYYY-MM-DD'), 3, 3);

-- Detalles de Pedido
INSERT INTO detalle_pedido VALUES (1, 1, 1, 2, 15000); -- Martillo x2
INSERT INTO detalle_pedido VALUES (2, 1, 4, 1, 45000); -- Taladro x1
INSERT INTO detalle_pedido VALUES (3, 2, 7, 10, 55000); -- Cemento x10
INSERT INTO detalle_pedido VALUES (4, 2, 10, 3, 45000); -- Pintura x3
INSERT INTO detalle_pedido VALUES (5, 3, 16, 100, 50000); -- Tornillos x100

-- Facturas
INSERT INTO factura VALUES (1, 60000, TO_DATE('2024-06-06', 'YYYY-MM-DD'), 1);
INSERT INTO factura VALUES (2, 100000, TO_DATE('2024-06-07', 'YYYY-MM-DD'), 2);
INSERT INTO factura VALUES (3, 50000, TO_DATE('2024-06-08', 'YYYY-MM-DD'), 3);

-- Transacciones
INSERT INTO transaccion VALUES (1, 60000, TO_DATE('2024-06-06', 'YYYY-MM-DD'), 1);
INSERT INTO transaccion VALUES (2, 100000, TO_DATE('2024-06-07', 'YYYY-MM-DD'), 2);
INSERT INTO transaccion VALUES (3, 50000, TO_DATE('2024-06-08', 'YYYY-MM-DD'), 3);

-- Tipos de Transacción
INSERT INTO tipo_transaccion VALUES (1, 'Efectivo');
INSERT INTO tipo_transaccion VALUES (2, 'Tarjeta de Débito');
INSERT INTO tipo_transaccion VALUES (3, 'Tarjeta de Crédito');

-- Reportes de ventas
INSERT INTO reporte_ventas VALUES (1, TO_DATE('2024-06-08', 'YYYY-MM-DD'), 210000);

-- Reportes de desempeño
INSERT INTO reporte_desempenio VALUES (1, TO_DATE('2024-06-08', 'YYYY-MM-DD'), 'Evaluación semanal: ventas altas, cumplimiento 95%');

