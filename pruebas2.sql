BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE reporte_desempenio CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE reporte_ventas CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE transaccion CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE factura CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE detalle_pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE pedido CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE log_actividad_inventario CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE stock_sucursal CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE productos CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE cliente CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE empleado CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE sucursal CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE categoria CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE cargo CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE ciudad CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE tipo_transaccion CASCADE CONSTRAINTS';
  EXECUTE IMMEDIATE 'DROP TABLE estado_pedido CASCADE CONSTRAINTS';
EXCEPTION
  WHEN OTHERS THEN
    NULL;
END;
/

-- CREACIÓN DE TABLAS

CREATE TABLE ciudad (
    id_ciudad NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL UNIQUE
);

CREATE TABLE cargo (
    id_cargo NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL UNIQUE
);

CREATE TABLE categoria (
    id_categoria NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL UNIQUE
);

CREATE TABLE estado_pedido (
    id_estado_pedido NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL UNIQUE
);

CREATE TABLE tipo_transaccion (
    id_tipo_transaccion NUMBER PRIMARY KEY,
    descripcion VARCHAR2(100) NOT NULL UNIQUE
);

CREATE TABLE sucursal (
    id_sucursal NUMBER PRIMARY KEY,
    nombre_sucursal VARCHAR2(100) NOT NULL UNIQUE,
    direccion VARCHAR2(200),
    id_ciudad NUMBER,
    FOREIGN KEY (id_ciudad) REFERENCES ciudad(id_ciudad) ON DELETE SET NULL
);

CREATE TABLE empleado (
    id_empleado NUMBER PRIMARY KEY,
    rut VARCHAR2(20) NOT NULL UNIQUE,
    p_nombre VARCHAR2(50) NOT NULL,
    s_nombre VARCHAR2(50),
    p_apellido VARCHAR2(50) NOT NULL,
    s_apellido VARCHAR2(50),
    correo VARCHAR2(100) NOT NULL UNIQUE,
    telefono VARCHAR2(20),
    salario NUMBER(10, 2) NOT NULL CHECK (salario > 0),
    id_cargo NUMBER,
    id_sucursal NUMBER,
    clave_hash VARCHAR2(256) NOT NULL,
    activo CHAR(1) DEFAULT 'S' NOT NULL CHECK (activo IN ('S', 'N')),
    FOREIGN KEY (id_cargo) REFERENCES cargo(id_cargo) ON DELETE SET NULL,
    FOREIGN KEY (id_sucursal) REFERENCES sucursal(id_sucursal) ON DELETE SET NULL
);

CREATE TABLE cliente (
    id_cliente NUMBER PRIMARY KEY,
    p_nombre VARCHAR2(50) NOT NULL,
    s_nombre VARCHAR2(50),
    p_apellido VARCHAR2(50) NOT NULL,
    s_apellido VARCHAR2(50),
    correo VARCHAR2(100) NOT NULL UNIQUE,
    telefono VARCHAR2(20),
    clave_hash VARCHAR2(256) NOT NULL,
    activo CHAR(1) DEFAULT 'S' NOT NULL CHECK (activo IN ('S', 'N'))
);

CREATE TABLE productos (
    id_producto NUMBER PRIMARY KEY,
    nombre VARCHAR2(100) NOT NULL,
    marca VARCHAR2(50),
    descripcion_detallada VARCHAR2(500),
    precio NUMBER(10, 2) NOT NULL CHECK (precio > 0),
    id_categoria NUMBER,
    imagen_url VARCHAR2(255),
    FOREIGN KEY (id_categoria) REFERENCES categoria(id_categoria) ON DELETE SET NULL
);

CREATE TABLE stock_sucursal (
    id_stock_sucursal NUMBER PRIMARY KEY,
    id_producto NUMBER NOT NULL,
    id_sucursal NUMBER NOT NULL,
    cantidad NUMBER NOT NULL CHECK (cantidad >= 0),
    ubicacion_bodega VARCHAR2(100),
    fecha_ultima_actualizacion DATE DEFAULT SYSDATE NOT NULL,
    FOREIGN KEY (id_producto) REFERENCES productos(id_producto) ON DELETE CASCADE,
    FOREIGN KEY (id_sucursal) REFERENCES sucursal(id_sucursal) ON DELETE CASCADE,
    CONSTRAINT uq_stock_prod_suc UNIQUE (id_producto, id_sucursal)
);

CREATE TABLE log_actividad_inventario (
    id_log NUMBER PRIMARY KEY,
    tipo_actividad VARCHAR2(100) NOT NULL,
    id_producto NUMBER,
    id_sucursal NUMBER,
    cantidad_afectada NUMBER,
    stock_anterior NUMBER,
    stock_nuevo NUMBER,
    fecha_actividad DATE DEFAULT SYSDATE NOT NULL,
    id_empleado_responsable NUMBER,
    notas VARCHAR2(500),
    FOREIGN KEY (id_producto) REFERENCES productos(id_producto) ON DELETE SET NULL,
    FOREIGN KEY (id_sucursal) REFERENCES sucursal(id_sucursal) ON DELETE SET NULL,
    FOREIGN KEY (id_empleado_responsable) REFERENCES empleado(id_empleado) ON DELETE SET NULL
);

CREATE TABLE pedido (
    id_pedido NUMBER PRIMARY KEY,
    fecha_pedido DATE DEFAULT SYSDATE NOT NULL,
    id_cliente NUMBER,
    id_empleado_vendedor NUMBER,
    id_sucursal_origen NUMBER,
    id_estado_pedido NUMBER NOT NULL,
    total_pedido NUMBER(12,2) DEFAULT 0 NOT NULL,
    FOREIGN KEY (id_cliente) REFERENCES cliente(id_cliente) ON DELETE SET NULL,
    FOREIGN KEY (id_empleado_vendedor) REFERENCES empleado(id_empleado) ON DELETE SET NULL,
    FOREIGN KEY (id_sucursal_origen) REFERENCES sucursal(id_sucursal) ON DELETE SET NULL,
    FOREIGN KEY (id_estado_pedido) REFERENCES estado_pedido(id_estado_pedido) -- ON DELETE RESTRICT es default
);

CREATE TABLE detalle_pedido (
    id_detalle_pedido NUMBER PRIMARY KEY,
    id_pedido NUMBER NOT NULL,
    id_producto NUMBER NOT NULL,
    cantidad NUMBER NOT NULL CHECK (cantidad > 0),
    precio_unitario_venta NUMBER(10,2) NOT NULL,
    subtotal NUMBER(12, 2) NOT NULL,
    FOREIGN KEY (id_pedido) REFERENCES pedido(id_pedido) ON DELETE CASCADE,
    FOREIGN KEY (id_producto) REFERENCES productos(id_producto) -- ON DELETE RESTRICT es default
);

CREATE TABLE factura (
    id_factura NUMBER PRIMARY KEY,
    numero_factura VARCHAR2(50) UNIQUE NOT NULL,
    id_pedido NUMBER UNIQUE NOT NULL,
    fecha_emision DATE DEFAULT SYSDATE NOT NULL,
    total_neto NUMBER(12, 2) NOT NULL,
    iva NUMBER(10,2) NOT NULL,
    total_con_iva NUMBER(12, 2) NOT NULL,
    FOREIGN KEY (id_pedido) REFERENCES pedido(id_pedido) -- ON DELETE RESTRICT es default
);

CREATE TABLE transaccion (
    id_transaccion NUMBER PRIMARY KEY,
    id_factura NUMBER NOT NULL,
    id_tipo_transaccion NUMBER NOT NULL,
    monto_pagado NUMBER(12, 2) NOT NULL,
    fecha_transaccion DATE DEFAULT SYSDATE NOT NULL,
    referencia_pago VARCHAR2(100),
    id_empleado_cajero NUMBER,
    FOREIGN KEY (id_factura) REFERENCES factura(id_factura) ON DELETE CASCADE,
    FOREIGN KEY (id_tipo_transaccion) REFERENCES tipo_transaccion(id_tipo_transaccion), -- ON DELETE RESTRICT es default
    FOREIGN KEY (id_empleado_cajero) REFERENCES empleado(id_empleado) ON DELETE SET NULL
);

CREATE TABLE reporte_ventas (
    id_reporte_ventas NUMBER PRIMARY KEY,
    fecha_generacion DATE NOT NULL,
    periodo_inicio DATE,
    periodo_fin DATE,
    total_ventas_calculado NUMBER(14, 2) NOT NULL,
    id_sucursal NUMBER,
    FOREIGN KEY (id_sucursal) REFERENCES sucursal(id_sucursal) ON DELETE SET NULL
);

CREATE TABLE reporte_desempenio (
    id_reporte_desempenio NUMBER PRIMARY KEY,
    id_empleado NUMBER NOT NULL,
    fecha_generacion DATE NOT NULL,
    periodo_evaluacion_inicio DATE NOT NULL,
    periodo_evaluacion_fin DATE NOT NULL,
    datos_evaluacion VARCHAR2(1000) NOT NULL,
    FOREIGN KEY (id_empleado) REFERENCES empleado(id_empleado) ON DELETE CASCADE
);

INSERT INTO ciudad VALUES (1, 'Santiago');
INSERT INTO ciudad VALUES (2, 'Valparaíso');
INSERT INTO ciudad VALUES (3, 'Concepción');
INSERT INTO ciudad VALUES (4, 'La Serena');
INSERT INTO ciudad VALUES (5, 'Antofagasta');

INSERT INTO cargo VALUES (1, 'Vendedor');
INSERT INTO cargo VALUES (2, 'Bodeguero');
INSERT INTO cargo VALUES (3, 'Administrador de Sucursal');
INSERT INTO cargo VALUES (4, 'Cajero');

INSERT INTO sucursal VALUES (1, 'Ferremas Central Santiago', 'Av. Principal 123, Santiago', 1);
INSERT INTO sucursal VALUES (2, 'Ferremas Puerto Valpo', 'Calle del Puerto 456, Valparaíso', 2);
INSERT INTO sucursal VALUES (3, 'Ferremas Sur Conce', 'Camino a Penco 455, Concepción', 3);

INSERT INTO empleado VALUES (1, '11.111.111-1', 'Carlos', 'Alberto', 'Pérez', 'Gómez', 'cperez@ferremas.com', '+56911111111', 750000, 1, 1, 'hash_temporal_empleado1', 'S');
INSERT INTO empleado VALUES (2, '22.222.222-2', 'Ana', 'Isabel', 'López', 'Mora', 'alopez@ferremas.com', '+56922222222', 650000, 2, 1, 'hash_temporal_empleado2', 'S');
INSERT INTO empleado VALUES (3, '33.333.333-3', 'Luisa', NULL, 'Martínez', 'Silva', 'lmartinez@ferremas.com', '+56933333333', 900000, 3, 1, 'hash_temporal_empleado3', 'S');
INSERT INTO empleado VALUES (4, '44.444.444-4', 'Jorge', 'Andrés', 'Silva', 'Rojas', 'jsilva@ferremas.com', '+56944444444', 700000, 4, 2, 'hash_temporal_empleado4', 'S');
INSERT INTO empleado VALUES (5, '55.555.555-5', 'Sofia', 'Elena', 'Contreras', 'Diaz', 'scontreras@ferremas.com', '+56955555555', 680000, 2, 2, 'hash_temporal_empleado5', 'S');

INSERT INTO cliente VALUES (1, 'Juan', NULL, 'Pérez', 'López', 'juan.perez@cliente.com', '+56912345678', 'hash_temporal_cliente1', 'S');
INSERT INTO cliente VALUES (2, 'Claudia', 'Andrea', 'Gutiérrez', 'Ramírez', 'claudia.gutierrez@cliente.com', '+56987654321', 'hash_temporal_cliente2', 'S');
INSERT INTO cliente VALUES (3, 'Marco', NULL, 'Salinas', 'Torres', 'msalinas@cliente.com', '+56910293847', 'hash_temporal_cliente3', 'S');

INSERT INTO categoria VALUES (1, 'Herramientas Manuales');
INSERT INTO categoria VALUES (2, 'Herramientas Eléctricas');
INSERT INTO categoria VALUES (3, 'Materiales Básicos de Construcción');
INSERT INTO categoria VALUES (4, 'Acabados y Pinturas');
INSERT INTO categoria VALUES (5, 'Equipos de Seguridad Personal');
INSERT INTO categoria VALUES (6, 'Fijaciones y Adhesivos');
INSERT INTO categoria VALUES (7, 'Accesorios y Medición');

INSERT INTO productos VALUES (1, 'Martillo Carpintero 20oz', 'Stanley', 'Martillo de uña curva con mango de fibra de vidrio antivibración.', 12500, 1, 'martillo_carpintero.jpg');
INSERT INTO productos VALUES (2, 'Destornillador Phillips PH2x100mm', 'Irwin', 'Punta magnética, mango ergonómico bimaterial.', 3500, 1, 'destornillador_phillips.png');
INSERT INTO productos VALUES (3, 'Llave Ajustable Cromada 8"', 'Bahco', 'Acero cromo vanadio, alta resistencia.', 14800, 1, 'llave_ajustable_bahco.jpg');
INSERT INTO productos VALUES (4, 'Taladro Percutor Inalámbrico 18V', 'Bosch', 'Incluye 2 baterías Litio-Ion y maletín.', 79990, 2, 'taladro_bosch_18v.jpg');
INSERT INTO productos VALUES (5, 'Sierra Circular 7 1/4" 1500W', 'Makita', 'Guía láser, disco de 24 dientes.', 62000, 2, 'sierra_makita.jpg');
INSERT INTO productos VALUES (6, 'Lijadora Orbital 1/4 Hoja', 'Dewalt', 'Potente motor, bolsa recolectora de polvo.', 39000, 2, 'lijadora_orbital_dewalt.jpg');
INSERT INTO productos VALUES (7, 'Cemento Portland Especial 25kg', 'Melón', 'Alta resistencia inicial y final, uso general.', 5500, 3, 'cemento_melon.jpg');
INSERT INTO productos VALUES (8, 'Arena Fina Saco 25kg', 'Generica', 'Arena harneada para estucos y morteros finos.', 3000, 3, 'arena_fina.jpg');
INSERT INTO productos VALUES (9, 'Ladrillo Fiscal Estándar', 'Princesa', 'Ladrillo cerámico para albañilería.', 120, 3, 'ladrillo_fiscal.jpg');
INSERT INTO productos VALUES (10, 'Pintura Látex Extracubriente Blanca 1 Galón', 'Sipa', 'Para interiores y exteriores, lavable, antihongos.', 18900, 4, 'pintura_latex_sipa.jpg');
INSERT INTO productos VALUES (11, 'Barniz Marino Brillante 1/4 Galón', 'Sipa', 'Protección UV, alta durabilidad para maderas exteriores.', 9500, 4, 'barniz_marino_sipa.jpg');
INSERT INTO productos VALUES (12, 'Cerámico Muro Blanco Brillante 30x60cm (caja 1.44m2)', 'Cordillera', 'Revestimiento cerámico para muros interiores.', 10000, 4, 'ceramico_blanco.jpg');
INSERT INTO productos VALUES (13, 'Casco de Seguridad Amarillo con Ajuste Ratchet', '3M', 'Certificado, alta visibilidad, suspensión de 4 puntos, ajuste fácil.', 8500, 5, 'casco_3m.jpg');
INSERT INTO productos VALUES (14, 'Guantes de Cabritilla Reforzados Multiuso', 'Ansell', 'Cuero flexible, alta destreza y protección.', 2500, 5, 'guantes_ansell.jpg');
INSERT INTO productos VALUES (15, 'Lentes de Seguridad Claros Antirayadura', '3M', 'Protección UV, diseño envolvente, patillas ajustables.', 3200, 5, 'lentes_seguridad_claros.jpg');
INSERT INTO productos VALUES (16, 'Caja 100 Tornillos Volcanita 6x1" Punta Aguda', 'Mamut', 'Fosfatados para mayor durabilidad, autorroscantes.', 3800, 6, 'tornillos_volcanita_mamut.jpg');
INSERT INTO productos VALUES (17, 'Set Tarugos Fischer SX6 con Tornillo (50u)', 'Fischer', 'Para fijaciones seguras en diversos materiales.', 3900, 6, 'tarugos_fischer.jpg');
INSERT INTO productos VALUES (18, 'Silicona Multiuso Transparente Antihongos 280ml', 'Sika', 'Sellador elástico para baños, cocinas y exteriores.', 2700, 6, 'silicona_sika.jpg');
INSERT INTO productos VALUES (19, 'Adhesivo Montaje Extra Fuerte Sikaflex 11FC', 'Sika', 'Sellador y adhesivo elástico de poliuretano.', 4500, 6, 'adhesivo_montaje_sika.jpg');
INSERT INTO productos VALUES (20, 'Huincha de Medir Profesional 5m x 19mm', 'Stanley', 'Cinta metálica con freno PowerLock, carcasa ABS alto impacto.', 7200, 7, 'huincha_medir_stanley.jpg');

INSERT INTO stock_sucursal VALUES (1, 1, 1, 50, 'Pasillo A1', SYSDATE);
INSERT INTO stock_sucursal VALUES (2, 4, 1, 20, 'Estante B3', SYSDATE);
INSERT INTO stock_sucursal VALUES (3, 7, 1, 100, 'Patio Trasero', SYSDATE);
INSERT INTO stock_sucursal VALUES (4, 1, 2, 30, 'Bodega Principal', SYSDATE);
INSERT INTO stock_sucursal VALUES (5, 4, 2, 15, 'Sector Eléctricas', SYSDATE);
INSERT INTO stock_sucursal VALUES (6, 10, 1, 40, 'Zona Pinturas', SYSDATE);
INSERT INTO stock_sucursal VALUES (7, 2, 1, 60, 'Pasillo A2', SYSDATE);
INSERT INTO stock_sucursal VALUES (8, 13, 1, 25, 'EPP Estante 1', SYSDATE);
INSERT INTO stock_sucursal VALUES (9, 13, 2, 15, 'Seguridad', SYSDATE);
INSERT INTO stock_sucursal VALUES (10, 16, 1, 200, 'Fijaciones Caja 3', SYSDATE);

INSERT INTO log_actividad_inventario VALUES (1, 'Conteo Inicial', 1, 1, 50, 0, 50, TO_DATE('2024-05-01', 'YYYY-MM-DD'), 2, 'Stock inicial Martillo Carpintero Suc.1');
INSERT INTO log_actividad_inventario VALUES (2, 'Recepción Pedido Proveedor', 4, 1, 10, 10, 20, TO_DATE('2024-05-10', 'YYYY-MM-DD'), 2, 'Recepción de taladros Bosch');
INSERT INTO log_actividad_inventario VALUES (3, 'Venta Cliente', 1, 1, -2, 50, 48, TO_DATE('2024-05-20', 'YYYY-MM-DD'), 1, 'Venta Pedido #1');

INSERT INTO estado_pedido VALUES (1, 'Pendiente de Confirmación');
INSERT INTO estado_pedido VALUES (2, 'En Preparación');
INSERT INTO estado_pedido VALUES (3, 'Listo para Despacho/Retiro');
INSERT INTO estado_pedido VALUES (4, 'Despachado');
INSERT INTO estado_pedido VALUES (5, 'Entregado');
INSERT INTO estado_pedido VALUES (6, 'Facturado');
INSERT INTO estado_pedido VALUES (7, 'Pagado');
INSERT INTO estado_pedido VALUES (8, 'Cancelado');

INSERT INTO pedido VALUES (1, TO_DATE('2024-05-20', 'YYYY-MM-DD'), 1, 1, 1, 2, 92490);

INSERT INTO detalle_pedido VALUES (1, 1, 1, 1, 12500, 12500);
INSERT INTO detalle_pedido VALUES (2, 1, 4, 1, 79990, 79990);

INSERT INTO factura VALUES (1, 'F001-00001', 1, TO_DATE('2024-05-21', 'YYYY-MM-DD'), 77723, 14767, 92490);

INSERT INTO tipo_transaccion VALUES (1, 'Efectivo');
INSERT INTO tipo_transaccion VALUES (2, 'Tarjeta de Débito');
INSERT INTO tipo_transaccion VALUES (3, 'Tarjeta de Crédito');
INSERT INTO tipo_transaccion VALUES (4, 'Transferencia Bancaria');

INSERT INTO transaccion VALUES (1, 1, 2, 92490, TO_DATE('2024-05-21', 'YYYY-MM-DD'), 'Comp#778899', 4);

INSERT INTO reporte_ventas VALUES (1, TO_DATE('2024-06-01', 'YYYY-MM-DD'), TO_DATE('2024-05-01', 'YYYY-MM-DD'), TO_DATE('2024-05-31', 'YYYY-MM-DD'), 2500000, 1);

INSERT INTO reporte_desempenio VALUES (1, 1, TO_DATE('2024-06-01', 'YYYY-MM-DD'), TO_DATE('2024-05-01', 'YYYY-MM-DD'), TO_DATE('2024-05-31', 'YYYY-MM-DD'), 'Excelente desempeño en ventas, superó metas.');

COMMIT;