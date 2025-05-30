from fastapi import FastAPI, HTTPException, Header, status, File, UploadFile, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import oracledb
import datetime
from typing import Optional
from datetime import date
import shutil
import os 
import logging
import asyncio

tags_metadata = [
    {
        "name": "Ciudad",
        "description": "ID CIUDAD/DESCRIPCION de la Ciudad formato CRUD",
    },
    {
        "name": "Cargo",
        "description": "ID CARGO/DESCRIPCION del Cargo formato CRUD",
    },
    {
        "name": "Categoría",
        "description": "ID CATEGORIA/DESCRIPCION de la Categoría formato CRUD",
    },
    {
        "name": "Estado Pedido",
        "description": "ID ESTADO PEDIDO/DESCRIPCION del Estado Pedido formato CRUD",
    },
    {
        "name": "Tipo Transacción",
        "description": "ID TIPO TRANSACCION/DESCRIPCION del Tipo Transacción formato CRUD",
    },
    {
        "name": "Sucursal",
        "description": "ID SUCURSAL/NOMBRE SUCURSAL/DIRECCION/ID CIUDAD de la Sucursal formato CRUD",
    },
    {
        "name": "Empleado",
        "description": "ID EMPLEADO/RUT/P NOMBRE/S NOMBRE/P APELLIDO/S APELLIDO/CORREO/TELEFONO/SALARIO/ID CARGO/ID SUCURSAL/ACTIVO del Empleado formato CRUD", # Descripción actualizada para mayor detalle.
    },
    {
        "name": "Cliente",
        "description": "ID CLIENTE/P NOMBRE/S NOMBRE/P APELLIDO/S APELLIDO/CORREO/TELEFONO/ACTIVO del Cliente formato CRUD", # Descripción actualizada según tabla, sin RUT.
    },
    {
        "name": "Productos",
        "description": "ID PRODUCTO/NOMBRE/MARCA/DESCRIPCION/PRECIO/ID CATEGORIA/IMAGEN URL de los Productos formato CRUD", # Descripción actualizada para mayor detalle.
    },
    {
        "name": "Stock Sucursal",
        "description": "ID STOCK SUCURSAL/ID PRODUCTO/ID SUCURSAL/CANTIDAD/UBICACION BODEGA/FECHA ACTUALIZACION del Stock en Sucursal formato CRUD",
    },
    {
        "name": "Log Actividad Inventario",
        "description": "ID LOG/TIPO ACTIVIDAD/ID PRODUCTO/ID SUCURSAL/CANTIDAD/STOCK ANTERIOR/STOCK NUEVO/FECHA/ID EMPLEADO/NOTAS del Log de Actividad de Inventario formato CRUD",
    },
    {
        "name": "Pedidos",
        "description": "ID PEDIDO/FECHA PEDIDO/ID CLIENTE/ID EMPLEADO VENDEDOR/ID SUCURSAL ORIGEN/ID ESTADO PEDIDO/TOTAL PEDIDO de los Pedidos formato CRUD", # Descripción actualizada para mayor detalle.
    },
    {
        "name": "Detalle Pedido",
        "description": "ID DETALLE PEDIDO/ID PEDIDO/ID PRODUCTO/CANTIDAD/PRECIO UNITARIO VENTA/SUBTOTAL de los Detalles Pedidos formato CRUD",
    },
    {
        "name": "Factura",
        "description": "ID FACTURA/NUMERO FACTURA/ID PEDIDO/FECHA EMISION/TOTAL NETO/IVA/TOTAL CON IVA de la Factura formato CRUD", # Descripción actualizada para mayor detalle.
    },
    {
        "name": "Transacción",
        "description": "ID TRANSACCION/ID FACTURA/ID TIPO TRANSACCION/MONTO PAGADO/FECHA TRANSACCION/REFERENCIA PAGO/ID EMPLEADO CAJERO de la Transacción formato CRUD",
    },
    {
        "name": "Reporte Venta",
        "description": "ID REPORTE VENTAS/FECHA GENERACION/PERIODO INICIO/PERIODO FIN/TOTAL VENTAS CALCULADO/ID SUCURSAL de los Reportes Ventas formato CRUD", # Descripción actualizada para mayor detalle.
    },
    {
        "name": "Reporte Desempeño",
        "description": "ID REPORTE DESEMPENIO/ID EMPLEADO/FECHA GENERACION/PERIODO EVALUACION INICIO/PERIODO EVALUACION FIN/DATOS EVALUACION de los Reportes Desempeños formato CRUD", # Descripción actualizada para mayor detalle.
    },
]

origins = [
    "http://localhost:8000",  # Tu frontend Django
    "http://127.0.0.1:8000", # También para Django
]

app = FastAPI(openapi_tags=tags_metadata)
logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"], # Permite todos los métodos (GET, POST, etc.)
    allow_headers=["*"], # Permite todos los encabezados
)

#Conexión con OracleDB

def get_conexion():
    conexion = oracledb.connect(
        user="prueba_api",
        password="prueba_api",
        dsn="localhost:1521/orcl"
    )
    return conexion
"""
#Conexión con OracleDB Duoc
def get_conexion():
    conexion = oracledb.connect(
        user="prueba_api",
        password="prueba_api",
        dsn="localhost:1521/orcl.duoc.com.cl"
    )
    return conexion
"""
def dict_from_row(cursor, row_tuple: Optional[tuple]):
    if not row_tuple:
        return None
    column_names = [desc[0].lower() for desc in cursor.description]
    return dict(zip(column_names, row_tuple))

# CRUD Ciudad

@app.get("/ciudadget", tags=["Ciudad"])
def listar_ciudad():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_ciudad, descripcion FROM ciudad")
        ciudades = []
        for id_ciudad, descripcion in cursor:
            ciudades.append({
                "id_ciudad": id_ciudad,
                "descripcion": descripcion
            })
        cursor.close()
        cone.close()
        return ciudades
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/ciudadgetid/{id_ciudad}", tags=["Ciudad"])
def obtener_ciudad(id_ciudad: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_ciudad, descripcion FROM ciudad WHERE id_ciudad = :id_ciudad", {"id_ciudad": id_ciudad})
        ciudad = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if ciudad:
            return {"id_ciudad": ciudad[0], "descripcion": ciudad[1]}
        else:
            raise HTTPException(status_code=404, detail="Ciudad no encontrada")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.post("/ciudadpost", tags=["Ciudad"])
def agregar_ciudad(id_ciudad: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO ciudad (id_ciudad, descripcion)
            VALUES (:id_ciudad, :descripcion)
        """, {"id_ciudad": id_ciudad, "descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Ciudad creada con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.put("/ciudadputid/{id_ciudad}", tags=["Ciudad"])
def actualizar_ciudad(id_ciudad: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
                UPDATE ciudad
                SET descripcion = :descripcion
                WHERE id_ciudad = :id_ciudad
        """, {"id_ciudad": id_ciudad, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Ciudad no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Ciudad actualizada con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.delete("/ciudaddelete/{id_ciudad}", tags=["Ciudad"])
def eliminar_ciudad(id_ciudad: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM ciudad WHERE id_ciudad = :id_ciudad", {"id_ciudad": id_ciudad})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Ciudad no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Ciudad eliminada con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.patch("/ciudadpatch/{id_ciudad}", tags=["Ciudad"])
def actualizar_parcial_ciudad(id_ciudad: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        
        cone = get_conexion()
        cursor = cone.cursor()
        
        cursor.execute("""
            UPDATE ciudad SET descripcion = :descripcion WHERE id_ciudad = :id_ciudad
        """, {"id_ciudad": id_ciudad, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Ciudad no encontrada")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Ciudad actualizada parcialmente"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

# CRUD Cargo

@app.get("/cargoget", tags=["Cargo"])
def listar_cargo():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo, descripcion FROM cargo ORDER BY id_cargo")
        cargos = []
        for id_cargo, descripcion in cursor:
            cargos.append({
                "id_cargo": id_cargo,
                "descripcion": descripcion
            })
        cursor.close()
        cone.close()
        return cargos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cargogetid/{id_cargo_param}", tags=["Cargo"])
def obtener_cargo(id_cargo_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo, descripcion FROM cargo WHERE id_cargo = :id_cargo_bind", {"id_cargo_bind": id_cargo_param})
        cargo = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if cargo:
            return {"id_cargo": cargo[0], "descripcion": cargo[1]}
        else:
            raise HTTPException(status_code=404, detail=f"Cargo con id {id_cargo_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.post("/cargopost", tags=["Cargo"])
def agregar_cargo(id_cargo: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo FROM cargo WHERE id_cargo = :p_id_cargo OR LOWER(descripcion) = LOWER(:p_descripcion)",
                       p_id_cargo=id_cargo, p_descripcion=descripcion)
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe un cargo con ese ID o descripción.")
        cursor.execute("""
            INSERT INTO cargo (id_cargo, descripcion)
            VALUES (:id_cargo, :descripcion)
        """, {"id_cargo": id_cargo, "descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Cargo creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID o descripción ya existe).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear cargo: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Error inesperado al crear cargo: {str(ex)}")

@app.put("/cargoputid/{id_cargo_param}", tags=["Cargo"])
def actualizar_cargo(id_cargo_param: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo FROM cargo WHERE id_cargo = :id_check", {"id_check": id_cargo_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cargo con id {id_cargo_param} no encontrado para actualizar.")
        cursor.execute("SELECT id_cargo FROM cargo WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_cargo != :p_id_cargo",
                       p_descripcion=descripcion, p_id_cargo=id_cargo_param)
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro cargo con esa descripción.")
        cursor.execute("""
                UPDATE cargo 
                SET descripcion = :descripcion
                WHERE id_cargo = :id_cargo_bind
        """, {"id_cargo_bind": id_cargo_param, "descripcion": descripcion})
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cargo no encontrado o la descripción ya era la misma")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Cargo actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otro cargo.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar cargo: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=f"Error inesperado al actualizar cargo: {str(ex)}")

@app.delete("/cargodelete/{id_cargo_param}", tags=["Cargo"])
def eliminar_cargo(id_cargo_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM cargo WHERE id_cargo = :id_cargo_bind", {"id_cargo_bind": id_cargo_param})
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail=f"Cargo con id {id_cargo_param} no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Cargo eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el cargo. Existen registros (empleados) que dependen de él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar cargo: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar cargo: {str(ex)}")

@app.patch("/cargopatch/{id_cargo_param}", tags=["Cargo"])
def actualizar_parcial_cargo(id_cargo_param: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar una descripción para actualizar.")
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo FROM cargo WHERE id_cargo = :id_check", {"id_check": id_cargo_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cargo con id {id_cargo_param} no encontrado para actualizar.")
        cursor.execute("SELECT id_cargo FROM cargo WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_cargo != :p_id_cargo",
                       p_descripcion=descripcion, p_id_cargo=id_cargo_param)
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro cargo con esa descripción.")
        cursor.execute("""
            UPDATE cargo SET descripcion = :descripcion WHERE id_cargo = :id_cargo_bind
        """, {"id_cargo_bind": id_cargo_param, "descripcion": descripcion})
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cargo no encontrado o la descripción ya era la misma")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Cargo actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otro cargo.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente cargo: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente cargo: {str(ex)}")

# CRUD Categoría

@app.get("/categoriasget", tags=["Categoría"])
def obtener_categorias():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_categoria, descripcion FROM categoria ORDER BY id_categoria")
        categorias = []
        for id_categoria, descripcion in cursor:
            categorias.append({
                "id_categoria": id_categoria,
                "descripcion": descripcion
            })
        cursor.close()
        cone.close()
        return categorias
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar categorías: {str(e)}")

@app.get("/categoriasgetid/{id_categoria_param}", tags=["Categoría"])
def obtener_categoria(id_categoria_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_categoria, descripcion FROM categoria WHERE id_categoria = :id_cat_bind", {"id_cat_bind": id_categoria_param})
        categoria = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if categoria:
            return {"id_categoria": categoria[0], "descripcion": categoria[1]}
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Categoría con id {id_categoria_param} no encontrada")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener categoría: {str(ex)}")

@app.post("/categoriaspost", tags=["Categoría"])
def agregar_categoria(id_categoria: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        
        cursor.execute("SELECT id_categoria FROM categoria WHERE id_categoria = :p_id_categoria OR LOWER(descripcion) = LOWER(:p_descripcion)",
                       {"p_id_categoria": id_categoria, "p_descripcion": descripcion})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe una categoría con ese ID o descripción.")

        cursor.execute("""
            INSERT INTO categoria (id_categoria, descripcion)
            VALUES (:id_categoria, :descripcion)
        """, {"id_categoria": id_categoria, "descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría creada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID o descripción ya existe).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear categoría: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear categoría: {str(ex)}")

@app.put("/categoriasputid/{id_categoria_param}", tags=["Categoría"])
def actualizar_categoria(id_categoria_param: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_categoria FROM categoria WHERE id_categoria = :id_check", {"id_check": id_categoria_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Categoría con id {id_categoria_param} no encontrada para actualizar.")
        
        cursor.execute("SELECT id_categoria FROM categoria WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_categoria != :p_id_categoria",
                       {"p_descripcion": descripcion, "p_id_categoria": id_categoria_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otra categoría con esa descripción.")
            
        cursor.execute("""
                UPDATE categoria 
                SET descripcion = :descripcion
                WHERE id_categoria = :id_categoria_bind
        """, {"id_categoria_bind": id_categoria_param, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Categoría no encontrada o la descripción ya era la misma")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría actualizada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otra categoría.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar categoría: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar categoría: {str(ex)}")

@app.delete("/categoriasdelete/{id_categoria_param}", tags=["Categoría"])
def eliminar_categoria(id_categoria_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM categoria WHERE id_categoria = :id_cat_bind", {"id_cat_bind": id_categoria_param})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Categoría con id {id_categoria_param} no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría eliminada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar la categoría. Existen productos asociados a ella.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar categoría: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar categoría: {str(ex)}")

@app.patch("/categoriaspatch/{id_categoria_param}", tags=["Categoría"])
def actualizar_parcial_categoria(id_categoria_param: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar una descripción para actualizar.")
        
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_categoria FROM categoria WHERE id_categoria = :id_check", {"id_check": id_categoria_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Categoría con id {id_categoria_param} no encontrada para actualizar.")

        cursor.execute("SELECT id_categoria FROM categoria WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_categoria != :p_id_categoria",
                       {"p_descripcion": descripcion, "p_id_categoria": id_categoria_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otra categoría con esa descripción.")
            
        cursor.execute("""
            UPDATE categoria SET descripcion = :descripcion WHERE id_categoria = :id_cat_bind
        """, {"id_cat_bind": id_categoria_param, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Categoría no encontrada o la descripción ya era la misma")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría actualizada parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otra categoría.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente categoría: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente categoría: {str(ex)}")

# CRUD Estado Pedido

@app.get("/estados_pedidoget", tags=["Estado Pedido"])
def obtener_estados_pedido():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_estado_pedido, descripcion FROM estado_pedido ORDER BY id_estado_pedido")
        estados_pedido = []
        for id_estado_pedido, descripcion in cursor:
            estados_pedido.append({
                "id_estado_pedido": id_estado_pedido,
                "descripcion": descripcion
            })
        cursor.close()
        cone.close()
        return estados_pedido
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar estados de pedido: {str(e)}")

@app.get("/estados_pedidogetid/{id_estado_pedido_param}", tags=["Estado Pedido"])
def obtener_estado_pedido(id_estado_pedido_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_estado_pedido, descripcion FROM estado_pedido WHERE id_estado_pedido = :id_ep_bind", {"id_ep_bind": id_estado_pedido_param})
        estado_pedido = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if estado_pedido:
            return {"id_estado_pedido": estado_pedido[0], "descripcion": estado_pedido[1]}
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Estado de pedido con id {id_estado_pedido_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener estado de pedido: {str(ex)}")

@app.post("/estados_pedidopost", tags=["Estado Pedido"])
def agregar_estado_pedido(id_estado_pedido: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        
        cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE id_estado_pedido = :p_id_ep OR LOWER(descripcion) = LOWER(:p_descripcion)",
                       {"p_id_ep": id_estado_pedido, "p_descripcion": descripcion})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe un estado de pedido con ese ID o descripción.")

        cursor.execute("""
            INSERT INTO estado_pedido (id_estado_pedido, descripcion)
            VALUES (:id_estado_pedido, :descripcion)
        """, {"id_estado_pedido": id_estado_pedido, "descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID o descripción ya existe).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear estado de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear estado de pedido: {str(ex)}")

@app.put("/estados_pedidoputid/{id_estado_pedido_param}", tags=["Estado Pedido"])
def actualizar_estado_pedido(id_estado_pedido_param: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE id_estado_pedido = :id_check", {"id_check": id_estado_pedido_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Estado de pedido con id {id_estado_pedido_param} no encontrado para actualizar.")
        
        cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_estado_pedido != :p_id_ep",
                       {"p_descripcion": descripcion, "p_id_ep": id_estado_pedido_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro estado de pedido con esa descripción.")
            
        cursor.execute("""
                UPDATE estado_pedido 
                SET descripcion = :descripcion
                WHERE id_estado_pedido = :id_ep_bind
        """, {"id_ep_bind": id_estado_pedido_param, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Estado de pedido no encontrado o la descripción ya era la misma")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otro estado de pedido.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar estado de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar estado de pedido: {str(ex)}")

@app.delete("/estados_pedidodelete/{id_estado_pedido_param}", tags=["Estado Pedido"])
def eliminar_estado_pedido(id_estado_pedido_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM estado_pedido WHERE id_estado_pedido = :id_ep_bind", {"id_ep_bind": id_estado_pedido_param})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Estado de pedido con id {id_estado_pedido_param} no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el estado de pedido. Existen pedidos asociados a él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar estado de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar estado de pedido: {str(ex)}")

@app.patch("/estados_pedidopatch/{id_estado_pedido_param}", tags=["Estado Pedido"])
def actualizar_parcial_estado_pedido(id_estado_pedido_param: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar una descripción para actualizar.")
        
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE id_estado_pedido = :id_check", {"id_check": id_estado_pedido_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Estado de pedido con id {id_estado_pedido_param} no encontrado para actualizar.")

        cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_estado_pedido != :p_id_ep",
                       {"p_descripcion": descripcion, "p_id_ep": id_estado_pedido_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro estado de pedido con esa descripción.")
            
        cursor.execute("""
            UPDATE estado_pedido SET descripcion = :descripcion WHERE id_estado_pedido = :id_ep_bind
        """, {"id_ep_bind": id_estado_pedido_param, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Estado de pedido no encontrado o la descripción ya era la misma")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otro estado de pedido.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente estado de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente estado de pedido: {str(ex)}")

# CRUD Tipo Transacción

@app.get("/tipos_transaccionget", tags=["Tipo Transacción"])
def obtener_tipos_transaccion():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_tipo_transaccion, descripcion FROM tipo_transaccion ORDER BY id_tipo_transaccion")
        tipos_transaccion = []
        for id_tipo_transaccion, descripcion in cursor:
            tipos_transaccion.append({
                "id_tipo_transaccion": id_tipo_transaccion,
                "descripcion": descripcion
            })
        cursor.close()
        cone.close()
        return tipos_transaccion
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar tipos de transacción: {str(e)}")

@app.get("/tipos_transacciongetid/{id_tipo_transaccion_param}", tags=["Tipo Transacción"])
def obtener_tipo_transaccion(id_tipo_transaccion_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_tipo_transaccion, descripcion FROM tipo_transaccion WHERE id_tipo_transaccion = :id_tt_bind", {"id_tt_bind": id_tipo_transaccion_param})
        tipo_transaccion = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if tipo_transaccion:
            return {"id_tipo_transaccion": tipo_transaccion[0], "descripcion": tipo_transaccion[1]}
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Tipo de transacción con id {id_tipo_transaccion_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener tipo de transacción: {str(ex)}")

@app.post("/tipos_transaccionpost", tags=["Tipo Transacción"])
def agregar_tipo_transaccion(id_tipo_transaccion: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        
        cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE id_tipo_transaccion = :p_id_tt OR LOWER(descripcion) = LOWER(:p_descripcion)",
                       {"p_id_tt": id_tipo_transaccion, "p_descripcion": descripcion})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe un tipo de transacción con ese ID o descripción.")

        cursor.execute("""
            INSERT INTO tipo_transaccion (id_tipo_transaccion, descripcion)
            VALUES (:id_tipo_transaccion, :descripcion)
        """, {"id_tipo_transaccion": id_tipo_transaccion, "descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID o descripción ya existe).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear tipo de transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear tipo de transacción: {str(ex)}")

@app.put("/tipos_transaccionputid/{id_tipo_transaccion_param}", tags=["Tipo Transacción"])
def actualizar_tipo_transaccion(id_tipo_transaccion_param: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE id_tipo_transaccion = :id_check", {"id_check": id_tipo_transaccion_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Tipo de transacción con id {id_tipo_transaccion_param} no encontrado para actualizar.")
        
        cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_tipo_transaccion != :p_id_tt",
                       {"p_descripcion": descripcion, "p_id_tt": id_tipo_transaccion_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro tipo de transacción con esa descripción.")
            
        cursor.execute("""
                UPDATE tipo_transaccion 
                SET descripcion = :descripcion
                WHERE id_tipo_transaccion = :id_tt_bind
        """, {"id_tt_bind": id_tipo_transaccion_param, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tipo de transacción no encontrado o la descripción ya era la misma")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otro tipo de transacción.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar tipo de transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar tipo de transacción: {str(ex)}")

@app.delete("/tipos_transacciondelete/{id_tipo_transaccion_param}", tags=["Tipo Transacción"])
def eliminar_tipo_transaccion(id_tipo_transaccion_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM tipo_transaccion WHERE id_tipo_transaccion = :id_tt_bind", {"id_tt_bind": id_tipo_transaccion_param})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Tipo de transacción con id {id_tipo_transaccion_param} no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el tipo de transacción. Existen transacciones asociadas a él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar tipo de transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar tipo de transacción: {str(ex)}")

@app.patch("/tipos_transaccionpatch/{id_tipo_transaccion_param}", tags=["Tipo Transacción"])
def actualizar_parcial_tipo_transaccion(id_tipo_transaccion_param: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar una descripción para actualizar.")
        
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE id_tipo_transaccion = :id_check", {"id_check": id_tipo_transaccion_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Tipo de transacción con id {id_tipo_transaccion_param} no encontrado para actualizar.")

        cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE LOWER(descripcion) = LOWER(:p_descripcion) AND id_tipo_transaccion != :p_id_tt",
                       {"p_descripcion": descripcion, "p_id_tt": id_tipo_transaccion_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro tipo de transacción con esa descripción.")
            
        cursor.execute("""
            UPDATE tipo_transaccion SET descripcion = :descripcion WHERE id_tipo_transaccion = :id_tt_bind
        """, {"id_tt_bind": id_tipo_transaccion_param, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tipo de transacción no encontrado o la descripción ya era la misma")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: La descripción ya está en uso por otro tipo de transacción.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente tipo de transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente tipo de transacción: {str(ex)}")

# CRUD Sucursal

@app.get("/sucursalget", tags=["Sucursal"])
def listar_sucursal():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_sucursal, nombre_sucursal, direccion, id_ciudad FROM sucursal ORDER BY id_sucursal")
        sucursales = []
        for id_sucursal, nombre_sucursal, direccion, id_ciudad in cursor:
            sucursales.append({
                "id_sucursal": id_sucursal,
                "nombre_sucursal": nombre_sucursal,
                "direccion": direccion,
                "id_ciudad": id_ciudad
            })
        cursor.close()
        cone.close()
        return sucursales
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar sucursales: {str(e)}")

@app.get("/sucursalgetid/{id_sucursal_param}", tags=["Sucursal"])
def obtener_sucursal(id_sucursal_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_sucursal, nombre_sucursal, direccion, id_ciudad FROM sucursal WHERE id_sucursal = :id_suc_bind", {"id_suc_bind": id_sucursal_param})
        sucursal = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if sucursal:
            return {
                "id_sucursal": sucursal[0],
                "nombre_sucursal": sucursal[1],
                "direccion": sucursal[2],
                "id_ciudad": sucursal[3]
            }
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Sucursal con id {id_sucursal_param} no encontrada")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener sucursal: {str(ex)}")

@app.post("/sucursalpost", tags=["Sucursal"])
def agregar_sucursal(id_sucursal: int, nombre_sucursal: str, id_ciudad: int, direccion: Optional[str] = None):
    try:
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_sucursal OR LOWER(nombre_sucursal) = LOWER(:p_nombre_sucursal)",
                       {"p_id_sucursal": id_sucursal, "p_nombre_sucursal": nombre_sucursal})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe una sucursal con ese ID o nombre.")

        cursor.execute("SELECT id_ciudad FROM ciudad WHERE id_ciudad = :p_id_ciudad", {"p_id_ciudad": id_ciudad})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La ciudad con id {id_ciudad} no existe.")

        cursor.execute("""
            INSERT INTO sucursal (id_sucursal, nombre_sucursal, direccion, id_ciudad)
            VALUES (:id_sucursal, :nombre_sucursal, :direccion, :id_ciudad)
        """, {"id_sucursal": id_sucursal, "nombre_sucursal": nombre_sucursal, "direccion": direccion, "id_ciudad": id_ciudad})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal creada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID o nombre_sucursal ya existe).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La ciudad con id {id_ciudad} no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear sucursal: {str(ex)}")

@app.put("/sucursalputid/{id_sucursal_param}", tags=["Sucursal"])
def actualizar_sucursal(id_sucursal_param: int, nombre_sucursal: str, id_ciudad: int, direccion: Optional[str] = None):
    try:
        cone = get_conexion()
        cursor = cone.cursor()

        cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :id_check", {"id_check": id_sucursal_param})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Sucursal con id {id_sucursal_param} no encontrada para actualizar.")
        
        cursor.execute("SELECT id_sucursal FROM sucursal WHERE LOWER(nombre_sucursal) = LOWER(:p_nombre_sucursal) AND id_sucursal != :p_id_sucursal",
                       {"p_nombre_sucursal": nombre_sucursal, "p_id_sucursal": id_sucursal_param})
        if cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otra sucursal con ese nombre.")

        cursor.execute("SELECT id_ciudad FROM ciudad WHERE id_ciudad = :p_id_ciudad", {"p_id_ciudad": id_ciudad})
        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La ciudad con id {id_ciudad} no existe.")
            
        cursor.execute("""
                UPDATE sucursal 
                SET nombre_sucursal = :nombre_sucursal, direccion = :direccion, id_ciudad = :id_ciudad
                WHERE id_sucursal = :id_suc_bind
        """, {"id_suc_bind": id_sucursal_param, "nombre_sucursal": nombre_sucursal, "direccion": direccion, "id_ciudad": id_ciudad})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sucursal no encontrada o los datos ya eran los mismos")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal actualizada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: El nombre de sucursal ya está en uso.")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La ciudad con id {id_ciudad} no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar sucursal: {str(ex)}")

@app.delete("/sucursaldelete/{id_sucursal_param}", tags=["Sucursal"])
def eliminar_sucursal(id_sucursal_param: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM sucursal WHERE id_sucursal = :id_suc_bind", {"id_suc_bind": id_sucursal_param})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Sucursal con id {id_sucursal_param} no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal eliminada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar la sucursal. Existen registros (empleados, stock, etc.) que dependen de ella.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar sucursal: {str(ex)}")

@app.patch("/sucursalpatch/{id_sucursal_param}", tags=["Sucursal"])
def actualizar_parcial_sucursal(id_sucursal_param: int, nombre_sucursal: Optional[str] = None, direccion: Optional[str] = None, id_ciudad: Optional[int] = None):
    try:
        if not nombre_sucursal and direccion is None and id_ciudad is None:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :id_check", {"id_check": id_sucursal_param})

        if not cursor.fetchone():
            cursor.close()
            cone.close()
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Sucursal con id {id_sucursal_param} no encontrada para actualizar.")
        set_clauses = []
        bind_vars = {"p_id_sucursal": id_sucursal_param}

        if nombre_sucursal is not None:
            cursor.execute("SELECT id_sucursal FROM sucursal WHERE LOWER(nombre_sucursal) = LOWER(:p_nombre_sucursal) AND id_sucursal != :p_id_sucursal",
                           {"p_nombre_sucursal": nombre_sucursal, "p_id_sucursal": id_sucursal_param})
            if cursor.fetchone():
                cursor.close()
                cone.close()
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otra sucursal con ese nombre.")
            set_clauses.append("nombre_sucursal = :p_nombre_sucursal")
            bind_vars["p_nombre_sucursal"] = nombre_sucursal
        
        if direccion is not None:
            set_clauses.append("direccion = :p_direccion")
            bind_vars["p_direccion"] = direccion
        
        if id_ciudad is not None:
            cursor.execute("SELECT id_ciudad FROM ciudad WHERE id_ciudad = :p_id_ciudad", {"p_id_ciudad": id_ciudad})
            if not cursor.fetchone():
                cursor.close()
                cone.close()
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La ciudad con id {id_ciudad} no existe.")
            set_clauses.append("id_ciudad = :p_id_ciudad")
            bind_vars["p_id_ciudad"] = id_ciudad

        if not set_clauses:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")
        sql_update = f"UPDATE sucursal SET {', '.join(set_clauses)} WHERE id_sucursal = :p_id_sucursal"
        cursor.execute(sql_update, bind_vars)

        if cursor.rowcount == 0:
            pass
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal actualizada parcialmente"}
    
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: El nombre de sucursal ya está en uso.")
        elif error_obj.code == 2291 and "id_ciudad" in bind_vars:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La ciudad con id {id_ciudad} no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente sucursal: {str(ex)}")
    
# CRUD Empleado

@app.get("/empleadoget", tags=["Empleado"])
def listar_empleados():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_empleado, rut, p_nombre, s_nombre, p_apellido, s_apellido, 
                           correo, telefono, salario, id_cargo, id_sucursal, activo 
                    FROM empleado ORDER BY id_empleado
                """
                cursor.execute(sql)
                empleados = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        empleados.append(dict(zip(column_names, row_tuple)))
                return empleados
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar empleados: {str(e)}")

@app.get("/empleadogetid/{id_empleado_param}", tags=["Empleado"])
def obtener_empleado_por_id(id_empleado_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_empleado, rut, p_nombre, s_nombre, p_apellido, s_apellido,
                           correo, telefono, salario, id_cargo, id_sucursal, activo
                    FROM empleado WHERE id_empleado = :id_emp_bind
                """
                cursor.execute(sql, {"id_emp_bind": id_empleado_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        column_names = [desc[0].lower() for desc in cursor.description]
                        return dict(zip(column_names, row))
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Empleado con id {id_empleado_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener empleado: {str(ex)}")

@app.post("/empleadopost", tags=["Empleado"])
def agregar_empleado(
    id_empleado: int,
    rut: str,
    p_nombre: str,
    p_apellido: str,
    correo: str,
    salario: float,
    clave_hash: str,
    s_nombre: Optional[str] = None,
    s_apellido: Optional[str] = None,
    telefono: Optional[str] = None,
    id_cargo: Optional[int] = None,
    id_sucursal: Optional[int] = None,
    activo: str = 'S' # Default 'S'
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                # Verificar si RUT o correo ya existen
                cursor.execute("SELECT id_empleado FROM empleado WHERE rut = :p_rut OR LOWER(correo) = LOWER(:p_correo) OR id_empleado = :p_id_empleado",
                               {"p_rut": rut, "p_correo": correo, "p_id_empleado": id_empleado})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe un empleado con ese ID, RUT o correo.")

                # Verificar FKs si se proporcionan
                if id_cargo is not None:
                    cursor.execute("SELECT id_cargo FROM cargo WHERE id_cargo = :p_id_cargo", {"p_id_cargo": id_cargo})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El cargo con id {id_cargo} no existe.")
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_sucursal", {"p_id_sucursal": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")

                sql = """
                    INSERT INTO empleado (
                        id_empleado, rut, p_nombre, s_nombre, p_apellido, s_apellido,
                        correo, telefono, salario, id_cargo, id_sucursal, clave_hash, activo
                    ) VALUES (
                        :id_empleado, :rut, :p_nombre, :s_nombre, :p_apellido, :s_apellido,
                        :correo, :telefono, :salario, :id_cargo, :id_sucursal, :clave_hash, :activo
                    )
                """
                cursor.execute(sql, {
                    "id_empleado": id_empleado, "rut": rut, "p_nombre": p_nombre, "s_nombre": s_nombre,
                    "p_apellido": p_apellido, "s_apellido": s_apellido, "correo": correo, "telefono": telefono,
                    "salario": salario, "id_cargo": id_cargo, "id_sucursal": id_sucursal,
                    "clave_hash": clave_hash, "activo": activo
                })
                cone.commit()
                return {"Mensaje": "Empleado creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID, RUT o correo ya existe).")
        elif error_obj.code == 2291: # FK constraint violated
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El cargo o la sucursal especificada no existe.")
        elif error_obj.code == 1400: # Cannot insert NULL
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Un campo requerido es nulo.")
        # print(f"Error de BD en agregar_empleado: {error_obj.message}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear empleado: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear empleado: {str(ex)}")

@app.put("/empleadoputid/{id_empleado_param}", tags=["Empleado"])
def actualizar_empleado(
    id_empleado_param: int,
    rut: str,
    p_nombre: str,
    p_apellido: str,
    correo: str,
    salario: float,
    clave_hash: str,
    activo: str,
    s_nombre: Optional[str] = None,
    s_apellido: Optional[str] = None,
    telefono: Optional[str] = None,
    id_cargo: Optional[int] = None,
    id_sucursal: Optional[int] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :id_check", {"id_check": id_empleado_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Empleado con id {id_empleado_param} no encontrado para actualizar.")

                cursor.execute("SELECT id_empleado FROM empleado WHERE rut = :p_rut AND id_empleado != :p_id_empleado",
                               {"p_rut": rut, "p_id_empleado": id_empleado_param})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El RUT '{rut}' ya está en uso por otro empleado.")
                cursor.execute("SELECT id_empleado FROM empleado WHERE LOWER(correo) = LOWER(:p_correo) AND id_empleado != :p_id_empleado",
                               {"p_correo": correo, "p_id_empleado": id_empleado_param})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El correo '{correo}' ya está en uso por otro empleado.")

                if id_cargo is not None:
                    cursor.execute("SELECT id_cargo FROM cargo WHERE id_cargo = :p_id_cargo", {"p_id_cargo": id_cargo})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El cargo con id {id_cargo} no existe.")
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_sucursal", {"p_id_sucursal": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")

                sql = """
                    UPDATE empleado SET
                        rut = :rut, p_nombre = :p_nombre, s_nombre = :s_nombre, 
                        p_apellido = :p_apellido, s_apellido = :s_apellido, correo = :correo,
                        telefono = :telefono, salario = :salario, id_cargo = :id_cargo,
                        id_sucursal = :id_sucursal, clave_hash = :clave_hash, activo = :activo
                    WHERE id_empleado = :id_empleado_param
                """
                cursor.execute(sql, {
                    "rut": rut, "p_nombre": p_nombre, "s_nombre": s_nombre, "p_apellido": p_apellido,
                    "s_apellido": s_apellido, "correo": correo, "telefono": telefono, "salario": salario,
                    "id_cargo": id_cargo, "id_sucursal": id_sucursal, "clave_hash": clave_hash,
                    "activo": activo, "id_empleado_param": id_empleado_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empleado no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Empleado actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (RUT o correo ya existe para otro empleado).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El cargo o la sucursal especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar empleado: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar empleado: {str(ex)}")

@app.delete("/empleadodelete/{id_empleado_param}", tags=["Empleado"])
def eliminar_empleado(id_empleado_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM empleado WHERE id_empleado = :id_emp_bind", {"id_emp_bind": id_empleado_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Empleado con id {id_empleado_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Empleado eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el empleado. Existen registros (pedidos, logs de inventario, etc.) que dependen de él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar empleado: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar empleado: {str(ex)}")

@app.patch("/empleadopatch/{id_empleado_param}", tags=["Empleado"])
def actualizar_parcial_empleado(
    id_empleado_param: int,
    rut: Optional[str] = None,
    p_nombre: Optional[str] = None,
    s_nombre: Optional[str] = None,
    p_apellido: Optional[str] = None,
    s_apellido: Optional[str] = None,
    correo: Optional[str] = None,
    telefono: Optional[str] = None,
    salario: Optional[float] = None,
    id_cargo: Optional[int] = None,
    id_sucursal: Optional[int] = None,
    clave_hash: Optional[str] = None,
    activo: Optional[str] = None
):
    try:
        if not any([rut, p_nombre, s_nombre, p_apellido, s_apellido, correo, telefono, salario is not None, id_cargo is not None, id_sucursal is not None, clave_hash, activo]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")
        
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :id_check", {"id_check": id_empleado_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Empleado con id {id_empleado_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_empleado": id_empleado_param}

                if rut is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE rut = :p_rut AND id_empleado != :p_id_empleado", {"p_rut": rut, "p_id_empleado": id_empleado_param})
                    if cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El RUT '{rut}' ya está en uso por otro empleado.")
                    set_clauses.append("rut = :p_rut")
                    bind_vars["p_rut"] = rut
                if p_nombre is not None:
                    set_clauses.append("p_nombre = :p_p_nombre")
                    bind_vars["p_p_nombre"] = p_nombre
                if s_nombre is not None:
                    set_clauses.append("s_nombre = :p_s_nombre")
                    bind_vars["p_s_nombre"] = s_nombre
                if p_apellido is not None:
                    set_clauses.append("p_apellido = :p_p_apellido")
                    bind_vars["p_p_apellido"] = p_apellido
                if s_apellido is not None:
                    set_clauses.append("s_apellido = :p_s_apellido")
                    bind_vars["p_s_apellido"] = s_apellido
                if correo is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE LOWER(correo) = LOWER(:p_correo) AND id_empleado != :p_id_empleado", {"p_correo": correo, "p_id_empleado": id_empleado_param})
                    if cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El correo '{correo}' ya está en uso por otro empleado.")
                    set_clauses.append("correo = :p_correo")
                    bind_vars["p_correo"] = correo
                if telefono is not None:
                    set_clauses.append("telefono = :p_telefono")
                    bind_vars["p_telefono"] = telefono
                if salario is not None:
                    if salario <= 0: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El salario debe ser mayor que cero.")
                    set_clauses.append("salario = :p_salario")
                    bind_vars["p_salario"] = salario
                if id_cargo is not None:
                    cursor.execute("SELECT id_cargo FROM cargo WHERE id_cargo = :p_id_cargo", {"p_id_cargo": id_cargo})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El cargo con id {id_cargo} no existe.")
                    set_clauses.append("id_cargo = :p_id_cargo")
                    bind_vars["p_id_cargo"] = id_cargo
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_sucursal", {"p_id_sucursal": id_sucursal})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                    set_clauses.append("id_sucursal = :p_id_sucursal")
                    bind_vars["p_id_sucursal"] = id_sucursal
                if clave_hash is not None:
                    set_clauses.append("clave_hash = :p_clave_hash")
                    bind_vars["p_clave_hash"] = clave_hash
                if activo is not None:
                    if activo.upper() not in ('S', 'N'): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El campo 'activo' debe ser 'S' o 'N'.")
                    set_clauses.append("activo = :p_activo")
                    bind_vars["p_activo"] = activo.upper()
                
                if not set_clauses:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se proporcionaron campos válidos para actualizar.")

                sql_update = f"UPDATE empleado SET {', '.join(set_clauses)} WHERE id_empleado = :p_id_empleado"
                cursor.execute(sql_update, bind_vars)
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Empleado no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Empleado actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (RUT o correo ya existe para otro empleado).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El cargo o la sucursal especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar empleado: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar empleado: {str(ex)}")
    
# CRUD Cliente

@app.get("/clientes", tags=["Cliente"])
def listar_clientes():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_cliente, p_nombre, s_nombre, p_apellido, s_apellido, 
                           correo, telefono, activo 
                    FROM cliente ORDER BY id_cliente
                """
                cursor.execute(sql)
                clientes = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        clientes.append(dict(zip(column_names, row_tuple)))
                return clientes
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar clientes: {str(e)}")

@app.get("/clientes/{id_cliente_param}", tags=["Cliente"])
def obtener_cliente_por_id(id_cliente_param: int): 
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_cliente, p_nombre, s_nombre, p_apellido, s_apellido,
                           correo, telefono, activo
                    FROM cliente WHERE id_cliente = :id_cli_bind 
                """
                cursor.execute(sql, {"id_cli_bind": id_cliente_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        column_names = [desc[0].lower() for desc in cursor.description]
                        return dict(zip(column_names, row))
                    else: 
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cliente con id {id_cliente_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener cliente: {str(ex)}")

@app.post("/clientes", tags=["Cliente"], status_code=status.HTTP_201_CREATED)
def agregar_cliente(
    id_cliente: int, 
    p_nombre: str,
    p_apellido: str,
    correo: str,
    clave_hash: str, 
    s_nombre: Optional[str] = None,
    s_apellido: Optional[str] = None,
    telefono: Optional[str] = None,
    activo: str = 'S' 
):
    if activo.upper() not in ('S', 'N'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El campo 'activo' debe ser 'S' o 'N'.")
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_cliente FROM cliente WHERE id_cliente = :p_id_cliente OR LOWER(correo) = LOWER(:p_correo)",
                               {"p_id_cliente": id_cliente, "p_correo": correo})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe un cliente con ese ID o correo.")

                sql = """
                    INSERT INTO cliente (
                        id_cliente, p_nombre, s_nombre, p_apellido, s_apellido,
                        correo, telefono, clave_hash, activo
                    ) VALUES (
                        :id_cliente, :p_nombre, :s_nombre, :p_apellido, :s_apellido,
                        :correo, :telefono, :clave_hash, :activo
                    )
                """
                cursor.execute(sql, {
                    "id_cliente": id_cliente, "p_nombre": p_nombre, "s_nombre": s_nombre,
                    "p_apellido": p_apellido, "s_apellido": s_apellido, "correo": correo, "telefono": telefono,
                    "clave_hash": clave_hash, "activo": activo.upper()
                })
                cone.commit()
                return {"id_cliente": id_cliente, "correo": correo, "mensaje": "Cliente creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de cliente o correo ya existe).")
        elif error_obj.code == 1400:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Un campo requerido (p_nombre, p_apellido, correo, clave_hash, activo) es nulo.")
        elif error_obj.code == 2290:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Violación de restricción CHECK (ej: campo 'activo' inválido).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear cliente: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear cliente: {str(ex)}")

@app.put("/clientes/{id_cliente_param}", tags=["Cliente"])
def actualizar_cliente(
    id_cliente_param: int,
    p_nombre: str,
    p_apellido: str,
    correo: str,
    clave_hash: str,
    activo: str,
    s_nombre: Optional[str] = None,
    s_apellido: Optional[str] = None,
    telefono: Optional[str] = None
):
    if activo.upper() not in ('S', 'N'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El campo 'activo' debe ser 'S' o 'N'.")
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                # Verificar si el cliente existe
                cursor.execute("SELECT id_cliente FROM cliente WHERE id_cliente = :id_check", {"id_check": id_cliente_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cliente con id {id_cliente_param} no encontrado para actualizar.")

                # Verificar si el nuevo correo ya está en uso por OTRO cliente
                cursor.execute("SELECT id_cliente FROM cliente WHERE LOWER(correo) = LOWER(:p_correo) AND id_cliente != :p_id_cliente",
                               {"p_correo": correo, "p_id_cliente": id_cliente_param})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El correo '{correo}' ya está en uso por otro cliente.")
                
                sql = """
                    UPDATE cliente SET
                        p_nombre = :p_nombre, s_nombre = :s_nombre, 
                        p_apellido = :p_apellido, s_apellido = :s_apellido, correo = :correo,
                        telefono = :telefono, clave_hash = :clave_hash, activo = :activo
                    WHERE id_cliente = :id_cliente_param_bind 
                """ # :id_cliente_param_bind para consistencia
                cursor.execute(sql, {
                    "p_nombre": p_nombre, "s_nombre": s_nombre, "p_apellido": p_apellido,
                    "s_apellido": s_apellido, "correo": correo, "telefono": telefono, 
                    "clave_hash": clave_hash, "activo": activo.upper(), 
                    "id_cliente_param_bind": id_cliente_param
                })
                
                # rowcount debería ser 1 si el cliente existía y se actualizó.
                # Si es 0 aquí, significaría que el ID no coincidió, pero ya lo verificamos.
                # O que los datos eran idénticos y Oracle no contó la fila (depende de la config/versión, usualmente sí cuenta).
                if cursor.rowcount == 0:
                     # Esto es improbable si la verificación de existencia anterior pasó y los datos son diferentes.
                     # Podría significar que los datos eran idénticos y no hubo cambio.
                    raise HTTPException(status_code=status.HTTP_304_NOT_MODIFIED, detail="Cliente no modificado (los datos eran los mismos o no se encontró).")

                cone.commit()
                return {"mensaje": "Cliente actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (correo ya existe para otro cliente).")
        elif error_obj.code == 1400: 
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Un campo requerido es nulo.")
        elif error_obj.code == 2290: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Violación de restricción CHECK (ej: campo 'activo' inválido).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar cliente: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar cliente: {str(ex)}")

@app.delete("/clientes/{id_cliente_param}", tags=["Cliente"])
def eliminar_cliente(id_cliente_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM cliente WHERE id_cliente = :id_cli_bind", {"id_cli_bind": id_cliente_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cliente con id {id_cliente_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Cliente eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el cliente. Existen registros (pedidos, etc.) que dependen de él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar cliente: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar cliente: {str(ex)}")

@app.patch("/clientes/{id_cliente_param}", tags=["Cliente"])
def actualizar_parcial_cliente(
    id_cliente_param: int,
    p_nombre: Optional[str] = None,
    s_nombre: Optional[str] = None,
    p_apellido: Optional[str] = None,
    s_apellido: Optional[str] = None,
    correo: Optional[str] = None,
    telefono: Optional[str] = None,
    clave_hash: Optional[str] = None,
    activo: Optional[str] = None
):
    if not any([p_nombre, s_nombre, p_apellido, s_apellido, correo, telefono, clave_hash, activo]):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")
    
    if activo and activo.upper() not in ('S', 'N'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El campo 'activo' debe ser 'S' o 'N'.")
        
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_cliente FROM cliente WHERE id_cliente = :id_check", {"id_check": id_cliente_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Cliente con id {id_cliente_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_cliente_bind": id_cliente_param}

                if p_nombre is not None:
                    set_clauses.append("p_nombre = :p_p_nombre")
                    bind_vars["p_p_nombre"] = p_nombre
                if s_nombre is not None:
                    set_clauses.append("s_nombre = :p_s_nombre")
                    bind_vars["p_s_nombre"] = s_nombre
                if p_apellido is not None:
                    set_clauses.append("p_apellido = :p_p_apellido")
                    bind_vars["p_p_apellido"] = p_apellido
                if s_apellido is not None:
                    set_clauses.append("s_apellido = :p_s_apellido")
                    bind_vars["p_s_apellido"] = s_apellido
                if correo is not None:
                    cursor.execute("SELECT id_cliente FROM cliente WHERE LOWER(correo) = LOWER(:p_correo) AND id_cliente != :p_id_cliente_check", 
                                   {"p_correo": correo, "p_id_cliente_check": id_cliente_param})
                    if cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El correo '{correo}' ya está en uso por otro cliente.")
                    set_clauses.append("correo = :p_correo")
                    bind_vars["p_correo"] = correo
                if telefono is not None:
                    set_clauses.append("telefono = :p_telefono")
                    bind_vars["p_telefono"] = telefono
                if clave_hash is not None:
                    set_clauses.append("clave_hash = :p_clave_hash")
                    bind_vars["p_clave_hash"] = clave_hash
                if activo is not None:
                    set_clauses.append("activo = :p_activo")
                    bind_vars["p_activo"] = activo.upper()
                
                if not set_clauses:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se proporcionaron campos válidos para actualizar.")

                sql_update = f"UPDATE cliente SET {', '.join(set_clauses)} WHERE id_cliente = :p_id_cliente_bind"
                cursor.execute(sql_update, bind_vars)
                
                if cursor.rowcount == 0:
                    pass
                cone.commit()
                return {"Mensaje": "Cliente actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ej: correo ya existe para otro cliente).")
        elif error_obj.code == 1400:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Intento de insertar NULL en un campo no permitido.")
        elif error_obj.code == 2290: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Violación de restricción CHECK (ej: campo 'activo' inválido).")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente cliente: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente cliente: {str(ex)}")
    
@app.get("/clientes/{id_cliente_param}/pedidos", tags=["Cliente", "Pedidos"], summary="Obtener todos los pedidos de un cliente")
async def obtener_pedidos_por_cliente(id_cliente_param: int, db_conn: oracledb.Connection = Depends(get_conexion)):
    pedidos_completos = []
    try:
        with db_conn.cursor() as cursor:
            # Primero, obtener todos los pedidos del cliente
            sql_pedidos = """
                SELECT p.id_pedido, p.fecha_pedido, p.total_pedido, ep.descripcion as estado_descripcion, ep.id_estado_pedido
                FROM pedido p
                JOIN estado_pedido ep ON p.id_estado_pedido = ep.id_estado_pedido
                WHERE p.id_cliente = :id_cliente
                ORDER BY p.fecha_pedido DESC, p.id_pedido DESC
            """
            await asyncio.to_thread(cursor.execute, sql_pedidos, id_cliente=id_cliente_param)
            pedidos_raw = await asyncio.to_thread(cursor.fetchall)

            if not pedidos_raw:
                return [] # Devuelve lista vacía si no hay pedidos

            column_names_pedido = [desc[0].lower() for desc in cursor.description]

            for pedido_row_tuple in pedidos_raw:
                pedido_dict = dict(zip(column_names_pedido, pedido_row_tuple))
                
                # Formatear fecha si es necesario (ya lo haces en otros endpoints)
                if pedido_dict.get('fecha_pedido') and isinstance(pedido_dict['fecha_pedido'], datetime.date):
                    pedido_dict['fecha_pedido'] = pedido_dict['fecha_pedido'].isoformat()

                pedido_dict['detalles'] = []
                sql_detalles = """
                    SELECT dp.id_detalle_pedido, dp.id_producto, pr.nombre as nombre_producto, 
                           dp.cantidad, dp.precio_unitario_venta, dp.subtotal, pr.imagen_url
                    FROM detalle_pedido dp
                    JOIN productos pr ON dp.id_producto = pr.id_producto
                    WHERE dp.id_pedido = :id_pedido_actual
                    ORDER BY dp.id_detalle_pedido
                """
                with db_conn.cursor() as cursor_detalle:
                    await asyncio.to_thread(cursor_detalle.execute, sql_detalles, id_pedido_actual=pedido_dict['id_pedido'])
                    detalles_raw = await asyncio.to_thread(cursor_detalle.fetchall)
                    
                    if detalles_raw:
                        column_names_detalle = [desc[0].lower() for desc in cursor_detalle.description]
                        for detalle_row_tuple in detalles_raw:
                            pedido_dict['detalles'].append(dict(zip(column_names_detalle, detalle_row_tuple)))
                
                pedidos_completos.append(pedido_dict)
            
            return pedidos_completos

    except oracledb.Error as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de base de datos al obtener pedidos: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado en el servidor: {str(ex)}")
    
# CRUD Productos

@app.get("/productosget", tags=["Productos"])
def obtener_productos_todos():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_producto, nombre, marca, descripcion_detallada, 
                           precio, id_categoria, imagen_url 
                    FROM productos ORDER BY id_producto
                """
                cursor.execute(sql)
                productos = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        productos.append(dict(zip(column_names, row_tuple)))
                return productos
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar productos: {str(e)}")

@app.get("/productosgetid/{id_producto_param}", tags=["Productos"])
def obtener_producto_por_id(id_producto_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_producto, nombre, marca, descripcion_detallada, 
                           precio, id_categoria, imagen_url
                    FROM productos WHERE id_producto = :id_prod_bind
                """
                cursor.execute(sql, {"id_prod_bind": id_producto_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        column_names = [desc[0].lower() for desc in cursor.description]
                        return dict(zip(column_names, row))
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Producto con id {id_producto_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener producto: {str(ex)}")

@app.post("/productospost", tags=["Productos"])
def agregar_producto(
    id_producto: int,
    nombre: str,
    precio: float,
    marca: Optional[str] = None,
    descripcion_detallada: Optional[str] = None,
    id_categoria: Optional[int] = None,
    imagen_url: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_producto", {"p_id_producto": id_producto})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe un producto con el ID {id_producto}.")

                if id_categoria is not None:
                    cursor.execute("SELECT id_categoria FROM categoria WHERE id_categoria = :p_id_categoria", {"p_id_categoria": id_categoria})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La categoría con id {id_categoria} no existe.")
                sql = """
                    INSERT INTO productos (
                        id_producto, nombre, marca, descripcion_detallada, 
                        precio, id_categoria, imagen_url
                    ) VALUES (
                        :id_producto, :nombre, :marca, :descripcion_detallada, 
                        :precio, :id_categoria, :imagen_url
                    )
                """
                cursor.execute(sql, {
                    "id_producto": id_producto, "nombre": nombre, "marca": marca, 
                    "descripcion_detallada": descripcion_detallada, "precio": precio, 
                    "id_categoria": id_categoria, "imagen_url": imagen_url
                })
                cone.commit()
                return {"Mensaje": "Producto creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de producto ya existe).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La categoría especificada no existe.")
        elif error_obj.code == 1400:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Nombre o precio no pueden ser nulos.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear producto: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear producto: {str(ex)}")

@app.put("/productosputid/{id_producto_param}", tags=["Productos"])
def actualizar_producto(
    id_producto_param: int,
    nombre: str,
    precio: float,
    marca: Optional[str] = None,
    descripcion_detallada: Optional[str] = None,
    id_categoria: Optional[int] = None,
    imagen_url: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :id_check", {"id_check": id_producto_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Producto con id {id_producto_param} no encontrado para actualizar.")

                if id_categoria is not None:
                    cursor.execute("SELECT id_categoria FROM categoria WHERE id_categoria = :p_id_categoria", {"p_id_categoria": id_categoria})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La categoría con id {id_categoria} no existe.")
            
                sql = """
                    UPDATE productos SET
                        nombre = :nombre,
                        marca = :marca,
                        descripcion_detallada = :descripcion_detallada,
                        precio = :precio,
                        id_categoria = :id_categoria,
                        imagen_url = :imagen_url
                    WHERE id_producto = :id_producto_bind
                """
                cursor.execute(sql, {
                    "nombre": nombre, "marca": marca, "descripcion_detallada": descripcion_detallada,
                    "precio": precio, "id_categoria": id_categoria, "imagen_url": imagen_url,
                    "id_producto_bind": id_producto_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Producto actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La categoría especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar producto: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar producto: {str(ex)}")

@app.delete("/productosdelete/{id_producto_param}", tags=["Productos"])
def eliminar_producto(id_producto_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM productos WHERE id_producto = :id_prod_bind", {"id_prod_bind": id_producto_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Producto con id {id_producto_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Producto eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el producto. Existen registros (detalle_pedido, stock_sucursal, etc.) que dependen de él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar producto: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar producto: {str(ex)}")

@app.patch("/productospatch/{id_producto_param}", tags=["Productos"])
def actualizar_parcial_producto(
    id_producto_param: int,
    nombre: Optional[str] = None,
    marca: Optional[str] = None,
    descripcion_detallada: Optional[str] = None,
    precio: Optional[float] = None,
    id_categoria: Optional[int] = None,
    imagen_url: Optional[str] = None
):
    try:
        if not any([nombre, marca, descripcion_detallada, precio is not None, id_categoria is not None, imagen_url]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")
        
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :id_check", {"id_check": id_producto_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Producto con id {id_producto_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_producto_bind": id_producto_param}

                if nombre is not None:
                    set_clauses.append("nombre = :p_nombre")
                    bind_vars["p_nombre"] = nombre
                if marca is not None:
                    set_clauses.append("marca = :p_marca")
                    bind_vars["p_marca"] = marca
                if descripcion_detallada is not None:
                    set_clauses.append("descripcion_detallada = :p_desc_detallada")
                    bind_vars["p_desc_detallada"] = descripcion_detallada
                if precio is not None:
                    if precio <= 0:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El precio debe ser mayor que cero.")
                    set_clauses.append("precio = :p_precio")
                    bind_vars["p_precio"] = precio
                if id_categoria is not None:
                    cursor.execute("SELECT id_categoria FROM categoria WHERE id_categoria = :p_id_categoria", {"p_id_categoria": id_categoria})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La categoría con id {id_categoria} no existe.")
                    set_clauses.append("id_categoria = :p_id_categoria")
                    bind_vars["p_id_categoria"] = id_categoria
                if imagen_url is not None:
                    set_clauses.append("imagen_url = :p_imagen_url")
                    bind_vars["p_imagen_url"] = imagen_url
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE productos SET {', '.join(set_clauses)} WHERE id_producto = :p_id_producto_bind"
                cursor.execute(sql_update, bind_vars)
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Producto actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291 and "id_categoria" in bind_vars :
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La categoría especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente producto: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente producto: {str(ex)}")
 
# CRUD Stock Sucursal

@app.get("/stock_sucursalget", tags=["Stock Sucursal"])
def listar_stock_sucursal():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_stock_sucursal, id_producto, id_sucursal, cantidad, 
                           ubicacion_bodega, fecha_ultima_actualizacion 
                    FROM stock_sucursal ORDER BY id_sucursal, id_producto
                """
                cursor.execute(sql)
                stock_sucursales = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        stock_sucursales.append(dict(zip(column_names, row_tuple)))
                return stock_sucursales
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar stock de sucursales: {str(e)}")

@app.get("/stock_sucursalgetid/{id_stock_sucursal_param}", tags=["Stock Sucursal"])
def obtener_stock_sucursal_por_id(id_stock_sucursal_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_stock_sucursal, id_producto, id_sucursal, cantidad, 
                           ubicacion_bodega, fecha_ultima_actualizacion
                    FROM stock_sucursal WHERE id_stock_sucursal = :id_ss_bind
                """
                cursor.execute(sql, {"id_ss_bind": id_stock_sucursal_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        column_names = [desc[0].lower() for desc in cursor.description]
                        return dict(zip(column_names, row))
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Registro de stock con id {id_stock_sucursal_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener stock de sucursal: {str(ex)}")

@app.post("/stock_sucursalpost", tags=["Stock Sucursal"])
def agregar_stock_sucursal(
    id_stock_sucursal: int,
    id_producto: int,
    id_sucursal: int,
    cantidad: int,
    ubicacion_bodega: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_stock_sucursal FROM stock_sucursal WHERE id_stock_sucursal = :p_id_ss OR (id_producto = :p_id_prod AND id_sucursal = :p_id_suc)",
                               {"p_id_ss": id_stock_sucursal, "p_id_prod": id_producto, "p_id_suc": id_sucursal})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe un registro de stock para este producto en esta sucursal o el ID de stock ya está en uso.")

                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_prod", {"p_id_prod": id_producto})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                
                if cantidad < 0:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La cantidad no puede ser negativa.")

                sql = """
                    INSERT INTO stock_sucursal (
                        id_stock_sucursal, id_producto, id_sucursal, cantidad, 
                        ubicacion_bodega, fecha_ultima_actualizacion
                    ) VALUES (
                        :id_stock_sucursal, :id_producto, :id_sucursal, :cantidad, 
                        :ubicacion_bodega, SYSDATE
                    )
                """
                cursor.execute(sql, {
                    "id_stock_sucursal": id_stock_sucursal, "id_producto": id_producto, "id_sucursal": id_sucursal,
                    "cantidad": cantidad, "ubicacion_bodega": ubicacion_bodega
                })
                cone.commit()
                return {"Mensaje": "Stock en sucursal creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de stock o combinación producto-sucursal ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El producto o la sucursal especificada no existe.")
        elif error_obj.code == 2290:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La cantidad no puede ser negativa.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear stock en sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear stock en sucursal: {str(ex)}")

@app.put("/stock_sucursalputid/{id_stock_sucursal_param}", tags=["Stock Sucursal"])
def actualizar_stock_sucursal(
    id_stock_sucursal_param: int,
    id_producto: int,
    id_sucursal: int,
    cantidad: int,
    ubicacion_bodega: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_stock_sucursal FROM stock_sucursal WHERE id_stock_sucursal = :id_check", {"id_check": id_stock_sucursal_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Registro de stock con id {id_stock_sucursal_param} no encontrado para actualizar.")
                
                cursor.execute("SELECT id_stock_sucursal FROM stock_sucursal WHERE id_producto = :p_id_prod AND id_sucursal = :p_id_suc AND id_stock_sucursal != :p_id_ss",
                               {"p_id_prod": id_producto, "p_id_suc": id_sucursal, "p_id_ss": id_stock_sucursal_param})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe otro registro de stock para este producto en esta sucursal.")

                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_prod", {"p_id_prod": id_producto})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")

                if cantidad < 0:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La cantidad no puede ser negativa.")

                sql = """
                    UPDATE stock_sucursal SET
                        id_producto = :id_producto,
                        id_sucursal = :id_sucursal,
                        cantidad = :cantidad,
                        ubicacion_bodega = :ubicacion_bodega,
                        fecha_ultima_actualizacion = SYSDATE
                    WHERE id_stock_sucursal = :id_ss_bind
                """
                cursor.execute(sql, {
                    "id_producto": id_producto, "id_sucursal": id_sucursal, "cantidad": cantidad,
                    "ubicacion_bodega": ubicacion_bodega, "id_ss_bind": id_stock_sucursal_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Registro de stock no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Stock en sucursal actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (combinación producto-sucursal ya existe).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El producto o la sucursal especificada no existe.")
        elif error_obj.code == 2290:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La cantidad no puede ser negativa.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar stock en sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar stock en sucursal: {str(ex)}")

@app.delete("/stock_sucursaldelete/{id_stock_sucursal_param}", tags=["Stock Sucursal"])
def eliminar_stock_sucursal(id_stock_sucursal_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM stock_sucursal WHERE id_stock_sucursal = :id_ss_bind", {"id_ss_bind": id_stock_sucursal_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Registro de stock con id {id_stock_sucursal_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Registro de stock en sucursal eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar stock en sucursal: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar stock en sucursal: {str(ex)}")

@app.patch("/stock_sucursalpatch/{id_stock_sucursal_param}", tags=["Stock Sucursal"])
def actualizar_parcial_stock_sucursal(
    id_stock_sucursal_param: int,
    id_producto: Optional[int] = None,
    id_sucursal: Optional[int] = None,
    cantidad: Optional[int] = None,
    ubicacion_bodega: Optional[str] = None
):
    try:
        if not any([id_producto is not None, id_sucursal is not None, cantidad is not None, ubicacion_bodega is not None]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_producto, id_sucursal FROM stock_sucursal WHERE id_stock_sucursal = :id_check", {"id_check": id_stock_sucursal_param})
                current_stock = cursor.fetchone()
                if not current_stock:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Registro de stock con id {id_stock_sucursal_param} no encontrado para actualizar.")
                current_id_producto = current_stock[0]
                current_id_sucursal = current_stock[1]
                set_clauses = []
                bind_vars = {"p_id_ss_bind": id_stock_sucursal_param}
                new_id_producto = id_producto if id_producto is not None else current_id_producto
                new_id_sucursal = id_sucursal if id_sucursal is not None else current_id_sucursal
                if id_producto is not None:
                    cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_prod", {"p_id_prod": id_producto})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                    set_clauses.append("id_producto = :p_id_producto")
                    bind_vars["p_id_producto"] = id_producto
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                    set_clauses.append("id_sucursal = :p_id_sucursal")
                    bind_vars["p_id_sucursal"] = id_sucursal
                if (id_producto is not None or id_sucursal is not None) and \
                   (new_id_producto != current_id_producto or new_id_sucursal != current_id_sucursal):
                    cursor.execute("SELECT id_stock_sucursal FROM stock_sucursal WHERE id_producto = :p_id_prod AND id_sucursal = :p_id_suc AND id_stock_sucursal != :p_id_ss",
                                   {"p_id_prod": new_id_producto, "p_id_suc": new_id_sucursal, "p_id_ss": id_stock_sucursal_param})
                    if cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="La combinación producto-sucursal ya existe en otro registro de stock.")
                if cantidad is not None:
                    if cantidad < 0:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La cantidad no puede ser negativa.")
                    set_clauses.append("cantidad = :p_cantidad")
                    bind_vars["p_cantidad"] = cantidad
                if ubicacion_bodega is not None:
                    set_clauses.append("ubicacion_bodega = :p_ubicacion_bodega")
                    bind_vars["p_ubicacion_bodega"] = ubicacion_bodega if ubicacion_bodega != "" else None
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización real.")
                set_clauses.append("fecha_ultima_actualizacion = SYSDATE")
                sql_update = f"UPDATE stock_sucursal SET {', '.join(set_clauses)} WHERE id_stock_sucursal = :p_id_ss_bind"
                cursor.execute(sql_update, bind_vars)
                
                if cursor.rowcount == 0:
                    pass 
                cone.commit()
                return {"Mensaje": "Stock en sucursal actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (combinación producto-sucursal ya existe).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El producto o la sucursal especificada no existe.")
        elif error_obj.code == 2290:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La cantidad no puede ser negativa.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente stock: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente stock: {str(ex)}")

# CRUD Log Actividad Inventario

@app.get("/log_actividad_inventarioget", tags=["Log Actividad Inventario"])
def listar_log_actividad_inventario():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_log, tipo_actividad, id_producto, id_sucursal, cantidad_afectada,
                           stock_anterior, stock_nuevo, fecha_actividad, id_empleado_responsable, notas
                    FROM log_actividad_inventario ORDER BY fecha_actividad DESC, id_log DESC
                """
                cursor.execute(sql)
                logs = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        row_as_dict = dict(zip(column_names, row_tuple))
                        if row_as_dict.get('fecha_actividad') and isinstance(row_as_dict['fecha_actividad'], date):
                            row_as_dict['fecha_actividad'] = row_as_dict['fecha_actividad'].isoformat()
                        logs.append(row_as_dict)
                return logs
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar logs de actividad de inventario: {str(e)}")


@app.get("/log_actividad_inventariogetid/{id_log_param}", tags=["Log Actividad Inventario"])
def obtener_log_actividad_inventario_por_id(id_log_param: int):
    try:
        with get_conexion() as cone:
            
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_log, tipo_actividad, id_producto, id_sucursal, cantidad_afectada,
                           stock_anterior, stock_nuevo, fecha_actividad, id_empleado_responsable, notas
                    FROM log_actividad_inventario WHERE id_log = :id_log_bind
                """
                cursor.execute(sql, {"id_log_bind": id_log_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        row_as_dict = dict_from_row(cursor, row) 
                        if row_as_dict.get('fecha_actividad') and isinstance(row_as_dict['fecha_actividad'], date):
                            row_as_dict['fecha_actividad'] = row_as_dict['fecha_actividad'].isoformat()
                        return row_as_dict
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Log de actividad de inventario con id {id_log_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener log de actividad de inventario: {str(ex)}")

@app.post("/log_actividad_inventariopost", tags=["Log Actividad Inventario"])
def agregar_log_actividad_inventario(
    id_log: int,
    tipo_actividad: str,
    id_producto: Optional[int] = None,
    id_sucursal: Optional[int] = None,
    cantidad_afectada: Optional[int] = None,
    stock_anterior: Optional[int] = None,
    stock_nuevo: Optional[int] = None,
    fecha_actividad: Optional[str] = None,
    id_empleado_responsable: Optional[int] = None,
    notas: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_log FROM log_actividad_inventario WHERE id_log = :p_id_log", {"p_id_log": id_log})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe un log de actividad con el ID {id_log}.")

                if id_producto is not None:
                    cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_prod", {"p_id_prod": id_producto})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                if id_empleado_responsable is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_responsable})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado_responsable} no existe.")

                fecha_act_to_insert = None
                if fecha_actividad:
                    try:
                        fecha_act_to_insert = date.fromisoformat(fecha_actividad)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_actividad inválido. Usar YYYY-MM-DD.")
                else:
                    fecha_act_to_insert = date.today()

                sql = """
                    INSERT INTO log_actividad_inventario (
                        id_log, tipo_actividad, id_producto, id_sucursal, cantidad_afectada,
                        stock_anterior, stock_nuevo, fecha_actividad, id_empleado_responsable, notas
                    ) VALUES (
                        :id_log, :tipo_actividad, :id_producto, :id_sucursal, :cantidad_afectada,
                        :stock_anterior, :stock_nuevo, :fecha_actividad, :id_empleado_responsable, :notas
                    )
                """
                cursor.execute(sql, {
                    "id_log": id_log, "tipo_actividad": tipo_actividad, "id_producto": id_producto, 
                    "id_sucursal": id_sucursal, "cantidad_afectada": cantidad_afectada,
                    "stock_anterior": stock_anterior, "stock_nuevo": stock_nuevo, 
                    "fecha_actividad": fecha_act_to_insert, 
                    "id_empleado_responsable": id_empleado_responsable, "notas": notas
                })
                cone.commit()
                return {"Mensaje": "Log de actividad de inventario creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de log ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El producto, sucursal o empleado especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear log de actividad: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear log de actividad: {str(ex)}")

@app.put("/log_actividad_inventarioputid/{id_log_param}", tags=["Log Actividad Inventario"])
def actualizar_log_actividad_inventario(
    id_log_param: int,
    tipo_actividad: str,
    id_producto: Optional[int] = None,
    id_sucursal: Optional[int] = None,
    cantidad_afectada: Optional[int] = None,
    stock_anterior: Optional[int] = None,
    stock_nuevo: Optional[int] = None,
    fecha_actividad: Optional[str] = None, 
    id_empleado_responsable: Optional[int] = None,
    notas: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_log FROM log_actividad_inventario WHERE id_log = :id_check", {"id_check": id_log_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Log de actividad con id {id_log_param} no encontrado para actualizar.")

                if id_producto is not None:
                    cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_prod", {"p_id_prod": id_producto})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                if id_empleado_responsable is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_responsable})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado_responsable} no existe.")
                
                fecha_act_to_update = date.today()
                if fecha_actividad:
                    try:
                        fecha_act_to_update = date.fromisoformat(fecha_actividad)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_actividad inválido. Usar YYYY-MM-DD.")
                
                sql = """
                    UPDATE log_actividad_inventario SET
                        tipo_actividad = :tipo_actividad,
                        id_producto = :id_producto,
                        id_sucursal = :id_sucursal,
                        cantidad_afectada = :cantidad_afectada,
                        stock_anterior = :stock_anterior,
                        stock_nuevo = :stock_nuevo,
                        fecha_actividad = :fecha_actividad,
                        id_empleado_responsable = :id_empleado_responsable,
                        notas = :notas
                    WHERE id_log = :id_log_bind
                """
                cursor.execute(sql, {
                    "tipo_actividad": tipo_actividad, "id_producto": id_producto, "id_sucursal": id_sucursal,
                    "cantidad_afectada": cantidad_afectada, "stock_anterior": stock_anterior, "stock_nuevo": stock_nuevo,
                    "fecha_actividad": fecha_act_to_update, "id_empleado_responsable": id_empleado_responsable,
                    "notas": notas, "id_log_bind": id_log_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Log de actividad no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Log de actividad de inventario actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El producto, sucursal o empleado especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar log: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar log: {str(ex)}")

@app.delete("/log_actividad_inventariodelete/{id_log_param}", tags=["Log Actividad Inventario"])
def eliminar_log_actividad_inventario(id_log_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM log_actividad_inventario WHERE id_log = :id_log_bind", {"id_log_bind": id_log_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Log de actividad con id {id_log_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Log de actividad de inventario eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar log: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar log: {str(ex)}")

@app.patch("/log_actividad_inventariopatch/{id_log_param}", tags=["Log Actividad Inventario"])
def actualizar_parcial_log_actividad_inventario(
    id_log_param: int,
    tipo_actividad: Optional[str] = None,
    id_producto: Optional[int] = None,
    id_sucursal: Optional[int] = None,
    cantidad_afectada: Optional[int] = None,
    stock_anterior: Optional[int] = None,
    stock_nuevo: Optional[int] = None,
    fecha_actividad: Optional[str] = None,
    id_empleado_responsable: Optional[int] = None,
    notas: Optional[str] = None
):
    try:
        if not any([
            tipo_actividad, id_producto is not None, id_sucursal is not None, cantidad_afectada is not None,
            stock_anterior is not None, stock_nuevo is not None, fecha_actividad, 
            id_empleado_responsable is not None, notas is not None
        ]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_log FROM log_actividad_inventario WHERE id_log = :id_check", {"id_check": id_log_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Log de actividad con id {id_log_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_log_bind": id_log_param}

                if tipo_actividad is not None:
                    set_clauses.append("tipo_actividad = :p_tipo_actividad")
                    bind_vars["p_tipo_actividad"] = tipo_actividad
                if id_producto is not None:
                    cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_prod", {"p_id_prod": id_producto})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                    set_clauses.append("id_producto = :p_id_producto")
                    bind_vars["p_id_producto"] = id_producto
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                    set_clauses.append("id_sucursal = :p_id_sucursal")
                    bind_vars["p_id_sucursal"] = id_sucursal
                if cantidad_afectada is not None:
                    set_clauses.append("cantidad_afectada = :p_cantidad_afectada")
                    bind_vars["p_cantidad_afectada"] = cantidad_afectada
                if stock_anterior is not None:
                    set_clauses.append("stock_anterior = :p_stock_anterior")
                    bind_vars["p_stock_anterior"] = stock_anterior
                if stock_nuevo is not None:
                    set_clauses.append("stock_nuevo = :p_stock_nuevo")
                    bind_vars["p_stock_nuevo"] = stock_nuevo
                if fecha_actividad is not None:
                    try:
                        parsed_date = date.fromisoformat(fecha_actividad)
                        set_clauses.append("fecha_actividad = :p_fecha_actividad")
                        bind_vars["p_fecha_actividad"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_actividad inválido. Usar YYYY-MM-DD.")
                else:
                    set_clauses.append("fecha_actividad = SYSDATE")

                if id_empleado_responsable is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_responsable})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado_responsable} no existe.")
                    set_clauses.append("id_empleado_responsable = :p_id_empleado_responsable")
                    bind_vars["p_id_empleado_responsable"] = id_empleado_responsable
                if notas is not None:
                    set_clauses.append("notas = :p_notas")
                    bind_vars["p_notas"] = notas
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE log_actividad_inventario SET {', '.join(set_clauses)} WHERE id_log = :p_id_log_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Log de actividad de inventario actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El producto, sucursal o empleado especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente log: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente log: {str(ex)}")

# CRUD Pedido

@app.get("/pedidoget", tags=["Pedidos"])
def listar_pedidos():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_pedido, fecha_pedido, id_cliente, id_empleado_vendedor,
                           id_sucursal_origen, id_estado_pedido, total_pedido
                    FROM pedido ORDER BY fecha_pedido DESC, id_pedido DESC
                """
                cursor.execute(sql)
                pedidos = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        row_as_dict = dict(zip(column_names, row_tuple))
                        if row_as_dict.get('fecha_pedido') and isinstance(row_as_dict['fecha_pedido'], date):
                            row_as_dict['fecha_pedido'] = row_as_dict['fecha_pedido'].isoformat()
                        pedidos.append(row_as_dict)
                return pedidos
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar pedidos: {str(e)}")

@app.get("/pedidogetid/{id_pedido_param}", tags=["Pedidos"])
def obtener_pedido_por_id(id_pedido_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_pedido, fecha_pedido, id_cliente, id_empleado_vendedor,
                           id_sucursal_origen, id_estado_pedido, total_pedido
                    FROM pedido WHERE id_pedido = :id_ped_bind
                """
                cursor.execute(sql, {"id_ped_bind": id_pedido_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        row_as_dict = dict_from_row(cursor, row)
                        if row_as_dict.get('fecha_pedido') and isinstance(row_as_dict['fecha_pedido'], date):
                            row_as_dict['fecha_pedido'] = row_as_dict['fecha_pedido'].isoformat()
                        return row_as_dict
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pedido con id {id_pedido_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener pedido: {str(ex)}")

@app.post("/pedidopost", tags=["Pedidos"])
def agregar_pedido(
    id_pedido: int,
    id_estado_pedido: int,
    fecha_pedido_str: Optional[str] = None,
    id_cliente: Optional[int] = None,
    id_empleado_vendedor: Optional[int] = None,
    id_sucursal_origen: Optional[int] = None,
    total_pedido: float = 0.0
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido", {"p_id_pedido": id_pedido})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe un pedido con el ID {id_pedido}.")

                if id_cliente is not None:
                    cursor.execute("SELECT id_cliente FROM cliente WHERE id_cliente = :p_id_cli", {"p_id_cli": id_cliente})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El cliente con id {id_cliente} no existe.")
                if id_empleado_vendedor is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_vendedor})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado_vendedor} no existe.")
                if id_sucursal_origen is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal_origen})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal_origen} no existe.")
                
                cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE id_estado_pedido = :p_id_est_ped", {"p_id_est_ped": id_estado_pedido})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El estado de pedido con id {id_estado_pedido} no existe.")

                fecha_para_insertar = None
                if fecha_pedido_str:
                    try:
                        fecha_para_insertar = date.fromisoformat(fecha_pedido_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_pedido inválido. Usar YYYY-MM-DD.")
                
                sql = """
                    INSERT INTO pedido (
                        id_pedido, fecha_pedido, id_cliente, id_empleado_vendedor,
                        id_sucursal_origen, id_estado_pedido, total_pedido
                    ) VALUES (
                        :id_pedido, NVL(:fecha_pedido, SYSDATE), :id_cliente, :id_empleado_vendedor,
                        :id_sucursal_origen, :id_estado_pedido, :total_pedido
                    )
                """
                cursor.execute(sql, {
                    "id_pedido": id_pedido, "fecha_pedido": fecha_para_insertar, "id_cliente": id_cliente,
                    "id_empleado_vendedor": id_empleado_vendedor, "id_sucursal_origen": id_sucursal_origen,
                    "id_estado_pedido": id_estado_pedido, "total_pedido": total_pedido
                })
                cone.commit()
                return {"Mensaje": "Pedido creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de pedido ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Cliente, empleado, sucursal o estado de pedido no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear pedido: {str(ex)}")

@app.put("/pedidoputid/{id_pedido_param}", tags=["Pedidos"])
def actualizar_pedido(
    id_pedido_param: int,
    id_estado_pedido: int,
    fecha_pedido_str: Optional[str] = None,
    id_cliente: Optional[int] = None,
    id_empleado_vendedor: Optional[int] = None,
    id_sucursal_origen: Optional[int] = None,
    total_pedido: Optional[float] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :id_check", {"id_check": id_pedido_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pedido con id {id_pedido_param} no encontrado para actualizar.")

                if id_cliente is not None:
                    cursor.execute("SELECT id_cliente FROM cliente WHERE id_cliente = :p_id_cli", {"p_id_cli": id_cliente})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El cliente con id {id_cliente} no existe.")
                if id_empleado_vendedor is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_vendedor})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado_vendedor} no existe.")
                if id_sucursal_origen is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal_origen})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal_origen} no existe.")
                
                cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE id_estado_pedido = :p_id_est_ped", {"p_id_est_ped": id_estado_pedido})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El estado de pedido con id {id_estado_pedido} no existe.")

                fecha_para_update = None
                if fecha_pedido_str:
                    try:
                        fecha_para_update = date.fromisoformat(fecha_pedido_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_pedido inválido. Usar YYYY-MM-DD.")
                else:
                    fecha_para_update = date.today()
                sql = """
                    UPDATE pedido SET
                        fecha_pedido = NVL(:fecha_pedido, fecha_pedido), 
                        id_cliente = :id_cliente, 
                        id_empleado_vendedor = :id_empleado_vendedor,
                        id_sucursal_origen = :id_sucursal_origen,
                        id_estado_pedido = :id_estado_pedido,
                        total_pedido = NVL(:total_pedido, total_pedido)
                    WHERE id_pedido = :id_pedido_bind
                """
                cursor.execute(sql, {
                    "fecha_pedido": fecha_para_update, "id_cliente": id_cliente,
                    "id_empleado_vendedor": id_empleado_vendedor, "id_sucursal_origen": id_sucursal_origen,
                    "id_estado_pedido": id_estado_pedido, "total_pedido": total_pedido,
                    "id_pedido_bind": id_pedido_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pedido no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Pedido actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Cliente, empleado, sucursal o estado de pedido no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar pedido: {str(ex)}")

@app.delete("/pedidodelete/{id_pedido_param}", tags=["Pedidos"])
def eliminar_pedido(id_pedido_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM pedido WHERE id_pedido = :id_ped_bind", {"id_ped_bind": id_pedido_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pedido con id {id_pedido_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Pedido eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar el pedido. Existen registros (detalle_pedido, factura) que dependen de él.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar pedido: {str(ex)}")

@app.patch("/pedidopatch/{id_pedido_param}", tags=["Pedidos"])
def actualizar_parcial_pedido(
    id_pedido_param: int,
    fecha_pedido_str: Optional[str] = None,
    id_cliente: Optional[int] = None,
    id_empleado_vendedor: Optional[int] = None,
    id_sucursal_origen: Optional[int] = None,
    id_estado_pedido: Optional[int] = None,
    total_pedido: Optional[float] = None
):
    try:
        if not any([fecha_pedido_str, id_cliente is not None, id_empleado_vendedor is not None, 
                    id_sucursal_origen is not None, id_estado_pedido is not None, total_pedido is not None]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :id_check", {"id_check": id_pedido_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Pedido con id {id_pedido_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_pedido_bind": id_pedido_param}

                if fecha_pedido_str is not None:
                    try:
                        parsed_date = date.fromisoformat(fecha_pedido_str)
                        set_clauses.append("fecha_pedido = :p_fecha_pedido")
                        bind_vars["p_fecha_pedido"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_pedido inválido. Usar YYYY-MM-DD.")
                if id_cliente is not None:
                    cursor.execute("SELECT id_cliente FROM cliente WHERE id_cliente = :p_id_cli", {"p_id_cli": id_cliente})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El cliente con id {id_cliente} no existe.")
                    set_clauses.append("id_cliente = :p_id_cliente")
                    bind_vars["p_id_cliente"] = id_cliente
                if id_empleado_vendedor is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_vendedor})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado_vendedor} no existe.")
                    set_clauses.append("id_empleado_vendedor = :p_id_empleado_vendedor")
                    bind_vars["p_id_empleado_vendedor"] = id_empleado_vendedor
                if id_sucursal_origen is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal_origen})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal_origen} no existe.")
                    set_clauses.append("id_sucursal_origen = :p_id_sucursal_origen")
                    bind_vars["p_id_sucursal_origen"] = id_sucursal_origen
                if id_estado_pedido is not None:
                    cursor.execute("SELECT id_estado_pedido FROM estado_pedido WHERE id_estado_pedido = :p_id_est_ped", {"p_id_est_ped": id_estado_pedido})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El estado de pedido con id {id_estado_pedido} no existe.")
                    set_clauses.append("id_estado_pedido = :p_id_estado_pedido")
                    bind_vars["p_id_estado_pedido"] = id_estado_pedido
                if total_pedido is not None:
                    set_clauses.append("total_pedido = :p_total_pedido")
                    bind_vars["p_total_pedido"] = total_pedido
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE pedido SET {', '.join(set_clauses)} WHERE id_pedido = :p_id_pedido_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Pedido actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Cliente, empleado, sucursal o estado de pedido no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente pedido: {str(ex)}")

# CRUD Detalle Pedido

@app.get("/detalle_pedidoget", tags=["Detalle Pedido"])
def listar_detalle_pedidos():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_detalle_pedido, id_pedido, id_producto, cantidad, 
                           precio_unitario_venta, subtotal
                    FROM detalle_pedido ORDER BY id_detalle_pedido
                """
                cursor.execute(sql)
                detalles = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        detalles.append(dict(zip(column_names, row_tuple)))
                return detalles
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar detalles de pedido: {str(e)}")

@app.get("/detalle_pedidogetid/{id_detalle_pedido_param}", tags=["Detalle Pedido"])
def obtener_detalle_pedido_por_id(id_detalle_pedido_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_detalle_pedido, id_pedido, id_producto, cantidad, 
                           precio_unitario_venta, subtotal
                    FROM detalle_pedido WHERE id_detalle_pedido = :id_dp_bind
                """
                cursor.execute(sql, {"id_dp_bind": id_detalle_pedido_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        column_names = [desc[0].lower() for desc in cursor.description]
                        return dict(zip(column_names, row))
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Detalle de pedido con id {id_detalle_pedido_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener detalle de pedido: {str(ex)}")

@app.post("/detalle_pedidopost", tags=["Detalle Pedido"])
def agregar_detalle_pedido(
    id_detalle_pedido: int,
    id_pedido: int,
    id_producto: int,
    cantidad: int,
    precio_unitario_venta: float,
    subtotal: float 
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_detalle_pedido FROM detalle_pedido WHERE id_detalle_pedido = :p_id_dp", {"p_id_dp": id_detalle_pedido})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe un detalle de pedido con el ID {id_detalle_pedido}.")

                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido", {"p_id_pedido": id_pedido})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El pedido con id {id_pedido} no existe.")
                
                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_producto", {"p_id_producto": id_producto})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")

                if cantidad <= 0:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La cantidad debe ser mayor que cero.")

                sql = """
                    INSERT INTO detalle_pedido (
                        id_detalle_pedido, id_pedido, id_producto, cantidad, 
                        precio_unitario_venta, subtotal
                    ) VALUES (
                        :id_detalle_pedido, :id_pedido, :id_producto, :cantidad, 
                        :precio_unitario_venta, :subtotal
                    )
                """
                cursor.execute(sql, {
                    "id_detalle_pedido": id_detalle_pedido, "id_pedido": id_pedido, "id_producto": id_producto,
                    "cantidad": cantidad, "precio_unitario_venta": precio_unitario_venta, "subtotal": subtotal
                })
                cone.commit()
                return {"Mensaje": "Detalle de pedido creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de detalle_pedido ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El pedido o producto especificado no existe.")
        elif error_obj.code == 2290:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La cantidad debe ser mayor que cero.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear detalle de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear detalle de pedido: {str(ex)}")

@app.put("/detalle_pedidoputid/{id_detalle_pedido_param}", tags=["Detalle Pedido"])
def actualizar_detalle_pedido(
    id_detalle_pedido_param: int,
    id_pedido: int,
    id_producto: int,
    cantidad: int,
    precio_unitario_venta: float,
    subtotal: float
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_detalle_pedido FROM detalle_pedido WHERE id_detalle_pedido = :id_check", {"id_check": id_detalle_pedido_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Detalle de pedido con id {id_detalle_pedido_param} no encontrado para actualizar.")

                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido", {"p_id_pedido": id_pedido})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El pedido con id {id_pedido} no existe.")
                cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_producto", {"p_id_producto": id_producto})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")

                if cantidad <= 0:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La cantidad debe ser mayor que cero.")

                sql = """
                    UPDATE detalle_pedido SET
                        id_pedido = :id_pedido,
                        id_producto = :id_producto,
                        cantidad = :cantidad,
                        precio_unitario_venta = :precio_unitario_venta,
                        subtotal = :subtotal
                    WHERE id_detalle_pedido = :id_dp_bind
                """
                cursor.execute(sql, {
                    "id_pedido": id_pedido, "id_producto": id_producto, "cantidad": cantidad,
                    "precio_unitario_venta": precio_unitario_venta, "subtotal": subtotal,
                    "id_dp_bind": id_detalle_pedido_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Detalle de pedido no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Detalle de pedido actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El pedido o producto especificado no existe.")
        elif error_obj.code == 2290:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La cantidad debe ser mayor que cero.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar detalle de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar detalle de pedido: {str(ex)}")

@app.delete("/detalle_pedidodelete/{id_detalle_pedido_param}", tags=["Detalle Pedido"])
def eliminar_detalle_pedido(id_detalle_pedido_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM detalle_pedido WHERE id_detalle_pedido = :id_dp_bind", {"id_dp_bind": id_detalle_pedido_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Detalle de pedido con id {id_detalle_pedido_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Detalle de pedido eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar detalle de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar detalle de pedido: {str(ex)}")

@app.patch("/detalle_pedidopatch/{id_detalle_pedido_param}", tags=["Detalle Pedido"])
def actualizar_parcial_detalle_pedido(
    id_detalle_pedido_param: int,
    id_pedido: Optional[int] = None,
    id_producto: Optional[int] = None,
    cantidad: Optional[int] = None,
    precio_unitario_venta: Optional[float] = None,
    subtotal: Optional[float] = None
):
    try:
        if not any([id_pedido is not None, id_producto is not None, cantidad is not None, precio_unitario_venta is not None, subtotal is not None]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_detalle_pedido FROM detalle_pedido WHERE id_detalle_pedido = :id_check", {"id_check": id_detalle_pedido_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Detalle de pedido con id {id_detalle_pedido_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_dp_bind": id_detalle_pedido_param}

                if id_pedido is not None:
                    cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido", {"p_id_pedido": id_pedido})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El pedido con id {id_pedido} no existe.")
                    set_clauses.append("id_pedido = :p_id_pedido")
                    bind_vars["p_id_pedido"] = id_pedido
                if id_producto is not None:
                    cursor.execute("SELECT id_producto FROM productos WHERE id_producto = :p_id_producto", {"p_id_producto": id_producto})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El producto con id {id_producto} no existe.")
                    set_clauses.append("id_producto = :p_id_producto")
                    bind_vars["p_id_producto"] = id_producto
                if cantidad is not None:
                    if cantidad <= 0: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="La cantidad debe ser mayor que cero.")
                    set_clauses.append("cantidad = :p_cantidad")
                    bind_vars["p_cantidad"] = cantidad
                if precio_unitario_venta is not None:
                    set_clauses.append("precio_unitario_venta = :p_precio_unitario_venta")
                    bind_vars["p_precio_unitario_venta"] = precio_unitario_venta
                if subtotal is not None:
                    set_clauses.append("subtotal = :p_subtotal")
                    bind_vars["p_subtotal"] = subtotal
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE detalle_pedido SET {', '.join(set_clauses)} WHERE id_detalle_pedido = :p_id_dp_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Detalle de pedido actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El pedido o producto especificado no existe.")
        elif error_obj.code == 2290:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La cantidad debe ser mayor que cero.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente detalle de pedido: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente detalle de pedido: {str(ex)}")

# CRUD Factura

@app.get("/facturaget", tags=["Factura"])
def listar_facturas():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_factura, numero_factura, id_pedido, fecha_emision, 
                           total_neto, iva, total_con_iva
                    FROM factura ORDER BY fecha_emision DESC, id_factura DESC
                """
                cursor.execute(sql)
                facturas = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        row_as_dict = dict(zip(column_names, row_tuple))
                        if row_as_dict.get('fecha_emision') and isinstance(row_as_dict['fecha_emision'], date):
                            row_as_dict['fecha_emision'] = row_as_dict['fecha_emision'].isoformat()
                        facturas.append(row_as_dict)
                return facturas
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar facturas: {str(e)}")

@app.get("/facturagetid/{id_factura_param}", tags=["Factura"])
def obtener_factura_por_id(id_factura_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_factura, numero_factura, id_pedido, fecha_emision, 
                           total_neto, iva, total_con_iva
                    FROM factura WHERE id_factura = :id_fac_bind
                """
                cursor.execute(sql, {"id_fac_bind": id_factura_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        row_as_dict = dict_from_row(cursor, row)
                        if row_as_dict.get('fecha_emision') and isinstance(row_as_dict['fecha_emision'], date):
                            row_as_dict['fecha_emision'] = row_as_dict['fecha_emision'].isoformat()
                        return row_as_dict
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Factura con id {id_factura_param} no encontrada")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener factura: {str(ex)}")

@app.post("/facturapost", tags=["Factura"])
def agregar_factura(
    id_factura: int,
    numero_factura: str,
    id_pedido: int,
    total_neto: float,
    iva: float,
    total_con_iva: float,
    fecha_emision_str: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_factura FROM factura WHERE id_factura = :p_id_fac OR numero_factura = :p_num_fac OR id_pedido = :p_id_ped",
                               {"p_id_fac": id_factura, "p_num_fac": numero_factura, "p_id_ped": id_pedido})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Ya existe una factura con ese ID, número de factura o ID de pedido.")

                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido", {"p_id_pedido": id_pedido})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El pedido con id {id_pedido} no existe.")

                fecha_para_insertar = None
                if fecha_emision_str:
                    try:
                        fecha_para_insertar = date.fromisoformat(fecha_emision_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_emision inválido. Usar YYYY-MM-DD.")
                
                sql = """
                    INSERT INTO factura (
                        id_factura, numero_factura, id_pedido, fecha_emision,
                        total_neto, iva, total_con_iva
                    ) VALUES (
                        :id_factura, :numero_factura, :id_pedido, NVL(:fecha_emision, SYSDATE),
                        :total_neto, :iva, :total_con_iva
                    )
                """
                cursor.execute(sql, {
                    "id_factura": id_factura, "numero_factura": numero_factura, "id_pedido": id_pedido,
                    "fecha_emision": fecha_para_insertar, "total_neto": total_neto,
                    "iva": iva, "total_con_iva": total_con_iva
                })
                cone.commit()
                return {"Mensaje": "Factura creada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de factura, número de factura o ID de pedido ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El pedido especificado no existe.")
        elif error_obj.code == 1400:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Campos requeridos (numero_factura, id_pedido, total_neto, iva, total_con_iva) no pueden ser nulos.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear factura: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear factura: {str(ex)}")

@app.put("/facturaputid/{id_factura_param}", tags=["Factura"])
def actualizar_factura(
    id_factura_param: int,
    numero_factura: str,
    id_pedido: int,
    total_neto: float,
    iva: float,
    total_con_iva: float,
    fecha_emision_str: Optional[str] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_factura FROM factura WHERE id_factura = :id_check", {"id_check": id_factura_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Factura con id {id_factura_param} no encontrada para actualizar.")

                cursor.execute("SELECT id_factura FROM factura WHERE numero_factura = :p_num_fac AND id_factura != :p_id_fac",
                               {"p_num_fac": numero_factura, "p_id_fac": id_factura_param})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El número de factura '{numero_factura}' ya está en uso por otra factura.")
                
                cursor.execute("SELECT id_factura FROM factura WHERE id_pedido = :p_id_ped AND id_factura != :p_id_fac",
                               {"p_id_ped": id_pedido, "p_id_fac": id_factura_param})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El pedido con id {id_pedido} ya está asociado a otra factura.")

                cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido", {"p_id_pedido": id_pedido})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El pedido con id {id_pedido} no existe.")

                fecha_para_update = None
                if fecha_emision_str:
                    try:
                        fecha_para_update = date.fromisoformat(fecha_emision_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_emision inválido. Usar YYYY-MM-DD.")
                
                sql = """
                    UPDATE factura SET
                        numero_factura = :numero_factura,
                        id_pedido = :id_pedido,
                        fecha_emision = NVL(:fecha_emision, fecha_emision),
                        total_neto = :total_neto,
                        iva = :iva,
                        total_con_iva = :total_con_iva
                    WHERE id_factura = :id_factura_bind
                """
                cursor.execute(sql, {
                    "numero_factura": numero_factura, "id_pedido": id_pedido, 
                    "fecha_emision": fecha_para_update, "total_neto": total_neto,
                    "iva": iva, "total_con_iva": total_con_iva,
                    "id_factura_bind": id_factura_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Factura no encontrada o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Factura actualizada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (número de factura o ID de pedido ya existe).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El pedido especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar factura: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar factura: {str(ex)}")

@app.delete("/facturadelete/{id_factura_param}", tags=["Factura"])
def eliminar_factura(id_factura_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM factura WHERE id_factura = :id_fac_bind", {"id_fac_bind": id_factura_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Factura con id {id_factura_param} no encontrada")
                cone.commit()
                return {"Mensaje": "Factura eliminada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2292: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"No se puede eliminar la factura. Existen transacciones asociadas a ella.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar factura: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar factura: {str(ex)}")

@app.patch("/facturapatch/{id_factura_param}", tags=["Factura"])
def actualizar_parcial_factura(
    id_factura_param: int,
    numero_factura: Optional[str] = None,
    id_pedido: Optional[int] = None,
    fecha_emision_str: Optional[str] = None,
    total_neto: Optional[float] = None,
    iva: Optional[float] = None,
    total_con_iva: Optional[float] = None
):
    try:
        if not any([numero_factura, id_pedido is not None, fecha_emision_str, 
                    total_neto is not None, iva is not None, total_con_iva is not None]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_factura FROM factura WHERE id_factura = :id_check", {"id_check": id_factura_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Factura con id {id_factura_param} no encontrada para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_factura_bind": id_factura_param}

                if numero_factura is not None:
                    cursor.execute("SELECT id_factura FROM factura WHERE numero_factura = :p_num_fac AND id_factura != :p_id_fac",
                                   {"p_num_fac": numero_factura, "p_id_fac": id_factura_param})
                    if cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El número de factura '{numero_factura}' ya está en uso.")
                    set_clauses.append("numero_factura = :p_numero_factura")
                    bind_vars["p_numero_factura"] = numero_factura
                
                if id_pedido is not None:
                    cursor.execute("SELECT id_factura FROM factura WHERE id_pedido = :p_id_ped AND id_factura != :p_id_fac",
                                   {"p_id_ped": id_pedido, "p_id_fac": id_factura_param})
                    if cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"El pedido con id {id_pedido} ya está asociado a otra factura.")
                    cursor.execute("SELECT id_pedido FROM pedido WHERE id_pedido = :p_id_pedido_check", {"p_id_pedido_check": id_pedido})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El pedido con id {id_pedido} no existe.")
                    set_clauses.append("id_pedido = :p_id_pedido")
                    bind_vars["p_id_pedido"] = id_pedido

                if fecha_emision_str is not None:
                    try:
                        parsed_date = date.fromisoformat(fecha_emision_str)
                        set_clauses.append("fecha_emision = :p_fecha_emision")
                        bind_vars["p_fecha_emision"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_emision inválido. Usar YYYY-MM-DD.")
                
                if total_neto is not None:
                    set_clauses.append("total_neto = :p_total_neto")
                    bind_vars["p_total_neto"] = total_neto
                if iva is not None:
                    set_clauses.append("iva = :p_iva")
                    bind_vars["p_iva"] = iva
                if total_con_iva is not None:
                    set_clauses.append("total_con_iva = :p_total_con_iva")
                    bind_vars["p_total_con_iva"] = total_con_iva
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE factura SET {', '.join(set_clauses)} WHERE id_factura = :p_id_factura_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Factura actualizada parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (número de factura o ID de pedido ya existe).")
        elif error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El pedido especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente factura: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente factura: {str(ex)}")

# CRUD Transacción

@app.get("/transaccionget", tags=["Transacción"])
def listar_transacciones():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_transaccion, id_factura, id_tipo_transaccion, monto_pagado,
                           fecha_transaccion, referencia_pago, id_empleado_cajero
                    FROM transaccion ORDER BY fecha_transaccion DESC, id_transaccion DESC
                """
                cursor.execute(sql)
                transacciones = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        row_as_dict = dict(zip(column_names, row_tuple))
                        if row_as_dict.get('fecha_transaccion') and isinstance(row_as_dict['fecha_transaccion'], date):
                            row_as_dict['fecha_transaccion'] = row_as_dict['fecha_transaccion'].isoformat()
                        transacciones.append(row_as_dict)
                return transacciones
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar transacciones: {str(e)}")

@app.get("/transacciongetid/{id_transaccion_param}", tags=["Transacción"])
def obtener_transaccion_por_id(id_transaccion_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_transaccion, id_factura, id_tipo_transaccion, monto_pagado,
                           fecha_transaccion, referencia_pago, id_empleado_cajero
                    FROM transaccion WHERE id_transaccion = :id_trans_bind
                """
                cursor.execute(sql, {"id_trans_bind": id_transaccion_param})
                row = cursor.fetchone()
                if row:
                    if cursor.description:
                        row_as_dict = dict_from_row(cursor, row)
                        if row_as_dict.get('fecha_transaccion') and isinstance(row_as_dict['fecha_transaccion'], date):
                            row_as_dict['fecha_transaccion'] = row_as_dict['fecha_transaccion'].isoformat()
                        return row_as_dict
                    else:
                        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al obtener nombres de columnas.")
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Transacción con id {id_transaccion_param} no encontrada")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener transacción: {str(ex)}")

@app.post("/transaccionpost", tags=["Transacción"])
def agregar_transaccion(
    id_transaccion: int,
    id_factura: int,
    id_tipo_transaccion: int,
    monto_pagado: float,
    fecha_transaccion_str: Optional[str] = None,
    referencia_pago: Optional[str] = None,
    id_empleado_cajero: Optional[int] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_transaccion FROM transaccion WHERE id_transaccion = :p_id_trans", {"p_id_trans": id_transaccion})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe una transacción con el ID {id_transaccion}.")

                cursor.execute("SELECT id_factura FROM factura WHERE id_factura = :p_id_fac", {"p_id_fac": id_factura})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La factura con id {id_factura} no existe.")
                
                cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE id_tipo_transaccion = :p_id_tt", {"p_id_tt": id_tipo_transaccion})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El tipo de transacción con id {id_tipo_transaccion} no existe.")
                
                if id_empleado_cajero is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_cajero})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado cajero con id {id_empleado_cajero} no existe.")

                fecha_para_insertar = None
                if fecha_transaccion_str:
                    try:
                        fecha_para_insertar = date.fromisoformat(fecha_transaccion_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_transaccion inválido. Usar YYYY-MM-DD.")
                
                sql = """
                    INSERT INTO transaccion (
                        id_transaccion, id_factura, id_tipo_transaccion, monto_pagado,
                        fecha_transaccion, referencia_pago, id_empleado_cajero
                    ) VALUES (
                        :id_transaccion, :id_factura, :id_tipo_transaccion, :monto_pagado,
                        NVL(:fecha_transaccion, SYSDATE), :referencia_pago, :id_empleado_cajero
                    )
                """
                cursor.execute(sql, {
                    "id_transaccion": id_transaccion, "id_factura": id_factura, "id_tipo_transaccion": id_tipo_transaccion,
                    "monto_pagado": monto_pagado, "fecha_transaccion": fecha_para_insertar,
                    "referencia_pago": referencia_pago, "id_empleado_cajero": id_empleado_cajero
                })
                cone.commit()
                return {"Mensaje": "Transacción creada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de transacción ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Factura, tipo de transacción o empleado cajero no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear transacción: {str(ex)}")

@app.put("/transaccionputid/{id_transaccion_param}", tags=["Transacción"])
def actualizar_transaccion(
    id_transaccion_param: int,
    id_factura: int,
    id_tipo_transaccion: int,
    monto_pagado: float,
    fecha_transaccion_str: Optional[str] = None,
    referencia_pago: Optional[str] = None,
    id_empleado_cajero: Optional[int] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_transaccion FROM transaccion WHERE id_transaccion = :id_check", {"id_check": id_transaccion_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Transacción con id {id_transaccion_param} no encontrada para actualizar.")

                cursor.execute("SELECT id_factura FROM factura WHERE id_factura = :p_id_fac", {"p_id_fac": id_factura})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La factura con id {id_factura} no existe.")
                
                cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE id_tipo_transaccion = :p_id_tt", {"p_id_tt": id_tipo_transaccion})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El tipo de transacción con id {id_tipo_transaccion} no existe.")
                
                if id_empleado_cajero is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_cajero})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado cajero con id {id_empleado_cajero} no existe.")

                fecha_para_update = None
                if fecha_transaccion_str:
                    try:
                        fecha_para_update = date.fromisoformat(fecha_transaccion_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_transaccion inválido. Usar YYYY-MM-DD.")
                
                sql = """
                    UPDATE transaccion SET
                        id_factura = :id_factura,
                        id_tipo_transaccion = :id_tipo_transaccion,
                        monto_pagado = :monto_pagado,
                        fecha_transaccion = NVL(:fecha_transaccion, fecha_transaccion),
                        referencia_pago = :referencia_pago,
                        id_empleado_cajero = :id_empleado_cajero
                    WHERE id_transaccion = :id_trans_bind
                """
                cursor.execute(sql, {
                    "id_factura": id_factura, "id_tipo_transaccion": id_tipo_transaccion,
                    "monto_pagado": monto_pagado, "fecha_transaccion": fecha_para_update,
                    "referencia_pago": referencia_pago, "id_empleado_cajero": id_empleado_cajero,
                    "id_trans_bind": id_transaccion_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transacción no encontrada o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Transacción actualizada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Factura, tipo de transacción o empleado cajero no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar transacción: {str(ex)}")

@app.delete("/transacciondelete/{id_transaccion_param}", tags=["Transacción"])
def eliminar_transaccion(id_transaccion_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM transaccion WHERE id_transaccion = :id_trans_bind", {"id_trans_bind": id_transaccion_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Transacción con id {id_transaccion_param} no encontrada")
                cone.commit()
                return {"Mensaje": "Transacción eliminada con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar transacción: {str(ex)}")

@app.patch("/transaccionpatch/{id_transaccion_param}", tags=["Transacción"])
def actualizar_parcial_transaccion(
    id_transaccion_param: int,
    id_factura: Optional[int] = None,
    id_tipo_transaccion: Optional[int] = None,
    monto_pagado: Optional[float] = None,
    fecha_transaccion_str: Optional[str] = None,
    referencia_pago: Optional[str] = None,
    id_empleado_cajero: Optional[int] = None
):
    try:
        if not any([
            id_factura is not None, id_tipo_transaccion is not None, monto_pagado is not None,
            fecha_transaccion_str, referencia_pago is not None, id_empleado_cajero is not None
        ]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_transaccion FROM transaccion WHERE id_transaccion = :id_check", {"id_check": id_transaccion_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Transacción con id {id_transaccion_param} no encontrada para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_trans_bind": id_transaccion_param}

                if id_factura is not None:
                    cursor.execute("SELECT id_factura FROM factura WHERE id_factura = :p_id_fac", {"p_id_fac": id_factura})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La factura con id {id_factura} no existe.")
                    set_clauses.append("id_factura = :p_id_factura")
                    bind_vars["p_id_factura"] = id_factura
                
                if id_tipo_transaccion is not None:
                    cursor.execute("SELECT id_tipo_transaccion FROM tipo_transaccion WHERE id_tipo_transaccion = :p_id_tt", {"p_id_tt": id_tipo_transaccion})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El tipo de transacción con id {id_tipo_transaccion} no existe.")
                    set_clauses.append("id_tipo_transaccion = :p_id_tipo_transaccion")
                    bind_vars["p_id_tipo_transaccion"] = id_tipo_transaccion
                
                if monto_pagado is not None:
                    set_clauses.append("monto_pagado = :p_monto_pagado")
                    bind_vars["p_monto_pagado"] = monto_pagado
                
                if fecha_transaccion_str is not None:
                    try:
                        parsed_date = date.fromisoformat(fecha_transaccion_str)
                        set_clauses.append("fecha_transaccion = :p_fecha_transaccion")
                        bind_vars["p_fecha_transaccion"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_transaccion inválido. Usar YYYY-MM-DD.")
                
                if referencia_pago is not None:
                    set_clauses.append("referencia_pago = :p_referencia_pago")
                    bind_vars["p_referencia_pago"] = referencia_pago

                if id_empleado_cajero is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado_cajero})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado cajero con id {id_empleado_cajero} no existe.")
                    set_clauses.append("id_empleado_cajero = :p_id_empleado_cajero")
                    bind_vars["p_id_empleado_cajero"] = id_empleado_cajero
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE transaccion SET {', '.join(set_clauses)} WHERE id_transaccion = :p_id_trans_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Transacción actualizada parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: Factura, tipo de transacción o empleado cajero no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente transacción: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente transacción: {str(ex)}")

# CRUD Reporte Ventas

@app.get("/reporte_ventasget", tags=["Reporte Venta"])
def listar_reportes_ventas():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_reporte_ventas, fecha_generacion, periodo_inicio, 
                           periodo_fin, total_ventas_calculado, id_sucursal
                    FROM reporte_ventas ORDER BY fecha_generacion DESC, id_reporte_ventas DESC
                """
                cursor.execute(sql)
                reportes = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        row_as_dict = dict(zip(column_names, row_tuple))
                        if row_as_dict.get('fecha_generacion') and isinstance(row_as_dict['fecha_generacion'], date):
                            row_as_dict['fecha_generacion'] = row_as_dict['fecha_generacion'].isoformat()
                        if row_as_dict.get('periodo_inicio') and isinstance(row_as_dict['periodo_inicio'], date):
                            row_as_dict['periodo_inicio'] = row_as_dict['periodo_inicio'].isoformat()
                        if row_as_dict.get('periodo_fin') and isinstance(row_as_dict['periodo_fin'], date):
                            row_as_dict['periodo_fin'] = row_as_dict['periodo_fin'].isoformat()
                        reportes.append(row_as_dict)
                return reportes
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar reportes de ventas: {str(e)}")

@app.get("/reporte_ventasgetid/{id_reporte_ventas_param}", tags=["Reporte Venta"])
def obtener_reporte_ventas_por_id(id_reporte_ventas_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_reporte_ventas, fecha_generacion, periodo_inicio, 
                           periodo_fin, total_ventas_calculado, id_sucursal
                    FROM reporte_ventas WHERE id_reporte_ventas = :id_rv_bind
                """
                cursor.execute(sql, {"id_rv_bind": id_reporte_ventas_param})
                row = cursor.fetchone()
                if row:
                    return {
                        "id_reporte_ventas": row[0],
                        "fecha_generacion": row[1].isoformat() if isinstance(row[1], date) else row[1],
                        "periodo_inicio": row[2].isoformat() if isinstance(row[2], date) else row[2],
                        "periodo_fin": row[3].isoformat() if isinstance(row[3], date) else row[3],
                        "total_ventas_calculado": row[4],
                        "id_sucursal": row[5]
                    }
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de ventas con id {id_reporte_ventas_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener reporte de ventas: {str(ex)}")

@app.post("/reporte_ventaspost", tags=["Reporte Venta"])
def agregar_reporte_ventas(
    id_reporte_ventas: int,
    fecha_generacion_str: str, 
    total_ventas_calculado: float,
    periodo_inicio_str: Optional[str] = None,
    periodo_fin_str: Optional[str] = None,
    id_sucursal: Optional[int] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_reporte_ventas FROM reporte_ventas WHERE id_reporte_ventas = :p_id_rv", {"p_id_rv": id_reporte_ventas})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe un reporte de ventas con el ID {id_reporte_ventas}.")

                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                
                try:
                    fecha_g = date.fromisoformat(fecha_generacion_str)
                except ValueError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_generacion inválido. Usar YYYY-MM-DD.")
                
                fecha_pi = None
                if periodo_inicio_str:
                    try:
                        fecha_pi = date.fromisoformat(periodo_inicio_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_inicio inválido. Usar YYYY-MM-DD.")
                
                fecha_pf = None
                if periodo_fin_str:
                    try:
                        fecha_pf = date.fromisoformat(periodo_fin_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_fin inválido. Usar YYYY-MM-DD.")

                sql = """
                    INSERT INTO reporte_ventas (
                        id_reporte_ventas, fecha_generacion, periodo_inicio, 
                        periodo_fin, total_ventas_calculado, id_sucursal
                    ) VALUES (
                        :id_reporte_ventas, :fecha_generacion, :periodo_inicio, 
                        :periodo_fin, :total_ventas_calculado, :id_sucursal
                    )
                """
                cursor.execute(sql, {
                    "id_reporte_ventas": id_reporte_ventas, "fecha_generacion": fecha_g,
                    "periodo_inicio": fecha_pi, "periodo_fin": fecha_pf,
                    "total_ventas_calculado": total_ventas_calculado, "id_sucursal": id_sucursal
                })
                cone.commit()
                return {"Mensaje": "Reporte de ventas creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de reporte ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La sucursal especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear reporte de ventas: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear reporte de ventas: {str(ex)}")

@app.put("/reporte_ventasputid/{id_reporte_ventas_param}", tags=["Reporte Venta"])
def actualizar_reporte_ventas(
    id_reporte_ventas_param: int,
    fecha_generacion_str: str,
    total_ventas_calculado: float,
    periodo_inicio_str: Optional[str] = None,
    periodo_fin_str: Optional[str] = None,
    id_sucursal: Optional[int] = None
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_reporte_ventas FROM reporte_ventas WHERE id_reporte_ventas = :id_check", {"id_check": id_reporte_ventas_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de ventas con id {id_reporte_ventas_param} no encontrado para actualizar.")

                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone():
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                
                try:
                    fecha_g = date.fromisoformat(fecha_generacion_str)
                except ValueError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_generacion inválido. Usar YYYY-MM-DD.")
                
                fecha_pi = None
                if periodo_inicio_str:
                    try:
                        fecha_pi = date.fromisoformat(periodo_inicio_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_inicio inválido. Usar YYYY-MM-DD.")
                
                fecha_pf = None
                if periodo_fin_str:
                    try:
                        fecha_pf = date.fromisoformat(periodo_fin_str)
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_fin inválido. Usar YYYY-MM-DD.")

                sql = """
                    UPDATE reporte_ventas SET
                        fecha_generacion = :fecha_generacion,
                        periodo_inicio = :periodo_inicio,
                        periodo_fin = :periodo_fin,
                        total_ventas_calculado = :total_ventas_calculado,
                        id_sucursal = :id_sucursal
                    WHERE id_reporte_ventas = :id_rv_bind
                """
                cursor.execute(sql, {
                    "fecha_generacion": fecha_g, "periodo_inicio": fecha_pi, "periodo_fin": fecha_pf,
                    "total_ventas_calculado": total_ventas_calculado, "id_sucursal": id_sucursal,
                    "id_rv_bind": id_reporte_ventas_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reporte de ventas no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Reporte de ventas actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La sucursal especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar reporte de ventas: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar reporte de ventas: {str(ex)}")

@app.delete("/reporte_ventasdelete/{id_reporte_ventas_param}", tags=["Reporte Venta"])
def eliminar_reporte_ventas(id_reporte_ventas_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM reporte_ventas WHERE id_reporte_ventas = :id_rv_bind", {"id_rv_bind": id_reporte_ventas_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de ventas con id {id_reporte_ventas_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Reporte de ventas eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar reporte de ventas: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar reporte de ventas: {str(ex)}")

@app.patch("/reporte_ventaspatch/{id_reporte_ventas_param}", tags=["Reporte Venta"])
def actualizar_parcial_reporte_ventas(
    id_reporte_ventas_param: int,
    fecha_generacion_str: Optional[str] = None,
    periodo_inicio_str: Optional[str] = None,
    periodo_fin_str: Optional[str] = None,
    total_ventas_calculado: Optional[float] = None,
    id_sucursal: Optional[int] = None
):
    try:
        if not any([fecha_generacion_str, periodo_inicio_str, periodo_fin_str, 
                    total_ventas_calculado is not None, id_sucursal is not None]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_reporte_ventas FROM reporte_ventas WHERE id_reporte_ventas = :id_check", {"id_check": id_reporte_ventas_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de ventas con id {id_reporte_ventas_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_rv_bind": id_reporte_ventas_param}

                if fecha_generacion_str is not None:
                    try:
                        parsed_date = date.fromisoformat(fecha_generacion_str)
                        set_clauses.append("fecha_generacion = :p_fecha_generacion")
                        bind_vars["p_fecha_generacion"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_generacion inválido. Usar YYYY-MM-DD.")
                if periodo_inicio_str is not None:
                    try:
                        parsed_date = date.fromisoformat(periodo_inicio_str)
                        set_clauses.append("periodo_inicio = :p_periodo_inicio")
                        bind_vars["p_periodo_inicio"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_inicio inválido. Usar YYYY-MM-DD.")
                if periodo_fin_str is not None:
                    try:
                        parsed_date = date.fromisoformat(periodo_fin_str)
                        set_clauses.append("periodo_fin = :p_periodo_fin")
                        bind_vars["p_periodo_fin"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_fin inválido. Usar YYYY-MM-DD.")
                if total_ventas_calculado is not None:
                    set_clauses.append("total_ventas_calculado = :p_total_ventas")
                    bind_vars["p_total_ventas"] = total_ventas_calculado
                if id_sucursal is not None:
                    cursor.execute("SELECT id_sucursal FROM sucursal WHERE id_sucursal = :p_id_suc", {"p_id_suc": id_sucursal})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"La sucursal con id {id_sucursal} no existe.")
                    set_clauses.append("id_sucursal = :p_id_sucursal")
                    bind_vars["p_id_sucursal"] = id_sucursal
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE reporte_ventas SET {', '.join(set_clauses)} WHERE id_reporte_ventas = :p_id_rv_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Reporte de ventas actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: La sucursal especificada no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente reporte de ventas: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente reporte de ventas: {str(ex)}")
    
# CRUD Reporte Desempeño

@app.get("/reporte_desempenioget", tags=["Reporte Desempeño"])
def listar_reportes_desempenio():
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_reporte_desempenio, id_empleado, fecha_generacion, 
                           periodo_evaluacion_inicio, periodo_evaluacion_fin, datos_evaluacion
                    FROM reporte_desempenio ORDER BY fecha_generacion DESC, id_reporte_desempenio DESC
                """
                cursor.execute(sql)
                reportes = []
                if cursor.description:
                    column_names = [desc[0].lower() for desc in cursor.description]
                    for row_tuple in cursor:
                        row_as_dict = dict(zip(column_names, row_tuple))
                        if row_as_dict.get('fecha_generacion') and isinstance(row_as_dict['fecha_generacion'], date):
                            row_as_dict['fecha_generacion'] = row_as_dict['fecha_generacion'].isoformat()
                        if row_as_dict.get('periodo_evaluacion_inicio') and isinstance(row_as_dict['periodo_evaluacion_inicio'], date):
                            row_as_dict['periodo_evaluacion_inicio'] = row_as_dict['periodo_evaluacion_inicio'].isoformat()
                        if row_as_dict.get('periodo_evaluacion_fin') and isinstance(row_as_dict['periodo_evaluacion_fin'], date):
                            row_as_dict['periodo_evaluacion_fin'] = row_as_dict['periodo_evaluacion_fin'].isoformat()
                        reportes.append(row_as_dict)
                return reportes
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al listar reportes de desempeño: {str(e)}")

@app.get("/reporte_desempeniogetid/{id_reporte_desempenio_param}", tags=["Reporte Desempeño"])
def obtener_reporte_desempenio_por_id(id_reporte_desempenio_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                sql = """
                    SELECT id_reporte_desempenio, id_empleado, fecha_generacion, 
                           periodo_evaluacion_inicio, periodo_evaluacion_fin, datos_evaluacion
                    FROM reporte_desempenio WHERE id_reporte_desempenio = :id_rd_bind
                """
                cursor.execute(sql, {"id_rd_bind": id_reporte_desempenio_param})
                row = cursor.fetchone()
                if row:
                    return {
                        "id_reporte_desempenio": row[0], 
                        "id_empleado": row[1], 
                        "fecha_generacion": row[2].isoformat() if isinstance(row[2], date) else row[2],
                        "periodo_evaluacion_inicio": row[3].isoformat() if isinstance(row[3], date) else row[3],
                        "periodo_evaluacion_fin": row[4].isoformat() if isinstance(row[4], date) else row[4],
                        "datos_evaluacion": row[5]
                    }
                else:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de desempeño con id {id_reporte_desempenio_param} no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de servidor al obtener reporte de desempeño: {str(ex)}")

@app.post("/reporte_desempeniopost", tags=["Reporte Desempeño"])
def agregar_reporte_desempenio(
    id_reporte_desempenio: int,
    id_empleado: int,
    fecha_generacion_str: str, 
    periodo_evaluacion_inicio_str: str,
    periodo_evaluacion_fin_str: str,
    datos_evaluacion: str
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_reporte_desempenio FROM reporte_desempenio WHERE id_reporte_desempenio = :p_id_rd", {"p_id_rd": id_reporte_desempenio})
                if cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Ya existe un reporte de desempeño con el ID {id_reporte_desempenio}.")

                cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado} no existe.")
                
                try:
                    fecha_g = date.fromisoformat(fecha_generacion_str)
                    fecha_pei = date.fromisoformat(periodo_evaluacion_inicio_str)
                    fecha_pef = date.fromisoformat(periodo_evaluacion_fin_str)
                except ValueError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha inválido. Usar YYYY-MM-DD para todas las fechas.")

                sql = """
                    INSERT INTO reporte_desempenio (
                        id_reporte_desempenio, id_empleado, fecha_generacion, 
                        periodo_evaluacion_inicio, periodo_evaluacion_fin, datos_evaluacion
                    ) VALUES (
                        :id_reporte_desempenio, :id_empleado, :fecha_generacion, 
                        :periodo_evaluacion_inicio, :periodo_evaluacion_fin, :datos_evaluacion
                    )
                """
                cursor.execute(sql, {
                    "id_reporte_desempenio": id_reporte_desempenio, "id_empleado": id_empleado,
                    "fecha_generacion": fecha_g, "periodo_evaluacion_inicio": fecha_pei,
                    "periodo_evaluacion_fin": fecha_pef, "datos_evaluacion": datos_evaluacion
                })
                cone.commit()
                return {"Mensaje": "Reporte de desempeño creado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 1: 
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Error de BD: Violación de restricción única (ID de reporte ya existe).")
        elif error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El empleado especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al crear reporte de desempeño: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al crear reporte de desempeño: {str(ex)}")

@app.put("/reporte_desempenioputid/{id_reporte_desempenio_param}", tags=["Reporte Desempeño"])
def actualizar_reporte_desempenio(
    id_reporte_desempenio_param: int,
    id_empleado: int,
    fecha_generacion_str: str,
    periodo_evaluacion_inicio_str: str,
    periodo_evaluacion_fin_str: str,
    datos_evaluacion: str
):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_reporte_desempenio FROM reporte_desempenio WHERE id_reporte_desempenio = :id_check", {"id_check": id_reporte_desempenio_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de desempeño con id {id_reporte_desempenio_param} no encontrado para actualizar.")

                cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado} no existe.")
                
                try:
                    fecha_g = date.fromisoformat(fecha_generacion_str)
                    fecha_pei = date.fromisoformat(periodo_evaluacion_inicio_str)
                    fecha_pef = date.fromisoformat(periodo_evaluacion_fin_str)
                except ValueError:
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha inválido. Usar YYYY-MM-DD para todas las fechas.")

                sql = """
                    UPDATE reporte_desempenio SET
                        id_empleado = :id_empleado,
                        fecha_generacion = :fecha_generacion,
                        periodo_evaluacion_inicio = :periodo_evaluacion_inicio,
                        periodo_evaluacion_fin = :periodo_evaluacion_fin,
                        datos_evaluacion = :datos_evaluacion
                    WHERE id_reporte_desempenio = :id_rd_bind
                """
                cursor.execute(sql, {
                    "id_empleado": id_empleado, "fecha_generacion": fecha_g,
                    "periodo_evaluacion_inicio": fecha_pei, "periodo_evaluacion_fin": fecha_pef,
                    "datos_evaluacion": datos_evaluacion, "id_rd_bind": id_reporte_desempenio_param
                })
                
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reporte de desempeño no encontrado o ningún dato fue diferente para actualizar.")
                cone.commit()
                return {"Mensaje": "Reporte de desempeño actualizado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El empleado especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar reporte de desempeño: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar reporte de desempeño: {str(ex)}")

@app.delete("/reporte_desempeniodelete/{id_reporte_desempenio_param}", tags=["Reporte Desempeño"])
def eliminar_reporte_desempenio(id_reporte_desempenio_param: int):
    try:
        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("DELETE FROM reporte_desempenio WHERE id_reporte_desempenio = :id_rd_bind", {"id_rd_bind": id_reporte_desempenio_param})
                if cursor.rowcount == 0:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de desempeño con id {id_reporte_desempenio_param} no encontrado")
                cone.commit()
                return {"Mensaje": "Reporte de desempeño eliminado con éxito"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al eliminar reporte de desempeño: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al eliminar reporte de desempeño: {str(ex)}")

@app.patch("/reporte_desempeniopatch/{id_reporte_desempenio_param}", tags=["Reporte Desempeño"])
def actualizar_parcial_reporte_desempenio(
    id_reporte_desempenio_param: int,
    id_empleado: Optional[int] = None,
    fecha_generacion_str: Optional[str] = None,
    periodo_evaluacion_inicio_str: Optional[str] = None,
    periodo_evaluacion_fin_str: Optional[str] = None,
    datos_evaluacion: Optional[str] = None
):
    try:
        if not any([id_empleado is not None, fecha_generacion_str, periodo_evaluacion_inicio_str, 
                    periodo_evaluacion_fin_str, datos_evaluacion]):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Debe enviar al menos un dato para actualizar.")

        with get_conexion() as cone:
            with cone.cursor() as cursor:
                cursor.execute("SELECT id_reporte_desempenio FROM reporte_desempenio WHERE id_reporte_desempenio = :id_check", {"id_check": id_reporte_desempenio_param})
                if not cursor.fetchone():
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Reporte de desempeño con id {id_reporte_desempenio_param} no encontrado para actualizar.")

                set_clauses = []
                bind_vars = {"p_id_rd_bind": id_reporte_desempenio_param}

                if id_empleado is not None:
                    cursor.execute("SELECT id_empleado FROM empleado WHERE id_empleado = :p_id_emp", {"p_id_emp": id_empleado})
                    if not cursor.fetchone(): raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El empleado con id {id_empleado} no existe.")
                    set_clauses.append("id_empleado = :p_id_empleado")
                    bind_vars["p_id_empleado"] = id_empleado
                
                if fecha_generacion_str is not None:
                    try:
                        parsed_date = date.fromisoformat(fecha_generacion_str)
                        set_clauses.append("fecha_generacion = :p_fecha_generacion")
                        bind_vars["p_fecha_generacion"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de fecha_generacion inválido. Usar YYYY-MM-DD.")
                if periodo_evaluacion_inicio_str is not None:
                    try:
                        parsed_date = date.fromisoformat(periodo_evaluacion_inicio_str)
                        set_clauses.append("periodo_evaluacion_inicio = :p_periodo_evaluacion_inicio")
                        bind_vars["p_periodo_evaluacion_inicio"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_evaluacion_inicio inválido. Usar YYYY-MM-DD.")
                if periodo_evaluacion_fin_str is not None:
                    try:
                        parsed_date = date.fromisoformat(periodo_evaluacion_fin_str)
                        set_clauses.append("periodo_evaluacion_fin = :p_periodo_evaluacion_fin")
                        bind_vars["p_periodo_evaluacion_fin"] = parsed_date
                    except ValueError:
                        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de periodo_evaluacion_fin inválido. Usar YYYY-MM-DD.")
                if datos_evaluacion is not None:
                    set_clauses.append("datos_evaluacion = :p_datos_evaluacion")
                    bind_vars["p_datos_evaluacion"] = datos_evaluacion
                
                if not set_clauses:
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Ningún campo válido proporcionado para la actualización.")

                sql_update = f"UPDATE reporte_desempenio SET {', '.join(set_clauses)} WHERE id_reporte_desempenio = :p_id_rd_bind"
                cursor.execute(sql_update, bind_vars)
                
                cone.commit()
                return {"Mensaje": "Reporte de desempeño actualizado parcialmente"}
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        if error_obj.code == 2291: 
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Error de BD: El empleado especificado no existe.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error de BD al actualizar parcialmente reporte de desempeño: {error_obj.message.strip()}")
    except Exception as ex:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error inesperado al actualizar parcialmente reporte de desempeño: {str(ex)}")