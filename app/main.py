from fastapi import FastAPI, HTTPException, Header
import oracledb
from typing import Optional

app = FastAPI()

#Conexión con OracleDB
def get_conexion():
    conexion = oracledb.connect(
        user="prueba_api",
        password="prueba_api",
        dsn="localhost:1521/orcl"
    )
    return conexion

# CRUD Ciudad

@app.get("/ciudadget")
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

@app.get("/ciudadgetid/{id_ciudad}")
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

@app.post("/ciudadpost")
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

@app.put("/ciudadputid/{id_ciudad}")
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

@app.delete("/ciudaddelete/{id_ciudad}")
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

@app.patch("/ciudadpatch/{id_ciudad}")
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

# CRUD Sucursal

@app.get("/sucursalget")
def listar_sucursal():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_sucursal, nombre_sucursal, direccion, id_ciudad FROM sucursal")
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
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/sucursalgetid/{id_sucursal}")
def obtener_sucursal(id_sucursal: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_sucursal, nombre_sucursal, direccion, id_ciudad FROM sucursal WHERE id_sucursal = :id_sucursal", {"id_sucursal": id_sucursal})
        sucursal = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if sucursal:
            return {"id_sucursal": sucursal[0], "nombre_sucursal": sucursal[1], "direccion": sucursal[2], "id_ciudad": sucursal[3]}
        else:
            raise HTTPException(status_code=404, detail="Sucursal no encontrada")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.post("/sucursalpost")
def agregar_sucursal(nombre_sucursal: str, id_ciudad: int, direccion: Optional[str] = None):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
                INSERT INTO sucursal
                VALUES(:nombre_sucursal, :direccion, :id_ciudad)
        """, {"nombre_sucursal": nombre_sucursal, "direccion": direccion, "id_ciudad": id_ciudad})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal creada con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.put("/sucursalput/{id_sucursal}")
def actualizar_sucursal(id_sucursal: int, nombre_sucursal: str, id_ciudad: int, direccion: Optional[str] = None):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
                UPDATE sucursal
                SET nombre_sucursal = :nombre_sucursal, direccion = :direccion, id_ciudad = :id_ciudad
                WHERE id_sucursal = :id_sucursal
        """, {"id_sucursal": id_sucursal, "nombre_sucursal": nombre_sucursal, "direccion": direccion, "id_ciudad": id_ciudad})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Sucursal no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal actualizada con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.delete("/sucursaldelete/{id_sucursal}")
def eliminar_sucursal(id_sucursal: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM sucursal WHERE id_sucursal = :id_sucursal", {"id_sucursal": id_sucursal})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Sucursal no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal eliminada con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.patch("/sucursalpatch/{id_sucursal}")
def actualizar_parcial_sucursal(id_sucursal: int, nombre_sucursal: Optional[str] = None, direccion: Optional[str] = None, id_ciudad: Optional[int] = None):
    try:
        if not nombre_sucursal and not direccion and not id_ciudad:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        
        cone = get_conexion()
        cursor = cone.cursor()
        
        campos = []
        valores = {"id_sucursal": id_sucursal}
        if nombre_sucursal:
            campos.append("nombre_sucursal = :nombre_sucursal")
            valores["nombre_sucursal"] = nombre_sucursal
        if direccion:
            campos.append("direccion = :direccion")
            valores["direccion"] = direccion
        if id_ciudad:
            campos.append("id_ciudad = :id_ciudad")
            valores["id_ciudad"] = id_ciudad
            
        query = f"UPDATE sucursal SET {','.join(campos)} WHERE id_sucursal = :id_sucursal"
        cursor.execute(query, valores)
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Sucursal no encontrada")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Sucursal actualizada parcialmente"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

# CRUD empleado

@app.get("/empleadoget")
def listar_empleado():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_empleado, p_nombre, s_nombre, p_apellido, s_apellido, salario FROM empleado")
        empleados = []
        for id_empleado, p_nombre, s_nombre, p_apellido, s_apellido, salario in cursor:
            empleados.append({
                "id_empleado": id_empleado,
                "p_nombre": p_nombre,
                "s_nombre": s_nombre,
                "p_apellido": p_apellido,
                "s_apellido": s_apellido,
                "salario": salario
            })
        cursor.close()
        cone.close()
        return empleados
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/empleadogetid/{id_empleado}")
def obtener_empleado(id_empleado: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_empleado, p_nombre, s_nombre, p_apellido, s_apellido, salario FROM empleado WHERE id_empleado = :id_empleado", {"id_empleado": id_empleado})
        empleado = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if empleado:
            return {
                "id_empleado": empleado[0],
                "p_nombre": empleado[1],
                "s_nombre": empleado[2],
                "p_apellido": empleado[3],
                "s_apellido": empleado[4],
                "salario": empleado[5]
            }
        else:
            raise HTTPException(status_code=404, detail="Empleado no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.post("/empleadopost")
def agregar_empleado(id_empleado: int, p_nombre: str, s_nombre: Optional[str], p_apellido: str, s_apellido: Optional[str], salario: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO empleado
            VALUES(:id_empleado, :p_nombre, :s_nombre, :p_apellido, :s_apellido, :salario)
        """, {
            "id_empleado": id_empleado,
            "p_nombre": p_nombre,
            "s_nombre": s_nombre,
            "p_apellido": p_apellido,
            "s_apellido": s_apellido,
            "salario": salario
        })
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Empleado creado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.put("/empleadoput/{id_empleado}")
def actualizar_empleado(id_empleado: int, p_nombre: str, s_nombre: Optional[str], p_apellido: str, s_apellido: Optional[str], salario: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE empleado
            SET p_nombre = :p_nombre, s_nombre = :s_nombre, p_apellido = :p_apellido, s_apellido = :s_apellido, salario = :salario
            WHERE id_empleado = :id_empleado
        """, {
            "id_empleado": id_empleado,
            "p_nombre": p_nombre,
            "s_nombre": s_nombre,
            "p_apellido": p_apellido,
            "s_apellido": s_apellido,
            "salario": salario
        })
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Empleado no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Empleado actualizado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.delete("/empleadodelete/{id_empleado}")
def eliminar_empleado(id_empleado: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM empleado WHERE id_empleado = :id_empleado", {"id_empleado": id_empleado})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Empleado no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Empleado eliminado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.patch("/empleadopatch/{id_empleado}")
def actualizar_parcial_empleado(id_empleado: int, p_nombre: Optional[str], s_nombre: Optional[str], p_apellido: Optional[str], s_apellido: Optional[str], salario: Optional[float]):
    try:
        if not p_nombre and not s_nombre and not p_apellido and not s_apellido and not salario:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        
        cone = get_conexion()
        cursor = cone.cursor()
        
        campos = []
        valores = {"id_empleado": id_empleado}
        if p_nombre:
            campos.append("p_nombre = :p_nombre")
            valores["p_nombre"] = p_nombre
        if s_nombre:
            campos.append("s_nombre = :s_nombre")
            valores["s_nombre"] = s_nombre
        if p_apellido:
            campos.append("p_apellido = :p_apellido")
            valores["p_apellido"] = p_apellido
        if s_apellido:
            campos.append("s_apellido = :s_apellido")
            valores["s_apellido"] = s_apellido
        if salario is not None:
            campos.append("salario = :salario")
            valores["salario"] = salario
            
        query = f"UPDATE empleado SET {','.join(campos)} WHERE id_empleado = :id_empleado"
        cursor.execute(query, valores)
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Empleado no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Empleado actualizado parcialmente"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

# CRUD Cargo

@app.get("/cargosget")
def listar_cargos():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo, descripcion FROM cargo")
        cargos = []
        for id_cargo, descripcion in cursor:
            cargos.append({"id_cargo": id_cargo, "descripcion": descripcion})
        cursor.close()
        cone.close()
        return cargos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/cargosgetid/{id_cargo}")
def obtener_cargo(id_cargo: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cargo, descripcion FROM cargo WHERE id_cargo = :id_cargo", {"id_cargo": id_cargo})
        cargo = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if cargo:
            return {"id_cargo": cargo[0], "descripcion": cargo[1]}
        else:
            raise HTTPException(status_code=404, detail="Cargo no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.post("/cargospost")
def agregar_cargo(id_cargo: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("INSERT INTO cargo VALUES(:id_cargo, :descripcion)", {"id_cargo": id_cargo, "descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cargo creado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.put("/cargosput/{id_cargo}")
def actualizar_cargo(id_cargo: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("UPDATE cargo SET descripcion = :descripcion WHERE id_cargo = :id_cargo", {"id_cargo": id_cargo, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cargo no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cargo actualizado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.delete("/cargosdelete/{id_cargo}")
def eliminar_cargo(id_cargo: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM cargo WHERE id_cargo = :id_cargo", {"id_cargo": id_cargo})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cargo no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cargo eliminado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.patch("/cargospatch/{id_cargo}")
def actualizar_parcial_cargo(id_cargo: int, descripcion: Optional[str]):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        
        cone = get_conexion()
        cursor = cone.cursor()
        
        campos = []
        valores = {"id_cargo": id_cargo}
        if descripcion:
            campos.append("descripcion = :descripcion")
            valores["descripcion"] = descripcion
            
        query = f"UPDATE cargo SET {','.join(campos)} WHERE id_cargo = :id_cargo"
        cursor.execute(query, valores)
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cargo no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cargo actualizado parcialmente"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

# CRUD Clientes

@app.get("/clientesget")
def listar_clientes():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cliente, p_nombre || ' ' || p_apellido AS nombre_cliente FROM cliente")
        clientes = []
        for id_cliente, nombre_cliente in cursor:
            clientes.append({"id_cliente": id_cliente, "nombre_cliente": nombre_cliente})
        cursor.close()
        cone.close()
        return clientes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/clientesgetid/{id_cliente}")
def obtener_cliente(id_cliente: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_cliente, p_nombre || ' ' || p_apellido AS nombre_cliente, rut, correo, telefono FROM cliente WHERE id_cliente = :id_cliente", {"id_cliente": id_cliente})
        cliente = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if cliente:
            return {"id_cliente": cliente[0], "nombre_cliente": cliente[1], "rut": cliente[2], "correo": cliente[3], "telefono": cliente[4]}
        else:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.post("/clientespost")
def agregar_cliente(id_cliente: int, rut: str, p_nombre: str, s_nombre: Optional[str], p_apellido: str, s_apellido: Optional[str], correo: Optional[str], telefono: Optional[str]):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("INSERT INTO cliente VALUES (:id_cliente, :rut, :p_nombre, :s_nombre, :p_apellido, :s_apellido, :correo, :telefono)", 
                       {"id_cliente": id_cliente, "rut": rut, "p_nombre": p_nombre, "s_nombre": s_nombre, "p_apellido": p_apellido, "s_apellido": s_apellido, "correo": correo, "telefono": telefono})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cliente creado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.put("/clientesput/{id_cliente}")
def actualizar_cliente(id_cliente: int, rut: str, p_nombre: str, s_nombre: Optional[str], p_apellido: str, s_apellido: Optional[str], correo: Optional[str], telefono: Optional[str]):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE cliente 
            SET rut = :rut, p_nombre = :p_nombre, s_nombre = :s_nombre, p_apellido = :p_apellido, s_apellido = :s_apellido, correo = :correo, telefono = :telefono
            WHERE id_cliente = :id_cliente
        """, {"id_cliente": id_cliente, "rut": rut, "p_nombre": p_nombre, "s_nombre": s_nombre, "p_apellido": p_apellido, "s_apellido": s_apellido, "correo": correo, "telefono": telefono})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cliente actualizado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.delete("/clientesdelete/{id_cliente}")
def eliminar_cliente(id_cliente: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM cliente WHERE id_cliente = :id_cliente", {"id_cliente": id_cliente})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cliente eliminado con éxito"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

@app.patch("/clientespatch/{id_cliente}")
def actualizar_parcial_cliente(id_cliente: int, rut: Optional[str], p_nombre: Optional[str], s_nombre: Optional[str], p_apellido: Optional[str], s_apellido: Optional[str], correo: Optional[str], telefono: Optional[str]):
    try:
        if not (rut or p_nombre or s_nombre or p_apellido or s_apellido or correo or telefono):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        
        cone = get_conexion()
        cursor = cone.cursor()
        
        campos = []
        valores = {"id_cliente": id_cliente}
        if rut:
            campos.append("rut = :rut")
            valores["rut"] = rut
        if p_nombre:
            campos.append("p_nombre = :p_nombre")
            valores["p_nombre"] = p_nombre
        if s_nombre:
            campos.append("s_nombre = :s_nombre")
            valores["s_nombre"] = s_nombre
        if p_apellido:
            campos.append("p_apellido = :p_apellido")
            valores["p_apellido"] = p_apellido
        if s_apellido:
            campos.append("s_apellido = :s_apellido")
            valores["s_apellido"] = s_apellido
        if correo:
            campos.append("correo = :correo")
            valores["correo"] = correo
        if telefono:
            campos.append("telefono = :telefono")
            valores["telefono"] = telefono
            
        query = f"UPDATE cliente SET {','.join(campos)} WHERE id_cliente = :id_cliente"
        cursor.execute(query, valores)
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje: ": "Cliente actualizado parcialmente"}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))

# CRUD categoria

@app.get("/categoriasget")
def obtener_categorias():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_categoria, descripcion FROM categoria")
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
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/categoriasgetid/{id_categoria}")
def obtener_categoria(id_categoria: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_categoria, descripcion FROM categoria WHERE id_categoria = :id_categoria", {"id_categoria": id_categoria})
        categoria = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if categoria:
            return {"id_categoria": categoria[0], "descripcion": categoria[1]}
        else:
            raise HTTPException(status_code=404, detail="Categoría no encontrada")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/categoriaspost")
def agregar_categoria(descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO categoria
            VALUES (:descripcion)
        """, {"descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría creada con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/categoriasput/{id_categoria}")
def actualizar_categoria(id_categoria: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE categoria
            SET descripcion = :descripcion
            WHERE id_categoria = :id_categoria
        """, {"id_categoria": id_categoria, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Categoría no encontrada")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría actualizada con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/categoriasdelete/{id_categoria}")
def eliminar_categoria(id_categoria: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM categoria WHERE id_categoria = :id_categoria", {"id_categoria": id_categoria})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Categoría no encontrada")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría eliminada con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/categorias/{id_categoria}")
def actualizar_parcial_categoria(id_categoria: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("UPDATE categoria SET descripcion = :descripcion WHERE id_categoria = :id_categoria",
                       {"id_categoria": id_categoria, "descripcion": descripcion})
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Categoría no encontrada")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Categoría actualizada parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD productos

@app.get("/productosget")
def obtener_productos():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_producto, nombre, precio FROM productos")
        productos = []
        for id_producto, nombre, precio in cursor:
            productos.append({
                "id_producto": id_producto,
                "nombre": nombre,
                "precio": precio
            })
        cursor.close()
        cone.close()
        return productos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/productosgetid/{id_producto}")
def obtener_producto(id_producto: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_producto, nombre, precio FROM productos WHERE id_producto = :id_producto", {"id_producto": id_producto})
        producto = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if producto:
            return {"id_producto": producto[0], "nombre": producto[1], "precio": producto[2]}
        else:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/productospost")
def agregar_producto(nombre: str, precio: float, stock: int, id_categoria: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO productos
            VALUES (:nombre, :precio, :stock, :id_categoria)
        """, {"nombre": nombre, "precio": precio, "stock": stock, "id_categoria": id_categoria})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Producto creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/productosput/{id_producto}")
def actualizar_producto(id_producto: int, nombre: str, precio: float, stock: int, id_categoria: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE productos
            SET nombre = :nombre, precio = :precio, stock = :stock, id_categoria = :id_categoria
            WHERE id_producto = :id_producto
        """, {"id_producto": id_producto, "nombre": nombre, "precio": precio, "stock": stock, "id_categoria": id_categoria})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Producto actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/productosdelete/{id_producto}")
def eliminar_producto(id_producto: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM productos WHERE id_producto = :id_producto", {"id_producto": id_producto})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Producto eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/productos/{id_producto}")
def actualizar_parcial_producto(id_producto: int, nombre: Optional[str] = None, precio: Optional[float] = None,
                                 stock: Optional[int] = None, id_categoria: Optional[int] = None):
    try:
        if not any([nombre, precio, stock, id_categoria]):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        campos = []
        valores = {"id_producto": id_producto}
        if nombre:
            campos.append("nombre = :nombre")
            valores["nombre"] = nombre
        if precio:
            campos.append("precio = :precio")
            valores["precio"] = precio
        if stock:
            campos.append("stock = :stock")
            valores["stock"] = stock
        if id_categoria:
            campos.append("id_categoria = :id_categoria")
            valores["id_categoria"] = id_categoria
        query = f"UPDATE productos SET {', '.join(campos)} WHERE id_producto = :id_producto"
        cursor.execute(query, valores)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Producto actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD inventario

@app.get("/inventariosget")
def obtener_inventarios():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_inventario, fecha_actualizacion, id_sucursal FROM inventario")
        inventarios = []
        for id_inventario, fecha_actualizacion, id_sucursal in cursor:
            inventarios.append({
                "id_inventario": id_inventario,
                "fecha_actualizacion": fecha_actualizacion,
                "id_sucursal": id_sucursal
            })
        cursor.close()
        cone.close()
        return inventarios
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/inventariosgetid/{id_inventario}")
def obtener_inventario(id_inventario: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_inventario, fecha_actualizacion, id_sucursal FROM inventario WHERE id_inventario = :id_inventario", {"id_inventario": id_inventario})
        inventario = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if inventario:
            return {"id_inventario": inventario[0], "fecha_actualizacion": inventario[1], "id_sucursal": inventario[2]}
        else:
            raise HTTPException(status_code=404, detail="Inventario no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/inventariospost")
def agregar_inventario(fecha_actualizacion: str, id_sucursal: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO inventario
            VALUES (:fecha_actualizacion, :id_sucursal)
        """, {"fecha_actualizacion": fecha_actualizacion, "id_sucursal": id_sucursal})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Inventario creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/inventariosput/{id_inventario}")
def actualizar_inventario(id_inventario: int, fecha_actualizacion: str, id_sucursal: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE inventario
            SET fecha_actualizacion = :fecha_actualizacion, id_sucursal = :id_sucursal
            WHERE id_inventario = :id_inventario
        """, {"id_inventario": id_inventario, "fecha_actualizacion": fecha_actualizacion, "id_sucursal": id_sucursal})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Inventario no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Inventario actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/inventariosdelete/{id_inventario}")
def eliminar_inventario(id_inventario: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM inventario WHERE id_inventario = :id_inventario", {"id_inventario": id_inventario})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Inventario no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Inventario eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/inventarios/{id_inventario}")
def actualizar_parcial_inventario(id_inventario: int, fecha_actualizacion: Optional[str] = None,
                                  id_sucursal: Optional[int] = None):
    try:
        if not any([fecha_actualizacion, id_sucursal]):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        campos = []
        valores = {"id_inventario": id_inventario}
        if fecha_actualizacion:
            campos.append("fecha_actualizacion = :fecha_actualizacion")
            valores["fecha_actualizacion"] = fecha_actualizacion
        if id_sucursal:
            campos.append("id_sucursal = :id_sucursal")
            valores["id_sucursal"] = id_sucursal
        query = f"UPDATE inventario SET {', '.join(campos)} WHERE id_inventario = :id_inventario"
        cursor.execute(query, valores)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Inventario no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Inventario actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD pedido

@app.get("/pedidosget")
def obtener_pedidos():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_pedido, fecha_pedido, id_cliente, id_empleado FROM pedido")
        pedidos = []
        for id_pedido, fecha_pedido, id_cliente, id_empleado in cursor:
            pedidos.append({
                "id_pedido": id_pedido,
                "fecha_pedido": fecha_pedido,
                "id_cliente": id_cliente,
                "id_empleado": id_empleado
            })
        cursor.close()
        cone.close()
        return pedidos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pedidosgetid/{id_pedido}")
def obtener_pedido(id_pedido: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_pedido, fecha_pedido, id_cliente, id_empleado FROM pedido WHERE id_pedido = :id_pedido", {"id_pedido": id_pedido})
        pedido = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if pedido:
            return {"id_pedido": pedido[0], "fecha_pedido": pedido[1], "id_cliente": pedido[2], "id_empleado": pedido[3]}
        else:
            raise HTTPException(status_code=404, detail="Pedido no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pedidospost")
def agregar_pedido(cliente_id: int, fecha_pedido: str, id_empleado: int, total: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO pedido
            VALUES (:fecha_pedido, :cliente_id, :id_empleado, :total)
        """, {"fecha_pedido": fecha_pedido, "cliente_id": cliente_id, "id_empleado": id_empleado, "total": total})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Pedido creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/pedidosput/{id_pedido}")
def actualizar_pedido(id_pedido: int, cliente_id: int, fecha_pedido: str, id_empleado: int, total: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE pedido
            SET fecha_pedido = :fecha_pedido, id_cliente = :cliente_id, id_empleado = :id_empleado, total = :total
            WHERE id_pedido = :id_pedido
        """, {"id_pedido": id_pedido, "fecha_pedido": fecha_pedido, "cliente_id": cliente_id, "id_empleado": id_empleado, "total": total})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Pedido no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Pedido actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/pedidosdelete/{id_pedido}")
def eliminar_pedido(id_pedido: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM pedido WHERE id_pedido = :id_pedido", {"id_pedido": id_pedido})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Pedido no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Pedido eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/pedidos/{id_pedido}")
def actualizar_parcial_pedido(id_pedido: int, fecha_pedido: Optional[str] = None,
                               id_cliente: Optional[int] = None, id_empleado: Optional[int] = None):
    try:
        if not any([fecha_pedido, id_cliente, id_empleado]):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        campos = []
        valores = {"id_pedido": id_pedido}
        if fecha_pedido:
            campos.append("fecha_pedido = :fecha_pedido")
            valores["fecha_pedido"] = fecha_pedido
        if id_cliente:
            campos.append("id_cliente = :id_cliente")
            valores["id_cliente"] = id_cliente
        if id_empleado:
            campos.append("id_empleado = :id_empleado")
            valores["id_empleado"] = id_empleado
        query = f"UPDATE pedido SET {', '.join(campos)} WHERE id_pedido = :id_pedido"
        cursor.execute(query, valores)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Pedido no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Pedido actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD estado_pedido

@app.get("/estados_pedidoget")
def obtener_estados_pedido():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_estado_pedido, descripcion FROM estado_pedido")
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
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/estados_pedidogetid/{id_estado_pedido}")
def obtener_estado_pedido(id_estado_pedido: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_estado_pedido, descripcion FROM estado_pedido WHERE id_estado_pedido = :id_estado_pedido", {"id_estado_pedido": id_estado_pedido})
        estado_pedido = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if estado_pedido:
            return {"id_estado_pedido": estado_pedido[0], "descripcion": estado_pedido[1]}
        else:
            raise HTTPException(status_code=404, detail="Estado de pedido no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/estados_pedidopost")
def agregar_estado_pedido(descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO estado_pedido
            VALUES (:descripcion)
        """, {"descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/estados_pedidoput/{id_estado_pedido}")
def actualizar_estado_pedido(id_estado_pedido: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE estado_pedido
            SET descripcion = :descripcion
            WHERE id_estado_pedido = :id_estado_pedido
        """, {"id_estado_pedido": id_estado_pedido, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Estado de pedido no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/estados_pedidodelete/{id_estado_pedido}")
def eliminar_estado_pedido(id_estado_pedido: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM estado_pedido WHERE id_estado_pedido = :id_estado_pedido", {"id_estado_pedido": id_estado_pedido})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Estado de pedido no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/estados_pedido/{id_estado_pedido}")
def actualizar_parcial_estado_pedido(id_estado_pedido: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("UPDATE estado_pedido SET descripcion = :descripcion WHERE id_estado_pedido = :id_estado_pedido",
                       {"id_estado_pedido": id_estado_pedido, "descripcion": descripcion})
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Estado de pedido no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Estado de pedido actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD detalle_pedido

@app.get("/detalle_pedidosget")
def obtener_detalle_pedidos():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_detalle_pedido, id_pedido, id_producto, cantidad, subtotal FROM detalle_pedido")
        detalle_pedidos = []
        for id_detalle_pedido, id_pedido, id_producto, cantidad, subtotal in cursor:
            detalle_pedidos.append({
                "id_detalle_pedido": id_detalle_pedido,
                "id_pedido": id_pedido,
                "id_producto": id_producto,
                "cantidad": cantidad,
                "subtotal": subtotal
            })
        cursor.close()
        cone.close()
        return detalle_pedidos
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/detalle_pedidosgetid/{id_detalle_pedido}")
def obtener_detalle_pedido(id_detalle_pedido: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_detalle_pedido, id_pedido, id_producto, cantidad, subtotal FROM detalle_pedido WHERE id_detalle_pedido = :id_detalle_pedido", {"id_detalle_pedido": id_detalle_pedido})
        detalle_pedido = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if detalle_pedido:
            return {
                "id_detalle_pedido": detalle_pedido[0],
                "id_pedido": detalle_pedido[1],
                "id_producto": detalle_pedido[2],
                "cantidad": detalle_pedido[3],
                "subtotal": detalle_pedido[4]
            }
        else:
            raise HTTPException(status_code=404, detail="Detalle de pedido no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/detalle_pedidospost")
def agregar_detalle_pedido(id_pedido: int, id_producto: int, cantidad: int, subtotal: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO detalle_pedido
            VALUES (:id_pedido, :id_producto, :cantidad, :subtotal)
        """, {"id_pedido": id_pedido, "id_producto": id_producto, "cantidad": cantidad, "subtotal": subtotal})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Detalle de pedido creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/detalle_pedidosput/{id_detalle_pedido}")
def actualizar_detalle_pedido(id_detalle_pedido: int, id_pedido: int, id_producto: int, cantidad: int, subtotal: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE detalle_pedido
            SET id_pedido = :id_pedido, id_producto = :id_producto, cantidad = :cantidad, subtotal = :subtotal
            WHERE id_detalle_pedido = :id_detalle_pedido
        """, {"id_detalle_pedido": id_detalle_pedido, "id_pedido": id_pedido, "id_producto": id_producto, "cantidad": cantidad, "subtotal": subtotal})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Detalle de pedido no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Detalle de pedido actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/detalle_pedidosdelete/{id_detalle_pedido}")
def eliminar_detalle_pedido(id_detalle_pedido: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM detalle_pedido WHERE id_detalle_pedido = :id_detalle_pedido", {"id_detalle_pedido": id_detalle_pedido})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Detalle de pedido no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Detalle de pedido eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/detalle_pedidos/{id_detalle_pedido}")
def actualizar_parcial_detalle_pedido(id_detalle_pedido: int, id_pedido: Optional[int] = None,
                                      id_producto: Optional[int] = None, cantidad: Optional[int] = None,
                                      subtotal: Optional[float] = None):
    try:
        if not any([id_pedido, id_producto, cantidad, subtotal]):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        campos = []
        valores = {"id_detalle_pedido": id_detalle_pedido}
        if id_pedido:
            campos.append("id_pedido = :id_pedido")
            valores["id_pedido"] = id_pedido
        if id_producto:
            campos.append("id_producto = :id_producto")
            valores["id_producto"] = id_producto
        if cantidad:
            campos.append("cantidad = :cantidad")
            valores["cantidad"] = cantidad
        if subtotal:
            campos.append("subtotal = :subtotal")
            valores["subtotal"] = subtotal
        query = f"UPDATE detalle_pedido SET {', '.join(campos)} WHERE id_detalle_pedido = :id_detalle_pedido"
        cursor.execute(query, valores)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Detalle de pedido no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Detalle de pedido actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD factura

@app.get("/facturasget")
def obtener_facturas():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_factura, fecha_emision, total FROM factura")
        facturas = []
        for id_factura, fecha_emision, total in cursor:
            facturas.append({
                "id_factura": id_factura,
                "fecha_emision": fecha_emision,
                "total": total
            })
        cursor.close()
        cone.close()
        return facturas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/facturasgetid/{id_factura}")
def obtener_factura(id_factura: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_factura, fecha_emision, total FROM factura WHERE id_factura = :id_factura", {"id_factura": id_factura})
        factura = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if factura:
            return {
                "id_factura": factura[0],
                "fecha_emision": factura[1],
                "total": factura[2]
            }
        else:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/facturaspost")
def agregar_factura(id_pedido: int, fecha_emision: str, total: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO factura
            VALUES (:id_pedido, :fecha_emision, :total)
        """, {"id_pedido": id_pedido, "fecha_emision": fecha_emision, "total": total})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Factura creada con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/facturasput/{id_factura}")
def actualizar_factura(id_factura: int, id_pedido: int, fecha_emision: str, total: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE factura
            SET id_pedido = :id_pedido, fecha_emision = :fecha_emision, total = :total
            WHERE id_factura = :id_factura
        """, {"id_factura": id_factura, "id_pedido": id_pedido, "fecha_emision": fecha_emision, "total": total})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Factura actualizada con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/facturasdelete/{id_factura}")
def eliminar_factura(id_factura: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM factura WHERE id_factura = :id_factura", {"id_factura": id_factura})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Factura eliminada con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD tipo_transaccion

@app.get("/tipos_transaccionget")
def obtener_tipos_transaccion():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_tipo_transaccion, descripcion FROM tipo_transaccion")
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
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tipos_transacciongetid/{id_tipo_transaccion}")
def obtener_tipo_transaccion(id_tipo_transaccion: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_tipo_transaccion, descripcion FROM tipo_transaccion WHERE id_tipo_transaccion = :id_tipo_transaccion", {"id_tipo_transaccion": id_tipo_transaccion})
        tipo_transaccion = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if tipo_transaccion:
            return {
                "id_tipo_transaccion": tipo_transaccion[0],
                "descripcion": tipo_transaccion[1]
            }
        else:
            raise HTTPException(status_code=404, detail="Tipo de transacción no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tipos_transaccionpost")
def agregar_tipo_transaccion(descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO tipo_transaccion
            VALUES (:descripcion)
        """, {"descripcion": descripcion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/tipos_transaccionput/{id_tipo_transaccion}")
def actualizar_tipo_transaccion(id_tipo_transaccion: int, descripcion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE tipo_transaccion
            SET descripcion = :descripcion
            WHERE id_tipo_transaccion = :id_tipo_transaccion
        """, {"id_tipo_transaccion": id_tipo_transaccion, "descripcion": descripcion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Tipo de transacción no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/tipos_transacciondelete/{id_tipo_transaccion}")
def eliminar_tipo_transaccion(id_tipo_transaccion: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM tipo_transaccion WHERE id_tipo_transaccion = :id_tipo_transaccion", {"id_tipo_transaccion": id_tipo_transaccion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Tipo de transacción no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/tipos_transaccion/{id_tipo_transaccion}")
def actualizar_parcial_tipo_transaccion(id_tipo_transaccion: int, descripcion: Optional[str] = None):
    try:
        if not descripcion:
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("UPDATE tipo_transaccion SET descripcion = :descripcion WHERE id_tipo_transaccion = :id_tipo_transaccion",
                       {"id_tipo_transaccion": id_tipo_transaccion, "descripcion": descripcion})
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Tipo de transacción no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Tipo de transacción actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD reporte_ventas

@app.get("/reportes_ventasget")
def obtener_reportes_ventas():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_reportes, fecha_generacion, total_ventas FROM reporte_ventas")
        reportes_ventas = []
        for id_reportes, fecha_generacion, total_ventas in cursor:
            reportes_ventas.append({
                "id_reportes": id_reportes,
                "fecha_generacion": fecha_generacion,
                "total_ventas": total_ventas
            })
        cursor.close()
        cone.close()
        return reportes_ventas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reportes_ventasgetid/{id_reportes}")
def obtener_reporte_ventas(id_reportes: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_reportes, fecha_generacion, total_ventas FROM reporte_ventas WHERE id_reportes = :id_reportes", {"id_reportes": id_reportes})
        reporte_ventas = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if reporte_ventas:
            return {
                "id_reportes": reporte_ventas[0],
                "fecha_generacion": reporte_ventas[1],
                "total_ventas": reporte_ventas[2]
            }
        else:
            raise HTTPException(status_code=404, detail="Reporte de ventas no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reportes_ventaspost")
def agregar_reporte_ventas(fecha_generacion: str, total_ventas: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO reporte_ventas
            VALUES (:fecha_generacion, :total_ventas)
        """, {"fecha_generacion": fecha_generacion, "total_ventas": total_ventas})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de ventas creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/reportes_ventasput/{id_reportes}")
def actualizar_reporte_ventas(id_reportes: int, fecha_generacion: str, total_ventas: float):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE reporte_ventas
            SET fecha_generacion = :fecha_generacion, total_ventas = :total_ventas
            WHERE id_reportes = :id_reportes
        """, {"id_reportes": id_reportes, "fecha_generacion": fecha_generacion, "total_ventas": total_ventas})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Reporte de ventas no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de ventas actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/reportes_ventasdelete/{id_reportes}")
def eliminar_reporte_ventas(id_reportes: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM reporte_ventas WHERE id_reportes = :id_reportes", {"id_reportes": id_reportes})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Reporte de ventas no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de ventas eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/reportes_ventas/{id_reportes}")
def actualizar_parcial_reporte_ventas(id_reportes: int, fecha_generacion: Optional[str] = None,
                                      total_ventas: Optional[float] = None):
    try:
        if not any([fecha_generacion, total_ventas]):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        campos = []
        valores = {"id_reportes": id_reportes}
        if fecha_generacion:
            campos.append("fecha_generacion = :fecha_generacion")
            valores["fecha_generacion"] = fecha_generacion
        if total_ventas:
            campos.append("total_ventas = :total_ventas")
            valores["total_ventas"] = total_ventas
        query = f"UPDATE reporte_ventas SET {', '.join(campos)} WHERE id_reportes = :id_reportes"
        cursor.execute(query, valores)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Reporte de ventas no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de ventas actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CRUD reporte_desempenio

@app.get("/reportes_desempenioget")
def obtener_reportes_desempenio():
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_reporte_desempenio, fecha_generacion, datos_evaluacion FROM reporte_desempenio")
        reportes_desempenio = []
        for id_reporte_desempenio, fecha_generacion, datos_evaluacion in cursor:
            reportes_desempenio.append({
                "id_reporte_desempenio": id_reporte_desempenio,
                "fecha_generacion": fecha_generacion,
                "datos_evaluacion": datos_evaluacion
            })
        cursor.close()
        cone.close()
        return reportes_desempenio
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reportes_desempeniogetid/{id_reporte_desempenio}")
def obtener_reporte_desempenio(id_reporte_desempenio: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("SELECT id_reporte_desempenio, fecha_generacion, datos_evaluacion FROM reporte_desempenio WHERE id_reporte_desempenio = :id_reporte_desempenio", {"id_reporte_desempenio": id_reporte_desempenio})
        reporte_desempenio = cursor.fetchone()
        cursor.close()
        cone.close()
        
        if reporte_desempenio:
            return {
                "id_reporte_desempenio": reporte_desempenio[0],
                "fecha_generacion": reporte_desempenio[1],
                "datos_evaluacion": reporte_desempenio[2]
            }
        else:
            raise HTTPException(status_code=404, detail="Reporte de desempeño no encontrado")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reportes_desempeniopost")
def agregar_reporte_desempenio(fecha_generacion: str, datos_evaluacion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            INSERT INTO reporte_desempenio
            VALUES (:fecha_generacion, :datos_evaluacion)
        """, {"fecha_generacion": fecha_generacion, "datos_evaluacion": datos_evaluacion})
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de desempeño creado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/reportes_desempenioput/{id_reporte_desempenio}")
def actualizar_reporte_desempenio(id_reporte_desempenio: int, fecha_generacion: str, datos_evaluacion: str):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("""
            UPDATE reporte_desempenio
            SET fecha_generacion = :fecha_generacion, datos_evaluacion = :datos_evaluacion
            WHERE id_reporte_desempenio = :id_reporte_desempenio
        """, {"id_reporte_desempenio": id_reporte_desempenio, "fecha_generacion": fecha_generacion, "datos_evaluacion": datos_evaluacion})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Reporte de desempeño no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de desempeño actualizado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/reportes_desempeniodelete/{id_reporte_desempenio}")
def eliminar_reporte_desempenio(id_reporte_desempenio: int):
    try:
        cone = get_conexion()
        cursor = cone.cursor()
        cursor.execute("DELETE FROM reporte_desempenio WHERE id_reporte_desempenio = :id_reporte_desempenio", {"id_reporte_desempenio": id_reporte_desempenio})
        
        if cursor.rowcount == 0:
            cursor.close()
            cone.close()
            raise HTTPException(status_code=404, detail="Reporte de desempeño no encontrado")
        
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de desempeño eliminado con éxito"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/reportes_desempenio/{id_reporte_desempenio}")
def actualizar_parcial_reporte_desempenio(id_reporte_desempenio: int, fecha_generacion: Optional[str] = None,
                                          datos_evaluacion: Optional[str] = None):
    try:
        if not any([fecha_generacion, datos_evaluacion]):
            raise HTTPException(status_code=400, detail="Debe enviar al menos un dato")
        cone = get_conexion()
        cursor = cone.cursor()
        campos = []
        valores = {"id_reporte_desempenio": id_reporte_desempenio}
        if fecha_generacion:
            campos.append("fecha_generacion = :fecha_generacion")
            valores["fecha_generacion"] = fecha_generacion
        if datos_evaluacion:
            campos.append("datos_evaluacion = :datos_evaluacion")
            valores["datos_evaluacion"] = datos_evaluacion
        query = f"UPDATE reporte_desempenio SET {', '.join(campos)} WHERE id_reporte_desempenio = :id_reporte_desempenio"
        cursor.execute(query, valores)
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Reporte de desempeño no encontrado")
        cone.commit()
        cursor.close()
        cone.close()
        return {"Mensaje": "Reporte de desempeño actualizado parcialmente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
