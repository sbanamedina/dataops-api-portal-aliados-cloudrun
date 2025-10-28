import json
import sqlalchemy
import pandas as pd
import datetime as dt

from utils.connect_sql import getEngine

from utils.connect_aws import process_and_upload_data

def request_postgres(input_query, params):

  # Crear el motor de SQLAlchemy
  engine = getEngine()
  
  with engine.connect() as connection:

    results = connection.execute(sqlalchemy.text(input_query), params)

    data = pd.DataFrame(results.fetchall())
    
    return data

def consulta_clientes_aliados(CODIGO_AGENTE = None,TIPO_DOCUMENTO_ASEGURADO = None,NUMERO_DOCUMENTO_ASEGURADO = None,NOMBRE = None,ESTADO_POLIZA = None,CODIGO_PRODUCTO = None,numeroDocumentoSolicitante = None, tipoDocumentoSolicitante = None):
    try:
        filtros = {
            "CODIGO_AGENTE": CODIGO_AGENTE,
            "TIPO_DOCUMENTO_ASEGURADO": TIPO_DOCUMENTO_ASEGURADO,
            "NUMERO_DOCUMENTO_ASEGURADO": NUMERO_DOCUMENTO_ASEGURADO,
            "NOMBRE": NOMBRE,
            "ESTADO_POLIZA": ESTADO_POLIZA,
            "CODIGO_PRODUCTO": CODIGO_PRODUCTO
        }

        # Inicializar las cláusulas WHERE y los parámetros
        where_clauses = []
        params = {}

        # Construir las cláusulas WHERE dinámicamente (solo si el filtro es enviado dentro del body)
        for key, value in filtros.items():
            if value is not None:
                if key in ("CODIGO_AGENTE","NUMERO_DOCUMENTO_ASEGURADO","CODIGO_PRODUCTO"):
                    where_clauses.append(f'"{key}" = {value}')
                elif key in ("NOMBRE"):
                    where_clauses.append(f'UPPER("{key}") LIKE UPPER(\'%{value}%\')')
                else:
                    where_clauses.append(f'"{key}" = \'{value}\'')
                params[key] = value

        # Verificar si se proporcionaron filtros
        if not where_clauses:
            return {
                "error": "Debe proporcionar al menos un filtro"
            }
        
        # Unir las cláusulas WHERE con AND
        where_sql = " AND ".join(where_clauses)

        params = {}
        # Construir la consulta SQL
        query = f"""
            SELECT 
              "CODIGO_AGENTE",
              "NOMBRE_AGENTE",
              "CODIGO_LOCALIDAD",
              "VALOR_IVA",
              "VALOR_TOTAL",
              "TIPO_DOCUMENTO_ASEGURADO",
              "NUMERO_DOCUMENTO_ASEGURADO",
              "NOMBRE",
              "ESTADO_POLIZA",
              "CIUDAD",
              "DIRECCION",
              "CELULAR",
              "CORREO",
              "PRODUCTOS_VIGENTES",
              "FECHA_NACIMIENTO",
              "EDAD",
              "ESTADO_CIVIL",
              "FECHA_SARLAFT_SIMPLIFICADO",
              "FECHA_SARLAFT_ORDINARIO",
              "NUMERO_POLIZA",
              "PRODUCTO_POLIZA",
              "ROL",
              "VALOR_ASEGURADO",
              "NUMERO_ENDOSO",
              "DESCRIPCION_ENDOSO",
              "FECHA_EMISION_ENDOSO",
              "FECHA_INICIO_ENDOSO",
              "FECHA_FIN_ENDOSO",
              "MOVIMIENTO",
              "OBSERVACIONES_ENDOSO",
              "FECHA_CORTE",
              "VALOR_PRIMA",
              "VALOR_ASISTENCIA",
              "VIGENCIA_INICIO_POLIZA",
              "VIGENCIA_FIN_POLIZA",
              "FECHA_EMISION",
              "CODIGO_RAMO_EMISION",
              "NOMBRE_RAMO_EMISION",
              "CODIGO_PRODUCTO",
              "NOMBRE_PRODUCTO",
              "CODIGO_SUBPRODUCTO",
              "NOMBRE_SUBPRODUCTO",
              "TIPO_NEGOCIO",
              "NUMERO_POLIZA_MADRE",
              "CANTIDAD_RIESGOS",
              "TIPO_DOCUMENTO_TOMADOR",
              "NUMERO_DOCUMENTO_TOMADOR",
              "NOMBRE_TOMADOR",
              "VIGENCIA_INICIO_POLIZA_MADRE",
              "VIGENCIA_FIN_POLIZA_MADRE",
              "PERIODICIDAD_PAGO",
              "FORMA_PAGO",
              "CANAL_DE_DESCUENTO",
              "PORCENTAJE_COMISION",
              "PARENTESCO",
              "CODIGO_COBERTURA",
              "DESCRIPCION_COBERTURA", 
              "NOMBRE_LOCALIDAD"
            FROM "api_backend"."paliados_clientes"
            WHERE {where_sql};
        """
        response = request_postgres(query, params)
        response = json.loads(response.to_json(orient='records', date_format='iso'))

        return process_and_upload_data(data_list=response,numeroDocumentoSolicitante=numeroDocumentoSolicitante,tipoDocumentoSolicitante=tipoDocumentoSolicitante)

    except Exception as e:
        return {
            "error": str(e),
            "message": "Excepción en la consulta de clientes aliados"
        }

def consulta_clientes_aliados_portal(
    CODIGO_AGENTE=None,
    TIPO_DOCUMENTO_ASEGURADO=None,
    NUMERO_DOCUMENTO_ASEGURADO=None,
    NOMBRE=None,
    ESTADO_POLIZA=None,
    CODIGO_PRODUCTO=None,
    pagina=None,
    registros_por_pagina=None
):
    try:
        filtros = {
            "CODIGO_AGENTE": CODIGO_AGENTE,
            "TIPO_DOCUMENTO_ASEGURADO": TIPO_DOCUMENTO_ASEGURADO,
            "NUMERO_DOCUMENTO_ASEGURADO": NUMERO_DOCUMENTO_ASEGURADO,
            "NOMBRE": NOMBRE,
            "ESTADO_POLIZA": ESTADO_POLIZA,
            "CODIGO_PRODUCTO": CODIGO_PRODUCTO
        }

        where_clauses = []
        params = {}

        for key, value in filtros.items():
            if value is not None:
                if key in ("CODIGO_AGENTE", "NUMERO_DOCUMENTO_ASEGURADO", "CODIGO_PRODUCTO"):
                    where_clauses.append(f'"{key}" = {value}')
                elif key in ("NOMBRE"):
                    where_clauses.append(f'UPPER("{key}") LIKE UPPER(\'%{value}%\')')
                else:
                    where_clauses.append(f'"{key}" = \'{value}\'')
                params[key] = value

        if not where_clauses:
            return {
                "error": "Debe proporcionar al menos un filtro"
            }

        where_sql = " AND ".join(where_clauses)

        # Consulta para contar el total de registros
        count_query = f"""
            SELECT COUNT(*) as total
            FROM "api_backend"."paliados_clientes"
            WHERE {where_sql};
        """
        engine = getEngine()
        with engine.connect() as connection:
            total_result = connection.execute(sqlalchemy.text(count_query))
            total_registros = total_result.scalar()

        # Si no se envía paginación, consulta todos los datos
        if pagina is None or registros_por_pagina is None:
            query = f"""
                SELECT 
                  "CODIGO_AGENTE",
                  "TIPO_DOCUMENTO_ASEGURADO",
                  "NUMERO_DOCUMENTO_ASEGURADO",
                  "NOMBRE",
                  "ESTADO_POLIZA",
                  "CIUDAD",
                  "DIRECCION",
                  "CELULAR",
                  "CORREO",
                  "PRODUCTOS_VIGENTES",
                  "FECHA_NACIMIENTO",
                  "EDAD",
                  "ESTADO_CIVIL",
                  "FECHA_SARLAFT_SIMPLIFICADO",
                  "FECHA_SARLAFT_ORDINARIO",
                  "NUMERO_POLIZA",
                  "PRODUCTO_POLIZA",
                  "ROL",
                  "VALOR_ASEGURADO",
                  "VALOR_PRIMA",
                  "VALOR_ASISTENCIA",
                  "VIGENCIA_INICIO_POLIZA",
                  "VIGENCIA_FIN_POLIZA",
                  "FECHA_EMISION",
                  "CODIGO_RAMO_EMISION",
                  "NOMBRE_RAMO_EMISION",
                  "CODIGO_PRODUCTO",
                  "NOMBRE_PRODUCTO",
                  "CODIGO_SUBPRODUCTO",
                  "NOMBRE_SUBPRODUCTO",
                  "NUMERO_POLIZA_MADRE",
                  "CANTIDAD_RIESGOS",
                  "TIPO_DOCUMENTO_TOMADOR",
                  "NUMERO_DOCUMENTO_TOMADOR",
                  "NOMBRE_TOMADOR",
                  "VIGENCIA_INICIO_POLIZA_MADRE",
                  "VIGENCIA_FIN_POLIZA_MADRE",
                  "PERIODICIDAD_PAGO",
                  "FORMA_PAGO",
                  "CANAL_DE_DESCUENTO",
                  "NOMBRE_LOCALIDAD"
                FROM "api_backend"."paliados_clientes"
                WHERE {where_sql};
            """
        else:
            offset = (pagina - 1) * registros_por_pagina
            limit = registros_por_pagina
            query = f"""
                SELECT 
                  "CODIGO_AGENTE",
                  "TIPO_DOCUMENTO_ASEGURADO",
                  "NUMERO_DOCUMENTO_ASEGURADO",
                  "NOMBRE",
                  "ESTADO_POLIZA",
                  "CIUDAD",
                  "DIRECCION",
                  "CELULAR",
                  "CORREO",
                  "PRODUCTOS_VIGENTES",
                  "FECHA_NACIMIENTO",
                  "EDAD",
                  "ESTADO_CIVIL",
                  "FECHA_SARLAFT_SIMPLIFICADO",
                  "FECHA_SARLAFT_ORDINARIO",
                  "NUMERO_POLIZA",
                  "PRODUCTO_POLIZA",
                  "ROL",
                  "VALOR_ASEGURADO",
                  "VALOR_PRIMA",
                  "VALOR_ASISTENCIA",
                  "VIGENCIA_INICIO_POLIZA",
                  "VIGENCIA_FIN_POLIZA",
                  "FECHA_EMISION",
                  "CODIGO_RAMO_EMISION",
                  "NOMBRE_RAMO_EMISION",
                  "CODIGO_PRODUCTO",
                  "NOMBRE_PRODUCTO",
                  "CODIGO_SUBPRODUCTO",
                  "NOMBRE_SUBPRODUCTO",
                  "NUMERO_POLIZA_MADRE",
                  "CANTIDAD_RIESGOS",
                  "TIPO_DOCUMENTO_TOMADOR",
                  "NUMERO_DOCUMENTO_TOMADOR",
                  "NOMBRE_TOMADOR",
                  "VIGENCIA_INICIO_POLIZA_MADRE",
                  "VIGENCIA_FIN_POLIZA_MADRE",
                  "PERIODICIDAD_PAGO",
                  "FORMA_PAGO",
                  "CANAL_DE_DESCUENTO",
                  "NOMBRE_LOCALIDAD"
                FROM "api_backend"."paliados_clientes"
                WHERE {where_sql}
                OFFSET {offset} LIMIT {limit};
            """

        print(query)

        response = request_postgres(query, params)
        response = json.loads(response.to_json(orient='records', date_format='iso'))

        return {
            "clientes": response,
            "total_registros": total_registros,
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina
        }

    except Exception as e:
        return {
            "error": str(e),
            "message": "Excepción en la consulta de clientes aliados"
        }
