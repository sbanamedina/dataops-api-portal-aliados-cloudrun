import sys 
sys.path.append('.')

#* Librerías para el API
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import os

#* Librerías propias para el funcionamiento del API
from utils.request_postgres import consulta_clientes_aliados, consulta_clientes_aliados_portal

# Parametros básicos y clases
app = FastAPI() 
puerto = os.environ.get("PORT", 8080)

# Configuración de CORS
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#* Definición de un endpoint
descripcion_path = 'API que carga información de clientes aliados desde BigQuery en csv, consultando por filtros como código agente, documento, producto, etc.'
summary_path = 'Consulta clientes desde BigQuery con múltiples filtros opcionales (al menos uno requerido)'
endpoint_end = '/api_descarga_portal_aliados'

# Mock de ejemplo de entrada en Body de request
"""
{
  "numeroDocumentoSolicitante": "CC",
  "tipoDocumentoSolicitante": "123456",
  "codigo_agente": "55903",
  "tipo_documento": "CC",
  "id_documento": "1020948732",
  "nombre": "Juan",
  "estado_poliza": "Vigente",
  "producto": "123456"
}
"""

@app.post(endpoint_end, summary=summary_path, description=descripcion_path)
async def api_consulta_afiliacion_empresa(body: dict):
    numeroDocumentoSolicitante = body.get("numeroDocumentoSolicitante")
    tipoDocumentoSolicitante = body.get("tipoDocumentoSolicitante")
    codigo_agente = body.get("codigo_agente")
    tipo_documento = body.get("tipo_documento")
    id_documento = body.get("id_documento")
    nombre = body.get("nombre")
    estado_poliza = body.get("estado_poliza")
    producto = body.get("producto")

    if not any([codigo_agente, tipo_documento, id_documento, nombre, estado_poliza, producto]):
        return {"error": "Debe proporcionar al menos un parámetro"}
    
    if not tipoDocumentoSolicitante and numeroDocumentoSolicitante:
        return {"error": "Debe proporcionar la información de solicitante"}

    response = consulta_clientes_aliados(
        numeroDocumentoSolicitante=numeroDocumentoSolicitante,
        tipoDocumentoSolicitante=tipoDocumentoSolicitante,
        CODIGO_AGENTE=codigo_agente,
        TIPO_DOCUMENTO_ASEGURADO=tipo_documento,
        NUMERO_DOCUMENTO_ASEGURADO=id_documento,
        NOMBRE=nombre,
        ESTADO_POLIZA=estado_poliza,
        CODIGO_PRODUCTO=producto
    )
    return response

descripcion_path_consulta = 'API que retorna información de clientes aliados desde BigQuery, consultando por filtros como código agente, documento, producto, etc.'
summary_path_consulta = 'Consulta clientes desde BigQuery con múltiples filtros opcionales (al menos uno requerido)'

@app.post("/api_consulta_portal_aliados", summary=summary_path_consulta, description=descripcion_path_consulta)
async def api_consulta_portal_aliados(body: dict):
    codigo_agente = body.get("codigo_agente")
    tipo_documento = body.get("tipo_documento")
    id_documento = body.get("id_documento")
    nombre = body.get("nombre")
    estado_poliza = body.get("estado_poliza")
    producto = body.get("producto")
    pagina = body.get("pagina")
    registros_por_pagina = body.get("registros_por_pagina")
    if not any([codigo_agente, tipo_documento, id_documento, nombre, estado_poliza, producto]):
        return {"error": "Debe proporcionar al menos un parámetro"}

    # Si no se envían ambos parámetros, pásalos como None
    if pagina is None or registros_por_pagina is None:
        pagina = None
        registros_por_pagina = None

    response = consulta_clientes_aliados_portal(
        CODIGO_AGENTE=codigo_agente,
        TIPO_DOCUMENTO_ASEGURADO=tipo_documento,
        NUMERO_DOCUMENTO_ASEGURADO=id_documento,
        NOMBRE=nombre,
        ESTADO_POLIZA=estado_poliza,
        CODIGO_PRODUCTO=producto,
        pagina=pagina,
        registros_por_pagina=registros_por_pagina
    )
    return response

@app.get("/", response_class=RedirectResponse)
async def redirect_to_docs():
    return "/docs"

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=int(puerto))
