
import os
import json
import sqlalchemy
#import google.oauth2.id_token
#import google.auth.transport.requests

from sqlalchemy import create_engine
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes


def get_credentials():
    from google.cloud import secretmanager
    import os, json

    client = secretmanager.SecretManagerServiceClient()

    # Detectar el entorno
    environment = os.getenv("ENVIRONMENT", "stage").lower()

    if environment == "prod":
        secret_name = "projects/263751840195/secrets/postgres-db-prod-credentials-api_dataops_user/versions/latest"
    else:
        secret_name = "projects/911414108629/secrets/postgres-db-stage-credentials-usr_dev_stage/versions/latest"

    print(f"🔐 Conectando a base de datos: {environment.upper()}")

    response = client.access_secret_version(name=secret_name).payload.data.decode("UTF-8")
    creds_dict = json.loads(response)

    return creds_dict


def getEngine():
    
    def getconn() -> sqlalchemy.engine.base.Connection:
    
        # Se ontiene las credenciales de acceso a la base:
        creds_dict = get_credentials()
        
        # Parametros de conexión a la base
        INSTANCE_CONNECTION_NAME = creds_dict['host']
        DB_USER = creds_dict['user']
        DB_PASS = creds_dict['password']
        DB_NAME = creds_dict['database']
        IP_TYPE = IPTypes.PUBLIC
    
        # Inicialización del objeto que permite la conexión a la base:
        connector = Connector()
        conn = connector.connect(INSTANCE_CONNECTION_NAME, 
                                 "pg8000", 
                                 user = DB_USER, 
                                 password = DB_PASS, 
                                 db = DB_NAME,
                                 ip_type = IP_TYPE)
        
        return conn

    # Crear el motor de SQLAlchemy
    engine = create_engine("postgresql+pg8000://", creator=getconn)
    
    return engine
