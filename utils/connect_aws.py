import pandas as pd
import boto3
import json
import io
import os
from datetime import datetime
import uuid
from google.cloud import secretmanager

def get_aws_credentials():

    client = secretmanager.SecretManagerServiceClient()
    aws_creds = 'projects/911414108629/secrets/aws-dev-fuerza-ventas-credentials/versions/latest'
    response = client.access_secret_version(name=aws_creds).payload.data.decode("UTF-8")
    creds_dict = json.loads(response)

    return creds_dict

def process_and_upload_data(data_list,numeroDocumentoSolicitante=None, tipoDocumentoSolicitante=None):
    """
    Convierte una lista de diccionarios (resultado de la query) a CSV,
    la sube a S3 y retorna una URL prefirmada.

    :param data_list: Una lista de diccionarios, representando los registros de la query.
    :return: Un diccionario con la URL prefirmada del archivo CSV en S3 o un mensaje de error.
    """

    aws_credentials = get_aws_credentials()

    S3_BUCKET_NAME = aws_credentials.get("S3_BUCKET_NAME")
    AWS_REGION = aws_credentials.get("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID = aws_credentials.get("ACCESS_KEY")
    AWS_SECRET_ACCESS_KEY = aws_credentials.get("SECRET_ACCESS_KEY")

    S3_FOLDER_PATH = "resultados_apis_dataops/cliente_portal_aliados/" # La carpeta dentro de tu bucket S3

    if not S3_BUCKET_NAME or not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        error_msg = "Error: Las variables de entorno S3_BUCKET_NAME, AWS_ACCESS_KEY_ID o AWS_SECRET_ACCESS_KEY no están configuradas."
        print(error_msg)
        return {"error": error_msg}

    if not data_list:
        print("La lista de datos está vacía, no hay nada que exportar.")
        return {"message": "La consulta no devolvió resultados para exportar."}

    try:
        # 1. Convertir la lista de diccionarios a un DataFrame de Pandas
        df = pd.DataFrame(data_list)

        # 2. Mapear a CSV en memoria
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()

        # 3. Generar un nombre de archivo único para S3
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8] 
        file_name = f"{numeroDocumentoSolicitante}_{tipoDocumentoSolicitante}_{timestamp}_{unique_id}.csv"
        s3_object_key = f"{S3_FOLDER_PATH}{file_name}"

        # 4. Enviar el archivo CSV a S3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_object_key, Body=csv_content, ContentType='text/csv')
        print(f"Archivo '{s3_object_key}' subido exitosamente al bucket '{S3_BUCKET_NAME}'.")

        # 5. Retornar una URL prefirmada con vigencia de un día (86400 segundos)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_object_key},
            ExpiresIn=86400 
        )
        print(f"URL prefirmada generada: {presigned_url}")

        return {"csv_download_url": presigned_url}

    except Exception as e:
        print(f"Error en el proceso: {e}")
        return {"error": str(e)}