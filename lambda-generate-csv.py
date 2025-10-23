import json
import csv
import boto3
from io import StringIO
from datetime import datetime

# Cliente de S3 (opcional, si quieres guardar el CSV en S3)
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda function que genera un archivo CSV con encabezados específicos
    
    Parámetros esperados en event:
    - data: lista de diccionarios con los datos (opcional)
    - bucket_name: nombre del bucket S3 donde guardar el archivo (opcional)
    - file_name: nombre del archivo CSV (opcional, default: output.csv)
    """
    
    # Encabezados del CSV
    headers = [
        'Item',
        'Order ID',
        'Order date',
        'Outposts ID',
        'Asset ID',
        'PO',
        'State',
        'Region Coppel (site name)',
        'Site detalle'
    ]
    
    # Obtener datos del evento (si se proporcionan)
    data = event.get('data', [])
    bucket_name = event.get('bucket_name')
    file_name = event.get('file_name', f'output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    
    # Si no hay datos, crear una fila de ejemplo
    if not data:
        data = [{
            'Item': 'Example Item',
            'Order ID': 'ORD-12345',
            'Order date': datetime.now().strftime('%Y-%m-%d'),
            'Outposts ID': 'OP-001',
            'Asset ID': 'AST-001',
            'PO': 'PO-12345',
            'State': 'Active',
            'Region Coppel (site name)': 'North Region',
            'Site detalle': 'Site details example'
        }]
    
    # Crear CSV en memoria
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=headers)
    
    # Escribir encabezados
    writer.writeheader()
    
    # Escribir datos
    for row in data:
        writer.writerow(row)
    
    # Obtener contenido del CSV
    csv_content = csv_buffer.getvalue()
    
    # Si se especifica un bucket, guardar en S3
    if bucket_name:
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=csv_content,
                ContentType='text/csv'
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'CSV file generated and uploaded successfully',
                    'bucket': bucket_name,
                    'file_name': file_name,
                    'records_written': len(data)
                })
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error uploading to S3',
                    'error': str(e)
                })
            }
    
    # Si no hay bucket, devolver el CSV como string en base64
    else:
        import base64
        csv_base64 = base64.b64encode(csv_content.encode()).decode()
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/csv',
                'Content-Disposition': f'attachment; filename="{file_name}"'
            },
            'body': csv_content,
            'isBase64Encoded': False,
            'metadata': {
                'records_written': len(data)
            }
        }
