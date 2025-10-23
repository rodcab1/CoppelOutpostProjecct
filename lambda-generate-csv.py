import json
import csv
import boto3
from io import StringIO
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def lambda_handler(event, context):
    """
    Lambda function que genera un archivo CSV y lo envía por email usando SES
    
    Parámetros esperados en event:
    - data: lista de diccionarios con los datos
    - file_name: nombre del archivo CSV (opcional)
    - include_example: si es True y no hay data, incluye datos de ejemplo (opcional)
    - email_to: email del destinatario (requerido para envío)
    - email_from: email del remitente (requerido para envío)
    - email_subject: asunto del email (opcional)
    - email_body: cuerpo del email (opcional)
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
    
    # Obtener parámetros del evento
    data = event.get('data', [])
    file_name = event.get('file_name', f'reporte_coppel_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    include_example = event.get('include_example', False)
    
    # Parámetros de email
    email_to = event.get('email_to')
    email_from = event.get('email_from')
    email_subject = event.get('email_subject', f'Reporte Coppel - {datetime.now().strftime("%Y-%m-%d")}')
    email_body = event.get('email_body', f'Adjunto encontrarás el reporte con {len(data)} registros.')
    
    # Validar parámetros requeridos para envío
    if not email_to or not email_from:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Se requieren los parámetros email_to y email_from para enviar el email'
            })
        }
    
    # Si no hay datos y se solicita ejemplo, crear datos de ejemplo
    if not data and include_example:
        data = [{
            'Item': '1',
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
    
    try:
        # Crear mensaje MIME
        msg = MIMEMultipart()
        msg['Subject'] = email_subject
        msg['From'] = email_from
        msg['To'] = email_to
        
        # Agregar cuerpo del email
        body = MIMEText(email_body, 'plain', 'utf-8')
        msg.attach(body)
        
        # Agregar archivo CSV como adjunto
        attachment = MIMEApplication(csv_content.encode('utf-8'))
        attachment.add_header('Content-Disposition', 'attachment', filename=file_name)
        msg.attach(attachment)
        
        # Enviar email usando SES
        ses_client = boto3.client('ses', region_name='us-east-1')  # Ajusta la región según tu configuración
        
        response = ses_client.send_raw_email(
            Source=email_from,
            Destinations=[email_to],
            RawMessage={'Data': msg.as_string()}
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Email enviado exitosamente',
                'messageId': response['MessageId'],
                'file_name': file_name,
                'records_count': len(data),
                'email_to': email_to,
                'generated_at': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Error al enviar email: {str(e)}'
            })
        }
