import json
import csv
from io import StringIO
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda function que genera un archivo CSV y lo retorna
    
    Parámetros esperados en event:
    - data: lista de diccionarios con los datos
    - file_name: nombre del archivo CSV (opcional)
    - include_example: si es True y no hay data, incluye datos de ejemplo (opcional)
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
    
    # Retornar el CSV
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'text/csv',
            'Content-Disposition': f'attachment; filename="{file_name}"'
        },
        'body': csv_content,
        'file_name': file_name,
        'records_count': len(data),
        'generated_at': datetime.now().isoformat()
    }
