import json
import boto3
from datetime import datetime

# Clientes de AWS
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')

def lambda_handler(event, context):
    """
    Lambda que lee fotos de etiquetas desde S3 y extrae key-value pairs usando Textract
    
    Parámetros esperados en event:
    - bucket_name: nombre del bucket S3 (default: 'outposts-fotos')
    - image_key: ruta de la imagen en S3 (ej: 'etiqueta_001.jpg')
    O si se activa automáticamente por S3:
    - Records: evento de S3 con información de la imagen subida
    """
    
    # Determinar bucket e imagen
    bucket_name = None
    image_key = None
    
    # Caso 1: Evento disparado por S3 (cuando se sube una imagen)
    if 'Records' in event:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        image_key = event['Records'][0]['s3']['object']['key']
        print(f"Evento S3 detectado: {bucket_name}/{image_key}")
    
    # Caso 2: Invocación manual con parámetros
    else:
        bucket_name = event.get('bucket_name', 'outposts-fotos')
        image_key = event.get('image_key')
        
        if not image_key:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'image_key is required',
                    'message': 'Proporciona la ruta de la imagen en S3'
                })
            }
    
    try:
        # Llamar a Textract para analizar el documento
        print(f"Analizando imagen: {bucket_name}/{image_key}")
        
        response = textract_client.analyze_document(
            Document={
                'S3Object': {
                    'Bucket': bucket_name,
                    'Name': image_key
                }
            },
            FeatureTypes=['FORMS']  # FORMS detecta key-value pairs
        )
        
        # Extraer los key-value pairs
        key_value_pairs = extract_key_values(response)
        
        # También extraer todo el texto plano (por si acaso)
        all_text = extract_all_text(response)
        
        # Preparar respuesta
        result = {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Imagen analizada exitosamente',
                'bucket': bucket_name,
                'image_key': image_key,
                'key_value_pairs': key_value_pairs,
                'all_text': all_text,
                'analyzed_at': datetime.now().isoformat(),
                'blocks_count': len(response.get('Blocks', []))
            }, ensure_ascii=False, indent=2)
        }
        
        return result
        
    except textract_client.exceptions.InvalidS3ObjectException:
        return {
            'statusCode': 404,
            'body': json.dumps({
                'error': 'Image not found',
                'message': f'No se encontró la imagen en {bucket_name}/{image_key}'
            })
        }
    
    except textract_client.exceptions.UnsupportedDocumentException:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Unsupported format',
                'message': 'El formato de la imagen no es compatible. Usa JPG, PNG o PDF.'
            })
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal error',
                'message': str(e)
            })
        }


def extract_key_values(response):
    """
    Extrae los key-value pairs del response de Textract
    """
    key_value_pairs = {}
    
    # Crear un mapa de bloques por ID para referencia rápida
    blocks = response.get('Blocks', [])
    block_map = {block['Id']: block for block in blocks}
    
    # Buscar bloques de tipo KEY_VALUE_SET
    for block in blocks:
        if block['BlockType'] == 'KEY_VALUE_SET':
            if 'KEY' in block.get('EntityTypes', []):
                # Es una key
                key_text = get_text_from_block(block, block_map)
                
                # Buscar el value asociado
                value_text = ''
                if 'Relationships' in block:
                    for relationship in block['Relationships']:
                        if relationship['Type'] == 'VALUE':
                            for value_id in relationship['Ids']:
                                value_block = block_map.get(value_id)
                                if value_block:
                                    value_text = get_text_from_block(value_block, block_map)
                
                # Limpiar la key (remover ':' al final si existe)
                key_text = key_text.strip().rstrip(':')
                value_text = value_text.strip()
                
                if key_text:
                    key_value_pairs[key_text] = value_text
    
    return key_value_pairs


def get_text_from_block(block, block_map):
    """
    Obtiene el texto de un bloque siguiendo sus relaciones
    """
    text = ''
    
    if 'Relationships' in block:
        for relationship in block['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    child_block = block_map.get(child_id)
                    if child_block and child_block['BlockType'] == 'WORD':
                        text += child_block.get('Text', '') + ' '
    
    return text.strip()


def extract_all_text(response):
    """
    Extrae todo el texto detectado (alternativa si key-value no funciona bien)
    """
    lines = []
    
    for block in response.get('Blocks', []):
        if block['BlockType'] == 'LINE':
            lines.append(block.get('Text', ''))
    
    return lines
