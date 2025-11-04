import boto3
import base64
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    Lambda que se activa cuando se sube una imagen a S3
    Usa Claude para detectar el color del sensor ShockWatch
    """
    
    # Inicializar clientes AWS
    s3 = boto3.client('s3')
    bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
    
    # Obtener información del evento S3
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"Procesando imagen: {bucket}/{key}")
    
    try:
        # Leer imagen desde S3
        response = s3.get_object(Bucket=bucket, Key=key)
        image_bytes = response['Body'].read()
        
        # Determinar tipo de imagen
        content_type = response['ContentType']
        if 'jpeg' in content_type or 'jpg' in content_type:
            media_type = 'image/jpeg'
        elif 'png' in content_type:
            media_type = 'image/png'
        else:
            media_type = 'image/jpeg'  # default
        
        # Convertir a base64
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        
        # Prompt para Claude
        prompt = """Tengo esta imagen de un Shock Sensor ShockWatch. Es una etiqueta verde con negra, 
dentro hay un rectángulo blanco horizontal y justo en el centro hay una barrita horizontal 
que puede ser blanca o roja. 

Si la barrita es ROJA, significa que el sensor se activó por mal manejo (mal handling).
Si la barrita es BLANCA, significa que el producto fue manejado correctamente.

Analiza la imagen cuidadosamente y responde SOLO con un JSON en este formato exacto:
{"color": "ROJO", "estado": "ACTIVADO - Mal handling", "confianza": 95}
o
{"color": "BLANCO", "estado": "OK - Buen handling", "confianza": 90}

Si no puedes ver claramente el sensor, responde:
{"color": "NO_DETECTADO", "estado": "No se puede determinar", "confianza": 0}"""
        
        # Llamar a Claude via Bedrock
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": image_base64
                            }
                        },
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ]
        })
        
        # Invocar Claude Sonnet 4.5
        response = bedrock.invoke_model(
            modelId='us.anthropic.claude-sonnet-4-5-20250929-v1:0',
            body=body
        )
        
        # Procesar respuesta
        result = json.loads(response['body'].read())
        respuesta_texto = result['content'][0]['text']
        
        print(f"Respuesta de Claude: {respuesta_texto}")
        
        # Intentar extraer JSON de la respuesta
        try:
            # Buscar JSON en la respuesta
            start_idx = respuesta_texto.find('{')
            end_idx = respuesta_texto.rfind('}') + 1
            if start_idx != -1 and end_idx > start_idx:
                json_str = respuesta_texto[start_idx:end_idx]
                resultado = json.loads(json_str)
            else:
                # Si no hay JSON, crear uno basado en la respuesta
                if 'rojo' in respuesta_texto.lower():
                    resultado = {
                        "color": "ROJO",
                        "estado": "ACTIVADO - Mal handling",
                        "confianza": 85
                    }
                elif 'blanco' in respuesta_texto.lower():
                    resultado = {
                        "color": "BLANCO",
                        "estado": "OK - Buen handling",
                        "confianza": 85
                    }
                else:
                    resultado = {
                        "color": "NO_DETECTADO",
                        "estado": respuesta_texto,
                        "confianza": 0
                    }
        except json.JSONDecodeError:
            resultado = {
                "color": "ERROR",
                "estado": respuesta_texto,
                "confianza": 0
            }
        
        # Agregar metadata
        resultado['archivo'] = key
        resultado['bucket'] = bucket
        resultado['timestamp'] = datetime.utcnow().isoformat()
        resultado['respuesta_completa'] = respuesta_texto
        
        # Guardar resultado en S3 (en carpeta de resultados)
        resultado_key = f"resultados/{key.split('/')[-1]}.json"
        s3.put_object(
            Bucket=bucket,
            Key=resultado_key,
            Body=json.dumps(resultado, indent=2, ensure_ascii=False),
            ContentType='application/json'
        )
        
        print(f"Resultado guardado en: {bucket}/{resultado_key}")
        print(f"Color detectado: {resultado['color']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(resultado, ensure_ascii=False)
        }
        
    except Exception as e:
        print(f"Error procesando imagen: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'archivo': key,
                'bucket': bucket
            })
        }


# Para testing local
if __name__ == "__main__":
    # Simular evento S3
    test_event = {
        'Records': [{
            's3': {
                'bucket': {'name': 'imagenes-sensores'},
                'object': {'key': 'test-image.jpg'}
            }
        }]
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
