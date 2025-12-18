"""
Script optimizado para ejecución desde n8n.
Retorna resultados en formato JSON para facilitar el procesamiento en workflows.
"""

import sys
import json
from pathlib import Path

# Agregar el directorio src al path
sys.path.insert(0, str(Path(__file__).parent))

from main import main

if __name__ == "__main__":
    try:
        # Ejecutar con fecha actual automática y retorno JSON
        resultado = main(usar_fecha_actual=True, retornar_json=True)
        
        # Imprimir JSON para que n8n lo capture
        if resultado:
            print(json.dumps(resultado, indent=2, ensure_ascii=False))
            
            # Exit code basado en éxito
            sys.exit(0 if resultado.get('exito', False) else 1)
        else:
            # Si no hay resultado, error
            error_result = {
                "exito": False,
                "error": "No se generó resultado del proceso"
            }
            print(json.dumps(error_result, indent=2, ensure_ascii=False))
            sys.exit(1)
            
    except Exception as e:
        error_result = {
            "exito": False,
            "error": str(e)
        }
        print(json.dumps(error_result, indent=2, ensure_ascii=False))
        sys.exit(1)

