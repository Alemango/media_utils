import psycopg2
import re
import pandas as pd

def ejecutar_consulta_con_parametros(query, db_params):
    """
    Ejecuta una consulta SQL, detectando y pidiendo valores para parámetros 
    con formato '&variable', y muestra los resultados en una tabla.

    Args:
        query (str): La cadena de la consulta SQL con placeholders.
        db_params (dict): Parámetros de conexión a la base de datos.
    """
    conn = None
    try:
        parametros_encontrados = re.findall(r'&\w+', query)
        
        parametros_unicos = sorted(list(dict.fromkeys(parametros_encontrados)))
        
        valores_parametros = []
        if parametros_unicos:
            print("--- Por favor, ingrese los valores para los siguientes parámetros ---")
            valores_dict = {}
            for param in parametros_unicos:
                valor = input(f"Ingrese el valor para {param.replace('&', '')}: ")
                valores_dict[param] = valor
            
            for param in parametros_encontrados:
                valores_parametros.append(valores_dict[param])

        query_segura = re.sub(r'&\w+', '%s', query)

        print("\nConectando a la base de datos y ejecutando la consulta...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        cursor.execute(query_segura, valores_parametros)
        
        if cursor.rowcount == 0:
            print("\nLa consulta se ejecutó correctamente, pero no arrojó resultados.")
            return

        columnas = [desc[0] for desc in cursor.description]
        
        registros = cursor.fetchall()
        
        df = pd.DataFrame(registros, columns=columnas)
        
        print("\n--- Resultados de la Consulta ---")
        print(df.to_string())

    except psycopg2.Error as e:
        print(f"\nOcurrió un error de base de datos: {e}")
    except Exception as e:
        print(f"\nOcurrió un error inesperado: {e}")
    finally:
        if conn:
            conn.close()
            print("\nConexión a la base de datos cerrada.")

# --- CONFIGURACIÓN ---

db_connection_params = {
    "dbname": "aml_project",
    "user": "mi19291",
    "password": "1ALEMAN9",
    "host": "localhost"
}

mi_query = """
SELECT
    T27.CD_CASO AS cdCaso,
    T27.FH_OPERACION AS fhOperacion,
    T27.CD_DIVISA AS nbMoneda,
    (SELECT NB_INS_MON FROM TSCA040_INST_MON WHERE CD_INS_MON = T27.CD_INST_MON::integer) AS nbInstrumento,
    (SELECT NB_OPERAC FROM TSCA029_TP_OPERAC WHERE TP_OPERAC = T27.TP_OPERACION) AS nbTipoTransaccion,
    T27.NU_CUENTA AS nuCuentas,
    T27.CD_FOLIO AS nbReferencia,
    T27.IM_MONTO AS imOperacion,
    T27.CD_OFICINA ||' - '||(SELECT NB_OFICINA FROM TSCA003_OFICINA WHERE CD_OFICINA = T27.CD_OFICINA) AS nbOficina,
    'RELEVANTES' AS tpOperacion
FROM
    TSCA027_OP_RELEV T27
JOIN TSCA016_CLIENTE T16
    ON (T16.CD_CLIENTE = T27.CD_CLIENTE AND T16.CD_CASO = T27.CD_CASO)
WHERE
    -- CAMBIO: AHORA ACEPTA UNA LISTA DE CASOS
    T27.CD_CASO IN (&CD_CASO_LIST)
    AND T27.CD_CLIENTE IN (
        SELECT CD_CLIENTE FROM TSCA013_ALERTA
        WHERE
            -- CAMBIO: SE AÑADE EL FILTRO DEL CASO AQUÍ PARA MAYOR EFICIENCIA
            CD_CASO IN (&CD_CASO_LIST)
            AND CD_ALERTA IN (&CD_ALERTA_LIST)
            AND CD_SISTEMA IN (&CD_SISTEMA_LIST)
    )
UNION ALL
SELECT
    T57.CD_CASO AS cdCaso,
    T57.FH_OPERACION AS fhOperacion,
    T57.CD_DIVISA AS nbMoneda,
    (SELECT NB_INS_MON FROM TSCA040_INST_MON WHERE CD_INS_MON = T57.CD_INST_MONE::integer) AS nbInstrumento,
    (SELECT NB_OPERAC FROM TSCA029_TP_OPERAC WHERE TP_OPERAC = T57.TP_OPERACION) AS nbTipoTransaccion,
    T57.NU_CUENTA AS nuCuentas,
    T57.CD_FOLIO AS nbReferencia,
    T57.IM_IMPORTE AS imOperacion,
    T57.CD_OFICINA ||' - '||(SELECT NB_OFICINA FROM TSCA003_OFICINA WHERE CD_OFICINA = T57.CD_OFICINA) AS nbOficina,
    'MEDIANAS' AS tpOperacion
FROM
    TSCA057_OP_MED T57
JOIN TSCA016_CLIENTE T16
    ON (T16.CD_CLIENTE = T57.CD_CLIENTE AND T16.CD_CASO = T57.CD_CASO)
WHERE
    -- CAMBIO: AHORA ACEPTA UNA LISTA DE CASOS
    T57.CD_CASO IN (&CD_CASO_LIST)
    AND T57.CD_CLIENTE IN (
        SELECT CD_CLIENTE FROM TSCA013_ALERTA
        WHERE
            -- CAMBIO: SE AÑADE EL FILTRO DEL CASO AQUÍ PARA MAYOR EFICIENCIA
            CD_CASO IN (&CD_CASO_LIST)
            AND CD_ALERTA IN (&CD_ALERTA_LIST)
            AND CD_SISTEMA IN (&CD_SISTEMA_LIST)
    );
"""

# --- EJECUCIÓN ---
if __name__ == "__main__":
    ejecutar_consulta_con_parametros(mi_query, db_connection_params)