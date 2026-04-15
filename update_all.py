import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
import socket
import requests.exceptions
import openpyxl
from datetime import datetime
import json
import os
import sys
import re
import unicodedata

if getattr(sys, 'frozen', False):
    RUTA_BASE = os.path.dirname(sys.executable)
else:
    RUTA_BASE = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(RUTA_BASE, 'config.json')
if not os.path.exists(CONFIG_PATH):
    config_default = {
        "creds_file": "credenciales.json",
        "excel_local": r"",
        "spreadsheet_id": "",
        "worksheet_name": "BASE"
    }
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config_default, f, indent=4, ensure_ascii=False)
    print(f"ℹ️  Archivo de configuración creado en: {CONFIG_PATH}")
    print("   Edítalo si necesitas cambiar rutas o IDs.")
    CONFIG = config_default
else:
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        CONFIG = json.load(f)

creds_file = CONFIG['creds_file']
if not os.path.isabs(creds_file):
    creds_file = os.path.join(RUTA_BASE, creds_file)
excel_local = CONFIG['excel_local']
spreadsheet_id = CONFIG['spreadsheet_id']
worksheet_name = CONFIG['worksheet_name']
ID_COL_INDEX = 0

scope = ['https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file(creds_file, scopes=scope)
client = gspread.authorize(creds)

print("📄 Archivos accesibles por la cuenta de servicio:")
spreadsheets = client.list_spreadsheet_files()
for file in spreadsheets:
    print(f"  - {file['name']} (ID: {file['id']})")

def con_reintentos(func, max_intentos=5, espera=3):
    for intento in range(1, max_intentos + 1):
        try:
            return func()
        except (
            requests.exceptions.ConnectionError,
            socket.gaierror,
            gspread.exceptions.APIError,
            OSError
        ) as e:
            if intento == max_intentos:
                print(f"❌ Falló después de {max_intentos} intentos: {e}")
                raise
            print(f"⚠️  Error de red (intento {intento}/{max_intentos}), reintentando en {espera}s...")
            time.sleep(espera)
            espera *= 2

MESES_ES = {
    'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
}

def normalizar_valor(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ''
    if v == '-':
        return ''
    if isinstance(v, (pd.Timestamp, datetime)):
        return v.strftime('%Y-%m-%d')
    if isinstance(v, float):
        if v == int(v):
            return str(int(v))
        return f"{v:.6f}".rstrip('0').rstrip('.')
    if isinstance(v, str):
        s = v.strip()
        s = re.sub(r'\s+', ' ', s)
        s = s.replace('\xa0', ' ')
        s = unicodedata.normalize('NFKC', s)
        if s == '-':
            return ''
        match_fecha_local = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', s)
        if match_fecha_local:
            dia, mes, anio = match_fecha_local.groups()
            try:
                fecha_obj = datetime(int(anio), int(mes), int(dia))
                return fecha_obj.strftime('%Y-%m-%d')
            except:
                pass
        match_fecha = re.match(r'^(\d{1,2})-([a-z]{3})-(\d{2})$', s.lower())
        if match_fecha:
            dia, mes_abr, anio = match_fecha.groups()
            mes_num = MESES_ES.get(mes_abr)
            if mes_num is not None:
                anio_completo = 2000 + int(anio) if int(anio) < 50 else 1900 + int(anio)
                try:
                    fecha_obj = datetime(anio_completo, mes_num, int(dia))
                    return fecha_obj.strftime('%Y-%m-%d')
                except:
                    pass
        if s.endswith('%'):
            try:
                num = float(s[:-1].replace(',', '.').replace(' ', '')) / 100.0
                return f"{num:.6f}".rstrip('0').rstrip('.')
            except:
                pass
        if re.match(r'^-?[\d\s\.]*,\d+$', s):
            limpio = s.replace('.', '').replace(' ', '').replace(',', '.')
            try:
                num = float(limpio)
                if num == int(num):
                    return str(int(num))
                return f"{num:.6f}".rstrip('0').rstrip('.')
            except:
                pass
        if re.match(r'^-?\d+\.\d+$', s):
            try:
                num = float(s)
                if num == int(num):
                    return str(int(num))
                return f"{num:.6f}".rstrip('0').rstrip('.')
            except:
                pass
        if re.match(r'^-?\d+$', s):
            return s
        return s
    return str(v).strip()

def valores_iguales(v1, v2, tolerancia=1e-6):
    if v1 == '-' and v2 == '':
        return True
    if v1 == '' and v2 == '-':
        return True
    try:
        n1 = float(str(v1).replace(',', '.')) if isinstance(v1, str) else float(v1)
        n2 = float(str(v2).replace(',', '.')) if isinstance(v2, str) else float(v2)
        if abs(n1 - n2) < tolerancia:
            return True
    except:
        pass
    return normalizar_valor(v1) == normalizar_valor(v2)

def serializar_valor(v):
    try:
        if pd.isna(v):
            return ''
    except (TypeError, ValueError):
        pass
    if hasattr(v, 'month'):
        return f"{v.day}/{v.month}/{v.year}"
    if isinstance(v, float) and v == int(v):
        return int(v)
    return v

def es_numero_valido(val):
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False

def limpiar_header(h):
    try:
        f = float(h.replace(',', '.'))
        if f == int(f):
            return str(int(f))
    except:
        pass
    return h

def leer_excel_con_valores(path):
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.worksheets[0]
    filas = list(ws.values)
    if not filas:
        return pd.DataFrame()

    raw_headers = filas[0]
    seen = {}
    clean_headers = []
    for h in raw_headers:
        if h is None or (isinstance(h, float) and pd.isna(h)):
            h = '__EMPTY__'
        elif isinstance(h, float) and h == int(h):
            h = str(int(h))
        else:
            h = str(h).strip()
        if h in seen:
            seen[h] += 1
            clean_headers.append(f"{h}__{seen[h]}")
        else:
            seen[h] = 0
            clean_headers.append(h)

    return pd.DataFrame(filas[1:], columns=clean_headers)

def aplicar_bordes(spreadsheet, sheet_id, start_row, num_filas, num_cols):
    if num_cols == 0 or num_filas == 0:
        return
    borde = {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}}
    con_reintentos(lambda: spreadsheet.batch_update({
        "requests": [{
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": start_row + num_filas,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols
                },
                "top": borde, "bottom": borde,
                "left": borde, "right": borde,
                "innerHorizontal": borde, "innerVertical": borde
            }
        }]
    }))

def extraer_clave_orden(id_str):
    """
    Convierte un ID (string) en una tupla (numero, sufijo) para ordenamiento natural.
    Ejemplos:
        "1029"    -> (1029, "")
        "1029-A"  -> (1029, "-A")
        "1029."   -> (1029, ".")
        "ABC123"  -> (9999999, "ABC123")  # Si no empieza con número, va al final
    """
    s = str(id_str).strip()
    if not s:
        return (float('inf'), '')
    # Buscar prefijo numérico al inicio
    match = re.match(r'^(\d+)(.*)$', s)
    if match:
        num = int(match.group(1))
        sufijo = match.group(2)
        return (num, sufijo)
    else:
        # No empieza con dígito -> mandar al final con número muy alto
        return (float('inf'), s)

def ordenar_y_reescribir_hoja(spreadsheet, worksheet, col_index=0):
    """
    Lee todos los datos de la hoja, los ordena según la columna ID (usando orden natural),
    y los vuelca de nuevo a la hoja, manteniendo cabecera fija.
    """
    print("📥 Leyendo datos para ordenamiento personalizado...")
    all_data = con_reintentos(lambda: worksheet.get_all_values())
    if len(all_data) <= 1:
        print("ℹ️  No hay suficientes datos para ordenar.")
        return

    cabecera = all_data[0]
    filas = all_data[1:]

    # Ordenar filas según la columna ID
    try:
        filas_ordenadas = sorted(
            filas,
            key=lambda fila: extraer_clave_orden(fila[col_index] if col_index < len(fila) else '')
        )
    except Exception as e:
        print(f"⚠️  Error al ordenar: {e}")
        return

    print("🧹 Limpiando hoja y escribiendo datos ordenados...")
    # Borrar contenido desde fila 2 hasta el final
    if len(filas) > 0:
        num_columnas = len(cabecera)
        rango_limpiar = f"A2:{gspread.utils.rowcol_to_a1(len(all_data), num_columnas)}"
        con_reintentos(lambda: worksheet.batch_clear([rango_limpiar]))

    # Escribir filas ordenadas
    if filas_ordenadas:
        con_reintentos(lambda: worksheet.update(
            f"A2:{gspread.utils.rowcol_to_a1(len(filas_ordenadas)+1, len(cabecera))}",
            filas_ordenadas,
            value_input_option='USER_ENTERED'
        ))

    print("✅ Datos ordenados y escritos correctamente.")

def aplicar_bordes_a_todo(spreadsheet, worksheet):
    """
    Aplica bordes a todas las celdas con datos (cabecera + filas de datos).
    """
    all_values = con_reintentos(lambda: worksheet.get_all_values())
    num_filas = len(all_values)
    if num_filas == 0:
        return
    num_columnas = len(all_values[0]) if num_filas > 0 else 0
    if num_columnas == 0:
        return

    sheet_id = worksheet.id
    borde = {
        "style": "SOLID",
        "width": 1,
        "color": {"red": 0, "green": 0, "blue": 0}
    }

    con_reintentos(lambda: spreadsheet.batch_update({
        "requests": [{
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": num_filas,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_columnas
                },
                "top": borde,
                "bottom": borde,
                "left": borde,
                "right": borde,
                "innerHorizontal": borde,
                "innerVertical": borde
            }
        }]
    }))
    print(f"🖊️  Bordes aplicados a todo el rango ({num_filas} filas x {num_columnas} columnas).")

print("🔌 Conectando con Google Sheets...")
spreadsheet = con_reintentos(lambda: client.open_by_key(spreadsheet_id))
worksheet = con_reintentos(lambda: spreadsheet.worksheet(worksheet_name))

print("📥 Leyendo datos remotos...")
all_values = con_reintentos(lambda: worksheet.get_all_values())

if len(all_values) <= 1:
    df_remoto = pd.DataFrame()
    filas_remotas_actuales = max(len(all_values), 1)
else:
    filas_remotas_actuales = len(all_values)
    raw_headers = all_values[0]
    seen = {}
    clean_headers = []
    for h in raw_headers:
        h = limpiar_header(h.strip()) if h.strip() else '__EMPTY__'
        if h in seen:
            seen[h] += 1
            clean_headers.append(f"{h}__{seen[h]}")
        else:
            seen[h] = 0
            clean_headers.append(h)
    df_remoto = pd.DataFrame(all_values[1:], columns=clean_headers)

print("📂 Leyendo Excel local...")
df_local = leer_excel_con_valores(excel_local)

if df_local.empty:
    print("❌ El Excel local está vacío")
    exit(1)

valores_local = df_local.iloc[:, ID_COL_INDEX].dropna()
ids_local_numericos = set()
ids_no_numericos = set()
for v in valores_local:
    v_str = str(v).strip()
    if es_numero_valido(v_str):
        ids_local_numericos.add(v_str)
    else:
        ids_no_numericos.add(v_str)

print(f"📊 IDs numéricos en local: {len(ids_local_numericos)}")
if ids_no_numericos:
    print(f"⚠️  IDs no numéricos ignorados: {ids_no_numericos}")

if not df_remoto.empty:
    valores_remoto = df_remoto.iloc[:, ID_COL_INDEX].dropna()
    ids_remoto = {str(v).strip() for v in valores_remoto if es_numero_valido(v)}
else:
    ids_remoto = set()

print(f"📊 IDs numéricos en remoto: {len(ids_remoto)}")

nuevas_keys = ids_local_numericos - ids_remoto
keys_existentes = ids_local_numericos & ids_remoto

if keys_existentes and not df_remoto.empty:
    print(f"🔍 Comparando {len(keys_existentes)} filas existentes...")

    id_col_remoto = df_remoto.iloc[:, ID_COL_INDEX].astype(str).str.strip()
    fila_por_id = {
        id_val: idx + 2
        for idx, id_val in enumerate(id_col_remoto)
        if es_numero_valido(id_val)
    }

    actualizaciones = []

    for key in keys_existentes:
        fila_local = df_local[
            df_local.iloc[:, ID_COL_INDEX].astype(str).str.strip() == key
        ]
        if fila_local.empty:
            continue

        valores_local_raw = fila_local.iloc[0].tolist()
        valores_remoto_raw = df_remoto[id_col_remoto == key].iloc[0].tolist()

        diferente = False
        for l_val, r_val in zip(valores_local_raw, valores_remoto_raw):
            if not valores_iguales(l_val, r_val):
                diferente = True
                
        if diferente:
            row_num = fila_por_id.get(key)
            if row_num:
                valores_serializados = [serializar_valor(v) for v in valores_local_raw]
                actualizaciones.append((row_num, valores_serializados))

    if actualizaciones:
        print(f"✏️  Actualizando {len(actualizaciones)} filas modificadas...")

        LOTE = 50
        for i in range(0, len(actualizaciones), LOTE):
            lote = actualizaciones[i:i + LOTE]
            cell_updates = []
            for row_num, valores in lote:
                for col_idx, val in enumerate(valores):
                    celda = gspread.utils.rowcol_to_a1(row_num, col_idx + 1)
                    cell_updates.append({'range': celda, 'values': [[val]]})

            con_reintentos(
                lambda upd=cell_updates: worksheet.batch_update(upd, value_input_option='USER_ENTERED')
            )
            print(f"  ✅ Lote {i // LOTE + 1} actualizado ({len(lote)} filas)")
    else:
        print("✅ Ninguna fila existente necesita actualización.")
else:
    print("ℹ️  No hay filas existentes para comparar.")

if not nuevas_keys:
    print("✅ No hay filas nuevas (IDs numéricos).")
else:
    print(f"🆕 Se encontraron {len(nuevas_keys)} filas nuevas, subiendo...")

    df_nuevas = df_local[
        df_local.iloc[:, ID_COL_INDEX].astype(str).str.strip().isin(nuevas_keys)
    ]

    filas = [
        [serializar_valor(celda) for celda in fila]
        for fila in df_nuevas.values.tolist()
    ]

    print("📤 Subiendo filas a Sheets...")
    con_reintentos(
        lambda: worksheet.append_rows(filas, value_input_option='USER_ENTERED')
    )

    print("🖊️  Aplicando bordes a filas nuevas...")
    sheet_id = worksheet.id
    num_cols = len(filas[0]) if filas else 0
    aplicar_bordes(spreadsheet, sheet_id, filas_remotas_actuales, len(filas), num_cols)
    filas_remotas_actuales += len(filas)  # Actualizar total de filas


print("🔃 Organizando hoja por ID con orden natural...")
ordenar_y_reescribir_hoja(spreadsheet, worksheet, col_index=ID_COL_INDEX)
aplicar_bordes_a_todo(spreadsheet, worksheet)

print("✅ Proceso completado.")