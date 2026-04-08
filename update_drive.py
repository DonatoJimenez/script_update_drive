import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import time
import socket
import requests.exceptions
import openpyxl

# Configuración
creds_file = r'C:\donato\scripts\excel_diario\scriptsPython\API_key\credenciales.json'
excel_local = r'\\DESKTOP-MVTII6G\Carpeta Compartida\PLANEACION 2024\BASES PLANEACION\1) BASE PLANEACION PROVISPOL.xlsx'
worksheet_name = 'BASE'
ID_COL_INDEX = 0

# AUTENTICACIÓN
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

def serializar_valor(v):
    """Convierte valores a formato apto para Google Sheets (texto, números, fechas)."""
    try:
        if pd.isna(v):
            return ''
    except (TypeError, ValueError):
        pass
    if hasattr(v, 'month'):  # es una fecha (datetime)
        return f"{v.day}/{v.month}/{v.year}"
    if isinstance(v, float) and v == int(v):
        return int(v)
    return v  # texto o cualquier otro tipo se deja igual

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
    
    datos = filas[1:]
    return pd.DataFrame(datos, columns=clean_headers)

print("🔌 Conectando con Google Sheets...")
spreadsheet = con_reintentos(
    lambda: client.open_by_key('1OAokMgYYgNOXY79PlaPy70PHv6mXN__f3kq0fpi_XfY')
)
worksheet = con_reintentos(
    lambda: spreadsheet.worksheet(worksheet_name)
)

print("📥 Leyendo datos remotos...")
all_values = con_reintentos(lambda: worksheet.get_all_values())

def limpiar_header(h):
    try:
        f = float(h.replace(',', '.'))
        if f == int(f):
            return str(int(f))
    except:
        pass
    return h

if len(all_values) <= 1:
    df_remoto = pd.DataFrame()
    filas_remotas_actuales = 1
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

# --- FILTRAR IDs NUMÉRICOS (para decidir qué filas subir) ---
def es_numero_valido(val):
    try:
        float(val)
        return True
    except (ValueError, TypeError):
        return False

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
    print(f"⚠️  IDs no numéricos ignorados (no se subirán): {ids_no_numericos}")

if not df_remoto.empty:
    valores_remoto = df_remoto.iloc[:, ID_COL_INDEX].dropna()
    ids_remoto = {str(v).strip() for v in valores_remoto if es_numero_valido(v)}
else:
    ids_remoto = set()

print(f"📊 IDs numéricos en remoto: {len(ids_remoto)}")

nuevas_keys = ids_local_numericos - ids_remoto

if not nuevas_keys:
    print("✅ No hay filas nuevas (IDs numéricos).")
    exit(0)

print(f"🆕 Se encontraron {len(nuevas_keys)} filas nuevas con IDs numéricos, subiendo...")

# Filtrar filas nuevas (solo las que tengan ID numérico)
df_nuevas = df_local[df_local.iloc[:, ID_COL_INDEX].astype(str).isin(nuevas_keys)]

# Convertir a lista de listas usando serializar_valor (conserva texto y fechas)
filas = [
    [serializar_valor(celda) for celda in fila]
    for fila in df_nuevas.values.tolist()
]

print("📤 Subiendo filas a Sheets...")
con_reintentos(
    lambda: worksheet.append_rows(filas, value_input_option='USER_ENTERED')
)

print("🖊️  Aplicando bordes...")
sheet_id = worksheet.id
num_filas_nuevas = len(filas)
num_cols = len(filas[0]) if filas else 0
start_row = filas_remotas_actuales

if num_cols > 0:
    borde = {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}}
    con_reintentos(lambda: spreadsheet.batch_update({
        "requests": [{
            "updateBorders": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": start_row + num_filas_nuevas,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols
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
else:
    print("⚠️  No se aplicaron bordes porque no hay columnas que subir.")

print("✅ Proceso completado.")