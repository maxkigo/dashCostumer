import pandas as pd
import streamlit as st
import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
import datetime
import tempfile
import os
import plotly.express as px
import streamlit.components.v1 as components

# Set Streamlit page configuration
st.set_page_config(layout="wide", page_title="Kigo Costumer Service", page_icon="decorations/kigo-icon-adaptative.png")

# Display the logo at the top of the page
st.markdown(
    """
    <div style="text-align: center;">
        <img src="https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png" 
        alt="Imagen al inicio" style="width: 25%; max-width: 30%; height: auto;">
    </div>
    """,
    unsafe_allow_html=True
)

dialogflow_html = """
<link rel="stylesheet" href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css">
<script src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"></script>
<df-messenger
  project-id="kigo-ai-customer-support"
  agent-id="a6f52763-f642-4c75-838a-94402118179d"
  language-code="es"
  max-query-length="-1">
  <df-messenger-chat
    chat-title="KIGO AI Customer Support">
  </df-messenger-chat>
</df-messenger>
<style>
  df-messenger {
    z-index: 999;
    position: fixed;
    --df-messenger-font-color: #000;
    --df-messenger-font-family: Google Sans;
    --df-messenger-chat-background: #f3f6fc;
    --df-messenger-message-user-background: #d3e3fd;
    --df-messenger-message-bot-background: #fff;
    bottom: 0;
    right: 0;
    top: 0;
    width: 350px;
  }
</style>
"""

# Load the private key from Streamlit secrets
pem_key = st.secrets['pem']['private_key']

# Create a temporary file to store the private key
with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_key_file:
    temp_key_file.write(pem_key)
    temp_key_file_path = temp_key_file.name

# Load the private key from the temporary file
try:
    mypkey = paramiko.RSAKey.from_private_key_file(temp_key_file_path)
    print("Private key loaded successfully.")
finally:
    # Clean up the temporary file after loading the private key
    os.remove(temp_key_file_path)

# Load environment variables for the SSH tunnel and database connection
sql_hostname = st.secrets["database"]["sql_hostname"]
sql_username = st.secrets["database"]["sql_username"]
sql_password = st.secrets["database"]["sql_password"]
sql_main_database = st.secrets["database"]["sql_main_database"]
sql_port = st.secrets["database"]["sql_port"]
ssh_host = st.secrets["ssh"]["ssh_host"]
ssh_user = st.secrets["ssh"]["ssh_user"]
ssh_port = st.secrets["ssh"]["ssh_port"]


# Conexión estándar (sin SSH)
@st.cache_resource
def create_rds_connection():
    try:
        return pymysql.connect(
            host=st.secrets["rds_geosek"]["host_geosek"],
            user=st.secrets["rds_geosek"]["user_geosek"],
            password=st.secrets["rds_geosek"]["pass_geosek"],
            port=st.secrets["rds_geosek"]["port_geosek"]
        )
    except Exception as e:
        st.error(f"No se pudo establecer la conexión con RDS: {str(e)}")
        return None


# Cache the SSH tunnel
@st.cache_resource
def create_ssh_tunnel():
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(sql_hostname, sql_port)
    )
    tunnel.start()
    return tunnel


# Cache the database connection
@st.cache_resource
def create_db_connection(_tunnel):
    conn = pymysql.connect(
        host='127.0.0.1',
        user=sql_username,
        passwd=sql_password,
        db=sql_main_database,
        port=_tunnel.local_bind_port
    )
    return conn


# Create SSH tunnel and database connection
try:
    tunnel = create_ssh_tunnel()
    conn = create_db_connection(tunnel)
except Exception as e:
    st.error(f"No se pudo establecer la conexión con la base de datos: {str(e)}")
    st.stop()

coldate, colphone = st.columns(2)

with coldate:
    # Date input for the query
    d = st.date_input("Fecha de Consulta",
                      value=[datetime.date(2024, 1, 1), datetime.date.today()],
                      min_value=datetime.date(2024, 1, 1),
                      max_value=datetime.date.today(),
                      format="DD/MM/YYYY")

    if len(d) == 2:
        start_date = f'{d[0]} 00:00:00'
        end_date = f'{d[1]} 23:59:59'
    else:
        st.warning("Por favor seleccione un rango de fechas válido")
        st.stop()

with colphone:
    default_phone = 000000000
    # Phone number for the query
    number = st.number_input("Ingresar el número del Usuario:",
                             value=default_phone,
                             step=1,
                             format="%d")


# Obtain the userid from the phone number
@st.cache_data
def useridLocate(phonenumber, _conn):
    if not phonenumber or phonenumber == default_phone:
        return None

    try:
        query = f'''
        SELECT userid
        FROM CARGOMOVIL_PD.SEC_USER_PROFILE
        WHERE phonenumber = '{phonenumber}';
        '''
        df = pd.read_sql_query(query, _conn)
        return df['userid'].iloc[0] if not df.empty else None
    except Exception as e:
        st.error(f"Error al buscar el usuario: {str(e)}")
        return None


if number and number != default_phone:
    userid = useridLocate(number, conn)

    if userid is None:
        st.error("Usuario no encontrado. Por favor verifique el número telefónico.")
        st.stop()
    else:
        st.success(f"Usuario encontrado: {userid}")
else:
    st.warning("Por favor ingrese un número telefónico válido")
    st.stop()


@st.cache_data
def accountUser(userid, _conn):
    try:
        query = f'''
            SELECT UP.firstname AS nombre, UP.lastname AS apellido, UP.phonenumber AS telefono, 
                   UP.facebookemail AS email, CDU.funds AS fondos, CDU.currency AS moneda
            FROM CARGOMOVIL_PD.CDX_USER_ACCOUNT CDU
            JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON CDU.userid = UP.userid
            WHERE UP.userid = {userid};
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener información de la cuenta: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def cardsUser(userid, _conn):
    try:
        query = f'''
        SELECT SQ_UC.brand AS 'marca', SQ_UC.last_4, SQ_UC.card_status AS status, SQ_UC.creation_date 
        FROM CARGOMOVIL_PD.uc_users_cards SQ_UC
        JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON SQ_UC.user_id = UP.userid
        WHERE UP.userid = {userid};
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener información de tarjetas: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def vehicleUser(userid, _conn):
    try:
        query = f'''
        SELECT V.licenseplate, (CASE WHEN V.status = 1 THEN 'active' ELSE 'inactive' END) AS status,
        O.modelname AS modelo, U.brandname AS marca
        FROM CARGOMOVIL_PD.PKM_VEHICLE V
        JOIN CARGOMOVIL_PD.PKM_VEHICLE_MODELS_CAT O ON V.modelid = O.id 
        JOIN CARGOMOVIL_PD.PKM_VEHICLE_BRANDS_CAT U ON O.brandid = U.id
        WHERE V.ownerid = {userid}
        ORDER BY status;
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener información de vehículos: {str(e)}")
        return pd.DataFrame()

#
@st.cache_data
def obtener_usuarios_rds(number, _conn):
    """
    Fetches user data from a remote RDS database using a provided connection and user number.

    This function runs a database query to retrieve user records where the 'user'
    field matches the provided number. It returns the query results or an empty list
    if an error occurs during the process.

    :param number: User identifier to filter the query.
    :type number: str
    :param _conn: Database connection object used for executing the query.
    :type _conn: Any
    :return: A list containing the queried user data or an empty list in case of an error.
    :rtype: list
    """
    try:
        with _conn.cursor() as cursor:
            query = f'''
            SELECT L.user AS 'Teléfono', R.alias AS 'Proyecto', L.date AS 'Fechas Acceso', R.name AS 'Acceso'
            FROM RASPIS.log_sek L
            JOIN RASPIS.raspis R ON L.QR = R.qr
            WHERE user LIKE '{number}'
            LIMIT 1
            '''

            cursor.execute(query)
            result = cursor.fetchall()
        return result
    except Exception as e:
        st.error(f"Error al consultar datos en RDS: {str(e)}")
        return []

# Establecer la conexión a RDS
rds_connection = create_rds_connection()

if rds_connection:
    datos_usuarios = obtener_usuarios_rds(number, rds_connection)

    if datos_usuarios:
        st.data_editor(datos_usuarios)
    else:
        st.warning("No se encontraron datos o ocurrió un error al ejecutar la consulta")



@st.cache_data
def lastEdOperations(userid, _conn, startDate, endDate):
    try:
        query = f'''
            SELECT U.phonenumber AS 'Teléfono', Z.parkinglotname AS 'Estacionamiento',
             (CASE WHEN PQR.status = 1 THEN 'Open Cicle' ELSE 'Close Cicle' END) AS status,
                   T.subtotal, T.tax, T.fee, T.total, T.qrcode,  
                   CASE
                    WHEN T.paymentType = 1 THEN 'NAP'
                    WHEN T.paymentType = 2 THEN 'SMS'
                    WHEN T.paymentType = 3 THEN 'TC/TD'
                    WHEN T.paymentType = 4 THEN 'SALDO'
                    WHEN T.paymentType = 5 THEN 'ATM'
                    ELSE ''
                    END AS 'Método de Pago',
                   TIMESTAMPDIFF(MINUTE, CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City'), 
                             CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City')) AS 'Minutos Pagados',
                   CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City') AS 'Entrada', 
                   CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City') AS 'Salida',
                   CONVERT_TZ(T.paymentdate, 'UTC', 'America/Mexico_City') AS 'Fecha de Pago',
                   (CASE WHEN PQR.isvalidated = 1 THEN 'Validated'
            WHEN PQR.isvalidated = 0 THEN 'No Validated' ELSE NULL END) AS 'Promoción Aplicada',
            PCAT.description AS 'Tipo de Promoción',
            T.transactionid
            FROM CARGOMOVIL_PD.PKM_SMART_QR_TRANSACTIONS T
            JOIN CARGOMOVIL_PD.PKM_SMART_QR_CHECKIN I ON T.checkinid = I.id
            JOIN CARGOMOVIL_PD.PKM_SMART_QR_CHECKOUT O ON T.checkoutid = O.id
            JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON T.parkinglotid = Z.id
            JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid
            LEFT JOIN CARGOMOVIL_PD.PKM_SMART_QR_PROMOTIONS PP ON T.qrcodeid = PP.qrcodeid
            LEFT JOIN CARGOMOVIL_PD.GEN_PROMOTION_TYPE_CAT PCAT ON PP.promotionid = PCAT.id
            LEFT JOIN CARGOMOVIL_PD.PKM_SMART_QR PQR ON T.qrcodeid = PQR.id
            WHERE U.userid = {userid} AND T.paymentdate BETWEEN '{startDate}' AND '{endDate}'
            ORDER BY T.paymentdate DESC;
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener operaciones en ED: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def movementsUser(phonenumber, _conn):
    try:
        query = f'''
        CALL usp_metabase_user_account_movements(
          2,
         NULL,
         NULL,
         '{phonenumber}',
         NULL);
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener movimientos: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def lastPVOperations(userid, _conn, startDate, endDate):
    try:
        query = f'''
        SELECT U.phonenumber AS 'Teléfono', 
        CASE
                    WHEN T.paymentType = 1 THEN 'NAP'
                    WHEN T.paymentType = 2 THEN 'SMS'
                    WHEN T.paymentType = 3 THEN 'TC/TD'
                    WHEN T.paymentType = 4 THEN 'SALDO'
                    WHEN T.paymentType = 5 THEN 'ATM'
                    ELSE ''
                    END AS 'Método de Pago',
        T.licenseplate AS Placa, Z.name AS Parquimetro, T.totalamount AS Pago, T.transactionid
        FROM CARGOMOVIL_PD.PKM_TRANSACTION T
        JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid
        JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z ON T.zoneid = Z.id
        WHERE U.userid = {userid} AND T.date BETWEEN '{startDate}' AND '{endDate}'
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener operaciones en PV: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def pensionsUser(userid, _conn):
    try:
        query = f'''
        SELECT Z.parkinglotname, pp.phonenumber, pp.startdate, pp.enddate, pp.status, pp.description
        FROM CARGOMOVIL_PD.PKM_PARKING_LOT_LODGINGS pp
        JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON pp.parkinglotid = Z.id
        WHERE pp.userid = {userid}
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener información de pensiones: {str(e)}")
        return pd.DataFrame()


@st.cache_data
def errorsUser(userid, _conn, start_date, end_date):
    try:
        query = f'''
        SELECT 
        e.id AS log_id,
        p.parkinglotname AS parking_name,
        e.userid AS user_id,
        JSON_UNQUOTE(JSON_EXTRACT(e.metadata, '$.user.username')) AS username,
        e.eventtype AS event_type,
        JSON_UNQUOTE(JSON_EXTRACT(e.metadata, '$.qrCode')) AS qrcode,
        JSON_EXTRACT(e.metadata, '$.error.code') AS error_code,
        JSON_EXTRACT(e.metadata, '$.error.status') AS error_status,
        JSON_UNQUOTE(JSON_EXTRACT(e.metadata, '$.error.message')) AS error_message,
        JSON_EXTRACT(e.metadata, '$.gateId') AS gate_id,
        e.eventtimestamp AS error_date
        FROM 
            CARGOMOVIL_PD.PKM_PARKING_LOT_EVENTS e
        JOIN 
            CARGOMOVIL_PD.PKM_PARKING_LOT_CAT p ON e.parkinglotid = p.id
        WHERE 
            e.eventtype LIKE '%error%' 
            AND e.userid = {userid}
            AND e.eventtimestamp BETWEEN '{start_date}' AND '{end_date}'; 
        '''
        return pd.read_sql_query(query, _conn)
    except Exception as e:
        st.error(f"Error al obtener registros de errores: {str(e)}")
        return pd.DataFrame()


# Mostrar información solo si tenemos un userid válido
if userid:
    colacount, colcards = st.columns(2)

    with colacount:
        st.header("Wallets del Usuario")
        account_data = accountUser(userid, conn)
        if not account_data.empty:
            st.data_editor(
                account_data,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed"
            )
        else:
            st.warning("No se encontró información de wallet para este usuario")

    with colcards:
        st.header("Tarjetas del Usuario")
        cards_data = cardsUser(userid, conn)
        if not cards_data.empty:
            st.data_editor(
                cards_data,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed"
            )
        else:
            st.warning("No se encontraron tarjetas registradas para este usuario")

    colveh, colpensions = st.columns(2)

    with colveh:
        st.header("Vehículos del Usuario")
        vehicles_data = vehicleUser(userid, conn)
        if not vehicles_data.empty:
            st.data_editor(vehicles_data)
        else:
            st.warning("No se encontraron vehículos registrados para este usuario")

    with colpensions:
        st.header("Pensiones del Usuario")
        pensions_data = pensionsUser(userid, conn)
        if not pensions_data.empty:
            st.data_editor(pensions_data)
        else:
            st.warning("No se encontraron pensiones registradas para este usuario")

    st.header("Operaciones del Usuario en ED")
    ed_operations = lastEdOperations(userid, conn, start_date, end_date)
    if not ed_operations.empty:
        st.data_editor(ed_operations)
    else:
        st.warning(f"No se encontraron operaciones en ED para este usuario en el rango de fechas seleccionado")

    st.header("Operaciones del Usuario en PV")
    pv_operations = lastPVOperations(userid, conn, start_date, end_date)
    if not pv_operations.empty:
        st.data_editor(pv_operations)
    else:
        st.warning(f"No se encontraron operaciones en PV para este usuario en el rango de fechas seleccionado")

    st.header("Movimientos del Usuario")
    movements_data = movementsUser(number, conn)
    if not movements_data.empty:
        st.data_editor(movements_data)

        # Gráfico de movimientos
        fig = px.line(
            movements_data,
            x="TRANSACTIOND_DATE",
            y="FINAL_FUNDS",
            title="Wallet del Usuario",
            labels={"TRANSACTIOND_DATE": "Fecha de Movimiento", "FINAL_FUNDS": "Fondos Finales"},
            height=400
        )
        fig.update_traces(
            marker_color=[
                "green" if funds >= 0 else "crimson" for funds in movements_data["FINAL_FUNDS"]
            ]
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No se encontraron movimientos para este usuario")

    st.header("Errores del Usuario")
    errors_data = errorsUser(userid, conn, start_date, end_date)
    if not errors_data.empty:
        st.data_editor(errors_data)
    else:
        st.warning("No se encontraron registros de errores para este usuario en el rango de fechas seleccionado")

# Call to Dialogflow Agent
components.html(dialogflow_html, height=700, scrolling=True)


# Close the connection and tunnel when the app stops
def cleanup():
    try:
        if 'conn' in globals() and conn:
            conn.close()
        if 'tunnel' in globals() and tunnel:
            tunnel.stop()
    except:
        pass


# Register the cleanup function to run when the app stops
import atexit

atexit.register(cleanup)