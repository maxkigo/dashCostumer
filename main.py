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
import atexit

# Set Streamlit page configuration
st.set_page_config(layout="wide", page_title="Kigo Customer Service", page_icon="decorations/kigo-icon-adaptative.png")

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
temp_key_file_path = None # Initialize to None

# Create a temporary file to store the private key
try:
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_key_file:
        temp_key_file.write(pem_key)
        temp_key_file_path = temp_key_file.name

    # Load the private key from the temporary file
    mypkey = paramiko.RSAKey.from_private_key_file(temp_key_file_path)
    # print("Private key loaded successfully.") # Optional: for debugging
except Exception as e:
    st.error(f"Error loading private key: {e}")
    mypkey = None # Ensure mypkey is None if loading fails
    st.stop()
finally:
    # Clean up the temporary file after loading the private key
    if temp_key_file_path and os.path.exists(temp_key_file_path):
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
    if not mypkey: # Don't try to create tunnel if key loading failed
        st.error("Cannot create SSH tunnel: Private key not loaded.")
        return None
    try:
        tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=mypkey,
            remote_bind_address=(sql_hostname, sql_port)
        )
        tunnel.start()
        return tunnel
    except Exception as e:
        st.error(f"Error creating SSH tunnel: {e}")
        return None


# Cache the database connection
@st.cache_resource
def create_db_connection(_tunnel):
    if not _tunnel: # Don't try to connect if tunnel failed
        st.error("Cannot create DB connection: SSH tunnel not available.")
        return None
    try:
        conn = pymysql.connect(
            host='127.0.0.1',
            user=sql_username,
            passwd=sql_password,
            db=sql_main_database,
            port=_tunnel.local_bind_port
        )
        return conn
    except Exception as e:
        st.error(f"Error creating database connection via tunnel: {e}")
        return None

# Create SSH tunnel and database connection
tunnel = None
conn = None
try:
    if mypkey: # Only proceed if private key was loaded
        tunnel = create_ssh_tunnel()
        if tunnel:
            conn = create_db_connection(tunnel)
        else:
            st.error("No se pudo establecer la conexión SSH.")
            st.stop()
    else:
        st.error("No se pudo cargar la llave privada para la conexión SSH.")
        st.stop()

except Exception as e:
    st.error(f"No se pudo establecer la conexión con la base de datos: {str(e)}")
    st.stop()

# --- Define Tabs ---
tab1_title = "Panel de Servicio al Cliente"
tab2_title = "Agente KIGO AI"
tab1, tab2 = st.tabs([tab1_title, tab2_title])

# --- Tab 1: Customer Service Dashboard ---
with tab1:
    if not conn:
        st.error("La conexión a la base de datos no está disponible para el panel de servicio.")
    else:
        coldate, colphone = st.columns(2)
        userid = None # Initialize userid

        with coldate:
            d = st.date_input("Fecha de Consulta",
                              value=[datetime.date(2024, 1, 1), datetime.date.today()],
                              min_value=datetime.date(2024, 1, 1),
                              max_value=datetime.date.today(),
                              format="DD/MM/YYYY",
                              key="date_input_tab1") # Unique key for date input

            if len(d) == 2:
                start_date = f'{d[0]} 00:00:00'
                end_date = f'{d[1]} 23:59:59'
            else:
                st.warning("Por favor seleccione un rango de fechas válido")
                start_date = None
                end_date = None
                # st.stop() # Avoid stopping if only this tab is affected

        with colphone:
            default_phone = 000000000
            number = st.number_input("Ingresar el número del Usuario:",
                                     value=default_phone,
                                     step=1,
                                     format="%d",
                                     key="phone_input_tab1") # Unique key for number input

        @st.cache_data
        def useridLocate(phonenumber, _conn_loc):
            if not phonenumber or phonenumber == default_phone:
                return None
            if not _conn_loc: # Check if connection is valid
                st.error("Conexión a base de datos no disponible para buscar userid.")
                return None
            try:
                query = f'''
                SELECT userid
                FROM CARGOMOVIL_PD.SEC_USER_PROFILE
                WHERE phonenumber = '{phonenumber}';
                '''
                df = pd.read_sql_query(query, _conn_loc)
                return df['userid'].iloc[0] if not df.empty else None
            except Exception as e:
                st.error(f"Error al buscar el usuario: {str(e)}")
                return None

        if number and number != default_phone and start_date and end_date:
            userid = useridLocate(number, conn)
            if userid is None:
                st.error("Usuario no encontrado. Por favor verifique el número telefónico.")
            else:
                st.success(f"Usuario encontrado: {userid}")
        elif not (number and number != default_phone):
            st.warning("Por favor ingrese un número telefónico válido.")
        elif not (start_date and end_date):
            st.warning("Por favor seleccione un rango de fechas válido.")


        @st.cache_data
        def accountUser(userid_loc, _conn_loc):
            if not _conn_loc: return pd.DataFrame()
            try:
                query = f'''
                    SELECT UP.firstname AS nombre, UP.lastname AS apellido, UP.phonenumber AS telefono, 
                           UP.facebookemail AS email, CDU.funds AS fondos, CDU.currency AS moneda
                    FROM CARGOMOVIL_PD.CDX_USER_ACCOUNT CDU
                    JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON CDU.userid = UP.userid
                    WHERE UP.userid = {userid_loc};
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener información de la cuenta: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def cardsUser(userid_loc, _conn_loc):
            if not _conn_loc: return pd.DataFrame()
            try:
                query = f'''
                SELECT SQ_UC.brand AS 'marca', SQ_UC.last_4, SQ_UC.card_status AS status, SQ_UC.creation_date 
                FROM CARGOMOVIL_PD.uc_users_cards SQ_UC
                JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON SQ_UC.user_id = UP.userid
                WHERE UP.userid = {userid_loc};
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener información de tarjetas: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def vehicleUser(userid_loc, _conn_loc):
            if not _conn_loc: return pd.DataFrame()
            try:
                query = f'''
                SELECT V.licenseplate, (CASE WHEN V.status = 1 THEN 'active' ELSE 'inactive' END) AS status,
                O.modelname AS modelo, U.brandname AS marca
                FROM CARGOMOVIL_PD.PKM_VEHICLE V
                JOIN CARGOMOVIL_PD.PKM_VEHICLE_MODELS_CAT O ON V.modelid = O.id 
                JOIN CARGOMOVIL_PD.PKM_VEHICLE_BRANDS_CAT U ON O.brandid = U.id
                WHERE V.ownerid = {userid_loc}
                ORDER BY status;
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener información de vehículos: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def obtener_usuarios_rds(user_number, _rds_conn):
            if not _rds_conn:
                st.error("Conexión a RDS no disponible.")
                return []
            try:
                with _rds_conn.cursor() as cursor:
                    query = f'''
                    SELECT L.user AS 'Teléfono', R.alias AS 'Proyecto', L.date AS 'Fechas Acceso', R.name AS 'Acceso'
                    FROM RASPIS.log_sek L
                    JOIN RASPIS.raspis R ON L.QR = R.qr
                    WHERE user LIKE '{user_number}'
                    LIMIT 1
                    '''
                    cursor.execute(query)
                    result = cursor.fetchall()
                return result
            except Exception as e:
                st.error(f"Error al consultar datos en RDS: {str(e)}")
                return []

        rds_connection = create_rds_connection()
        if rds_connection and number and number != default_phone:
            datos_usuarios = obtener_usuarios_rds(number, rds_connection)
            if datos_usuarios:
                st.subheader("Accesos del Usuario (RDS)")
                st.data_editor(datos_usuarios, hide_index=True, use_container_width=True)
            # else: # Commenting out to avoid too many warnings if user not found here
                # st.warning("No se encontraron datos de acceso en RDS para este número o ocurrió un error.")
            # Close RDS connection if it's not managed by cache_resource for session-long persistence
            if rds_connection and not hasattr(create_rds_connection, '__wrapped__'): # Check if not cached
                 rds_connection.close()

        @st.cache_data
        def lastEdOperations(userid_loc, _conn_loc, startDate_loc, endDate_loc):
            if not _conn_loc: return pd.DataFrame()
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
                    WHERE U.userid = {userid_loc} AND T.paymentdate BETWEEN '{startDate_loc}' AND '{endDate_loc}'
                    ORDER BY T.paymentdate DESC;
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener operaciones en ED: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def movementsUser(phonenumber_loc, _conn_loc):
            if not _conn_loc: return pd.DataFrame()
            try:
                # Ensure phonenumber_loc is a string for the SQL query
                query = f'''
                CALL usp_metabase_user_account_movements(
                  2,
                 NULL,
                 NULL,
                 '{str(phonenumber_loc)}', 
                 NULL);
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener movimientos: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def lastPVOperations(userid_loc, _conn_loc, startDate_loc, endDate_loc):
            if not _conn_loc: return pd.DataFrame()
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
                T.licenseplate AS Placa, Z.name AS Parquimetro, T.totalamount AS Pago, T.transactionid,
                CONVERT_TZ(T.date, 'UTC', 'America/Mexico_City') AS 'Fecha de Transacción'
                FROM CARGOMOVIL_PD.PKM_TRANSACTION T
                JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid
                JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z ON T.zoneid = Z.id
                WHERE U.userid = {userid_loc} AND T.date BETWEEN '{startDate_loc}' AND '{endDate_loc}'
                ORDER BY T.date DESC;
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener operaciones en PV: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def pensionsUser(userid_loc, _conn_loc):
            if not _conn_loc: return pd.DataFrame()
            try:
                query = f'''
                SELECT Z.parkinglotname, pp.phonenumber, pp.startdate, pp.enddate, pp.status, pp.description
                FROM CARGOMOVIL_PD.PKM_PARKING_LOT_LODGINGS pp
                JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON pp.parkinglotid = Z.id
                WHERE pp.userid = {userid_loc}
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener información de pensiones: {str(e)}")
                return pd.DataFrame()

        @st.cache_data
        def errorsUser(userid_loc, _conn_loc, startDate_loc, endDate_loc):
            if not _conn_loc: return pd.DataFrame()
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
                CONVERT_TZ(e.eventtimestamp, 'UTC', 'America/Mexico_City') AS error_date
                FROM 
                    CARGOMOVIL_PD.PKM_PARKING_LOT_EVENTS e
                JOIN 
                    CARGOMOVIL_PD.PKM_PARKING_LOT_CAT p ON e.parkinglotid = p.id
                WHERE 
                    e.eventtype LIKE '%error%' 
                    AND e.userid = {userid_loc}
                    AND e.eventtimestamp BETWEEN '{startDate_loc}' AND '{endDate_loc}'
                ORDER BY e.eventtimestamp DESC;
                '''
                return pd.read_sql_query(query, _conn_loc)
            except Exception as e:
                st.error(f"Error al obtener registros de errores: {str(e)}")
                return pd.DataFrame()

        # Mostrar información solo si tenemos un userid válido y fechas válidas
        if userid and start_date and end_date and conn:
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
                    st.warning("No se encontró información de wallet para este usuario.")
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
                    st.warning("No se encontraron tarjetas registradas para este usuario.")

            colveh, colpensions = st.columns(2)
            with colveh:
                st.header("Vehículos del Usuario")
                vehicles_data = vehicleUser(userid, conn)
                if not vehicles_data.empty:
                    st.data_editor(vehicles_data, hide_index=True, use_container_width=True)
                else:
                    st.warning("No se encontraron vehículos registrados para este usuario.")
            with colpensions:
                st.header("Pensiones del Usuario")
                pensions_data = pensionsUser(userid, conn)
                if not pensions_data.empty:
                    st.data_editor(pensions_data, hide_index=True, use_container_width=True)
                else:
                    st.warning("No se encontraron pensiones registradas para este usuario.")

            st.header("Operaciones del Usuario en ED")
            ed_operations = lastEdOperations(userid, conn, start_date, end_date)
            if not ed_operations.empty:
                st.data_editor(ed_operations, hide_index=True, use_container_width=True)
            else:
                st.warning(f"No se encontraron operaciones en ED para este usuario en el rango de fechas seleccionado.")

            st.header("Operaciones del Usuario en PV")
            pv_operations = lastPVOperations(userid, conn, start_date, end_date)
            if not pv_operations.empty:
                st.data_editor(pv_operations, hide_index=True, use_container_width=True)
            else:
                st.warning(f"No se encontraron operaciones en PV para este usuario en el rango de fechas seleccionado.")

            st.header("Movimientos del Usuario")
            movements_data = movementsUser(number, conn) # Uses phone number
            if not movements_data.empty:
                st.data_editor(movements_data, hide_index=True, use_container_width=True)
                try:
                    # Ensure 'TRANSACTIOND_DATE' is datetime
                    movements_data["TRANSACTIOND_DATE"] = pd.to_datetime(movements_data["TRANSACTIOND_DATE"])
                    # Ensure 'FINAL_FUNDS' is numeric
                    movements_data["FINAL_FUNDS"] = pd.to_numeric(movements_data["FINAL_FUNDS"], errors='coerce').fillna(0)

                    fig = px.line(
                        movements_data.sort_values(by="TRANSACTIOND_DATE"), # Sort by date for proper line chart
                        x="TRANSACTIOND_DATE",
                        y="FINAL_FUNDS",
                        title="Wallet del Usuario",
                        labels={"TRANSACTIOND_DATE": "Fecha de Movimiento", "FINAL_FUNDS": "Fondos Finales"},
                        height=400
                    )
                    # The marker_color logic might need adjustment if FINAL_FUNDS can be non-numeric
                    # For simplicity, this example assumes it's numeric after coercion
                    fig.update_traces(
                         line=dict(color='royalblue'), # Using a single color for the line
                         marker=dict(color=movements_data["FINAL_FUNDS"].apply(lambda x: "green" if x >= 0 else "crimson"))
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error al generar gráfico de movimientos: {e}")
            else:
                st.warning("No se encontraron movimientos para este usuario.")

            st.header("Errores del Usuario")
            errors_data = errorsUser(userid, conn, start_date, end_date)
            if not errors_data.empty:
                st.data_editor(errors_data, hide_index=True, use_container_width=True)
            else:
                st.warning("No se encontraron registros de errores para este usuario en el rango de fechas seleccionado.")
        # elif not conn: # Handled at the start of the tab
        #     pass
        elif not userid and number and number != default_phone : # If number was entered but no userid found
            pass # Error message for userid not found is already displayed
        elif not (start_date and end_date): # If dates are not valid
            pass # Warning for date range is already displayed


# --- Tab 2: KIGO AI Agent ---
with tab2:
    st.markdown("### Asistente Virtual KIGO AI")
    st.write("Interactúa con nuestro asistente virtual para resolver tus dudas.")
    components.html(dialogflow_html, height=700, scrolling=True)


# Close the connection and tunnel when the app stops
@atexit.register
def cleanup():
    global conn, tunnel # Ensure we are referring to the global variables
    # print("Running cleanup...") # For debugging
    try:
        if conn:
            # print("Closing DB connection.") # For debugging
            conn.close()
            conn = None
    except Exception as e:
        # print(f"Error closing DB connection: {e}") # For debugging
        pass # Or st.warning, but atexit runs outside Streamlit's main thread for UI
    try:
        if tunnel and tunnel.is_active:
            # print("Stopping SSH tunnel.") # For debugging
            tunnel.stop()
            tunnel = None
    except Exception as e:
        # print(f"Error stopping SSH tunnel: {e}") # For debugging
        pass