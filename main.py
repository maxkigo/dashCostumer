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

# --- Global connection state variables ---
_active_tunnel = None
_active_conn = None
_mypkey = None  # To store the loaded private key

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


# --- Function to load PEM key ---
def load_pem_key():
    # This function is called once. Avoid st.session_state for this initial load
    # if _mypkey is already loaded globally, it means this was called.
    global _mypkey
    if _mypkey is not None:
        return _mypkey

    pem_key_string = st.secrets['pem']['private_key']
    temp_key_file_path_local = None
    loaded_key = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_key_file:
            temp_key_file.write(pem_key_string)
            temp_key_file_path_local = temp_key_file.name

        loaded_key = paramiko.RSAKey.from_private_key_file(temp_key_file_path_local)
        # st.info("Private key loaded successfully.") # Optional, can be noisy
    except Exception as e:
        st.error(f"Error crítico al cargar la llave privada (PEM): {e}")
        # No st.stop() here, let the main script decide if it can proceed
    finally:
        if temp_key_file_path_local and os.path.exists(temp_key_file_path_local):
            os.remove(temp_key_file_path_local)
    return loaded_key


# Load the key once at the start and store it in the global variable
_mypkey = load_pem_key()

# Load environment variables
sql_hostname = st.secrets["database"]["sql_hostname"]
sql_username = st.secrets["database"]["sql_username"]
sql_password = st.secrets["database"]["sql_password"]
sql_main_database = st.secrets["database"]["sql_main_database"]
sql_port = st.secrets["database"]["sql_port"]
ssh_host = st.secrets["ssh"]["ssh_host"]
ssh_user = st.secrets["ssh"]["ssh_user"]
ssh_port = st.secrets["ssh"]["ssh_port"]


# --- Cached Resource Creation Functions ---
@st.cache_resource
def get_ssh_tunnel(ssh_host_arg, ssh_port_arg, ssh_user_arg, ssh_pkey_arg, remote_bind_address_arg):
    if not ssh_pkey_arg:
        raise RuntimeError("La llave SSH (PEM) no está disponible para la creación del túnel.")
    try:
        tunnel = SSHTunnelForwarder(
            (ssh_host_arg, ssh_port_arg),
            ssh_username=ssh_user_arg,
            ssh_pkey=ssh_pkey_arg,
            remote_bind_address=remote_bind_address_arg,
            # logger=st.logger.get_logger() # Optional: for more detailed SSH logs
        )
        tunnel.start()
        return tunnel
    except Exception as e:
        raise RuntimeError(f"Error al crear el túnel SSH: {e}")


@st.cache_resource
def get_db_connection(_tunnel, db_user, db_passwd, db_name, db_host='127.0.0.1'):
    if not (_tunnel and _tunnel.is_active):
        raise RuntimeError("El túnel SSH no está disponible o inactivo para la conexión a la base de datos.")
    try:
        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            passwd=db_passwd,
            db=db_name,
            port=_tunnel.local_bind_port,
            connect_timeout=10
        )
        conn.ping(reconnect=True)
        return conn
    except Exception as e:
        raise RuntimeError(f"Error al crear la conexión a la base de datos: {e}")


@st.cache_resource
def create_rds_geosek_connection():  # Renamed for clarity
    try:
        return pymysql.connect(
            host=st.secrets["rds_geosek"]["host_geosek"],
            user=st.secrets["rds_geosek"]["user_geosek"],
            password=st.secrets["rds_geosek"]["pass_geosek"],
            port=st.secrets["rds_geosek"]["port_geosek"],
            connect_timeout=5
        )
    except Exception as e:
        st.error(f"No se pudo establecer la conexión con RDS Geosek: {str(e)}")
        return None


# --- Define Tabs ---
tab_agente_ia_title = "Agente KIGO AI"
tab_panel_servicio_title = "Panel de Servicio al Cliente"
tab_agente_ia, tab_panel_servicio = st.tabs([tab_agente_ia_title, tab_panel_servicio_title])

# --- Tab 1 (Default): KIGO AI Agent ---
with tab_agente_ia:
    st.markdown("### Asistente Virtual KIGO AI")
    st.write("Interactúa con nuestro asistente virtual para resolver tus dudas.")
    components.html(dialogflow_html, height=700, scrolling=True)

# --- Tab 2: Customer Service Dashboard ---
with tab_panel_servicio:
    st.header(tab_panel_servicio_title)

    global _active_conn, _active_tunnel, _mypkey  # Allow modification of global state

    if _active_conn is None:  # Only attempt connection if not already active
        if not _mypkey:
            st.error(
                "Error crítico: La llave privada (PEM) no se cargó correctamente al inicio. No se pueden establecer conexiones.")
            # st.stop() # Use st.stop() cautiously as it halts script execution.
        else:
            with st.spinner("Estableciendo conexión segura a la base de datos... Por favor espere."):
                try:
                    current_tunnel = get_ssh_tunnel(
                        ssh_host_arg=ssh_host,
                        ssh_port_arg=ssh_port,
                        ssh_user_arg=ssh_user,
                        ssh_pkey_arg=_mypkey,
                        remote_bind_address_arg=(sql_hostname, sql_port)
                    )
                    _active_tunnel = current_tunnel

                    current_conn = get_db_connection(
                        _tunnel=_active_tunnel,
                        db_user=sql_username,
                        db_passwd=sql_password,
                        db_name=sql_main_database
                    )
                    _active_conn = current_conn
                    st.success("Conexión establecida exitosamente.")

                except RuntimeError as e:
                    st.error(f"Fallo en la conexión: {e}")
                    if _active_conn:  # Should not happen if error raised before this
                        try:
                            _active_conn.close()
                        except:
                            pass
                        _active_conn = None
                    if _active_tunnel and _active_tunnel.is_active:
                        try:
                            _active_tunnel.stop()
                        except:
                            pass
                        _active_tunnel = None
                except Exception as e:  # Catch any other unexpected errors
                    st.error(f"Un error inesperado ocurrió durante el proceso de conexión: {e}")
                    if _active_conn:
                        try:
                            _active_conn.close()
                        except:
                            pass
                        _active_conn = None
                    if _active_tunnel and _active_tunnel.is_active:
                        try:
                            _active_tunnel.stop()
                        except:
                            pass
                        _active_tunnel = None

    if _active_conn:
        coldate, colphone = st.columns(2)
        userid = None
        start_date, end_date = None, None  # Initialize

        with coldate:
            d_values = [datetime.date(2024, 1, 1), datetime.date.today()]
            selected_dates = st.date_input("Fecha de Consulta",
                                           value=d_values,
                                           min_value=datetime.date(2024, 1, 1),
                                           max_value=datetime.date.today(),
                                           format="DD/MM/YYYY",
                                           key="date_input_tab_panel")
            if selected_dates and len(selected_dates) == 2:
                start_date = f'{selected_dates[0]} 00:00:00'
                end_date = f'{selected_dates[1]} 23:59:59'
            else:
                st.warning("Rango de fechas inválido. Por favor, seleccione dos fechas.")

        with colphone:
            default_phone_val = 0  # Using 0 as a clear default placeholder
            number_input_val = st.number_input("Ingresar el número del Usuario (10 dígitos):",
                                               value=default_phone_val,
                                               step=1,
                                               format="%d",
                                               key="phone_input_tab_panel_v2")  # Changed key for safety

            number = int(number_input_val) if number_input_val != default_phone_val else default_phone_val


        @st.cache_data
        def useridLocate(phonenumber_loc, _conn_loc_internal):
            if not phonenumber_loc or phonenumber_loc == default_phone_val: return None
            if not _conn_loc_internal: st.warning("Conexión no disponible para buscar userid."); return None
            try:
                query = f"SELECT userid FROM CARGOMOVIL_PD.SEC_USER_PROFILE WHERE phonenumber = '{phonenumber_loc}';"
                df = pd.read_sql_query(query, _conn_loc_internal)
                return df['userid'].iloc[0] if not df.empty else None
            except Exception as e:
                st.error(f"Error al buscar el usuario: {str(e)}"); return None


        if start_date and end_date:  # Proceed only if dates are valid
            if number and number != default_phone_val:
                userid = useridLocate(number, _active_conn)
                if userid is None:
                    st.error("Usuario no encontrado. Verifique el número.")
                else:
                    st.success(f"Usuario encontrado: {userid}")
            elif number == default_phone_val:
                st.info("Ingrese un número telefónico para buscar datos del usuario.")
        else:
            if not (number and number != default_phone_val):  # if dates are bad AND phone is default
                st.info("Seleccione un rango de fechas e ingrese un número telefónico.")
            else:  # if dates are bad but phone is entered
                st.warning("Por favor, asegúrese de que el rango de fechas sea válido.")


        # Define other data functions, they will use _active_conn
        @st.cache_data
        def accountUser(userid_loc, _conn_loc_internal):
            if not _conn_loc_internal: return pd.DataFrame()
            try:
                query = f"SELECT UP.firstname AS nombre, UP.lastname AS apellido, UP.phonenumber AS telefono, UP.facebookemail AS email, CDU.funds AS fondos, CDU.currency AS moneda FROM CARGOMOVIL_PD.CDX_USER_ACCOUNT CDU JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON CDU.userid = UP.userid WHERE UP.userid = {userid_loc};"
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en accountUser: {e}"); return pd.DataFrame()


        @st.cache_data
        def cardsUser(userid_loc, _conn_loc_internal):
            if not _conn_loc_internal: return pd.DataFrame()
            try:
                query = f"SELECT SQ_UC.brand AS 'marca', SQ_UC.last_4, SQ_UC.card_status AS status, SQ_UC.creation_date FROM CARGOMOVIL_PD.uc_users_cards SQ_UC JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON SQ_UC.user_id = UP.userid WHERE UP.userid = {userid_loc};"
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en cardsUser: {e}"); return pd.DataFrame()


        @st.cache_data
        def vehicleUser(userid_loc, _conn_loc_internal):
            if not _conn_loc_internal: return pd.DataFrame()
            try:
                query = f"SELECT V.licenseplate, (CASE WHEN V.status = 1 THEN 'active' ELSE 'inactive' END) AS status, O.modelname AS modelo, U.brandname AS marca FROM CARGOMOVIL_PD.PKM_VEHICLE V JOIN CARGOMOVIL_PD.PKM_VEHICLE_MODELS_CAT O ON V.modelid = O.id JOIN CARGOMOVIL_PD.PKM_VEHICLE_BRANDS_CAT U ON O.brandid = U.id WHERE V.ownerid = {userid_loc} ORDER BY status;"
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en vehicleUser: {e}"); return pd.DataFrame()


        @st.cache_data
        def obtener_usuarios_rds_geosek(user_number_rds, _rds_conn_internal):
            if not _rds_conn_internal: st.warning("Conexión a RDS Geosek no disponible."); return []
            try:
                with _rds_conn_internal.cursor() as cursor:
                    query = f"SELECT L.user AS 'Teléfono', R.alias AS 'Proyecto', L.date AS 'Fechas Acceso', R.name AS 'Acceso' FROM RASPIS.log_sek L JOIN RASPIS.raspis R ON L.QR = R.qr WHERE user LIKE '{user_number_rds}' LIMIT 1;"
                    cursor.execute(query)
                    result = cursor.fetchall()
                return result
            except Exception as e:
                st.error(f"Error en obtener_usuarios_rds_geosek: {e}"); return []


        rds_connection_geosek = create_rds_geosek_connection()
        if rds_connection_geosek and number and number != default_phone_val:
            datos_usuarios_rds = obtener_usuarios_rds_geosek(number, rds_connection_geosek)
            if datos_usuarios_rds:
                st.subheader("Accesos del Usuario (RDS Geosek)")
                st.data_editor(datos_usuarios_rds, hide_index=True, use_container_width=True)


        @st.cache_data
        def lastEdOperations(userid_loc, _conn_loc_internal, startDate_loc_ed, endDate_loc_ed):
            if not (_conn_loc_internal and startDate_loc_ed and endDate_loc_ed): return pd.DataFrame()
            try:
                query = f"""SELECT U.phonenumber AS 'Teléfono', Z.parkinglotname AS 'Estacionamiento', (CASE WHEN PQR.status = 1 THEN 'Open Cicle' ELSE 'Close Cicle' END) AS status, T.subtotal, T.tax, T.fee, T.total, T.qrcode, CASE WHEN T.paymentType = 1 THEN 'NAP' WHEN T.paymentType = 2 THEN 'SMS' WHEN T.paymentType = 3 THEN 'TC/TD' WHEN T.paymentType = 4 THEN 'SALDO' WHEN T.paymentType = 5 THEN 'ATM' ELSE '' END AS 'Método de Pago', TIMESTAMPDIFF(MINUTE, CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City'), CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City')) AS 'Minutos Pagados', CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City') AS 'Entrada', CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City') AS 'Salida', CONVERT_TZ(T.paymentdate, 'UTC', 'America/Mexico_City') AS 'Fecha de Pago', (CASE WHEN PQR.isvalidated = 1 THEN 'Validated' WHEN PQR.isvalidated = 0 THEN 'No Validated' ELSE NULL END) AS 'Promoción Aplicada', PCAT.description AS 'Tipo de Promoción', T.transactionid FROM CARGOMOVIL_PD.PKM_SMART_QR_TRANSACTIONS T JOIN CARGOMOVIL_PD.PKM_SMART_QR_CHECKIN I ON T.checkinid = I.id JOIN CARGOMOVIL_PD.PKM_SMART_QR_CHECKOUT O ON T.checkoutid = O.id JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON T.parkinglotid = Z.id JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid LEFT JOIN CARGOMOVIL_PD.PKM_SMART_QR_PROMOTIONS PP ON T.qrcodeid = PP.qrcodeid LEFT JOIN CARGOMOVIL_PD.GEN_PROMOTION_TYPE_CAT PCAT ON PP.promotionid = PCAT.id LEFT JOIN CARGOMOVIL_PD.PKM_SMART_QR PQR ON T.qrcodeid = PQR.id WHERE U.userid = {userid_loc} AND T.paymentdate BETWEEN '{startDate_loc_ed}' AND '{endDate_loc_ed}' ORDER BY T.paymentdate DESC;"""
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en lastEdOperations: {e}"); return pd.DataFrame()


        @st.cache_data
        def movementsUser(phonenumber_loc, _conn_loc_internal):
            if not _conn_loc_internal: return pd.DataFrame()
            try:
                query = f"CALL usp_metabase_user_account_movements(2, NULL, NULL, '{str(phonenumber_loc)}', NULL);"
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en movementsUser: {e}"); return pd.DataFrame()


        @st.cache_data
        def lastPVOperations(userid_loc, _conn_loc_internal, startDate_loc_pv, endDate_loc_pv):
            if not (_conn_loc_internal and startDate_loc_pv and endDate_loc_pv): return pd.DataFrame()
            try:
                query = f"""SELECT U.phonenumber AS 'Teléfono', CASE WHEN T.paymentType = 1 THEN 'NAP' WHEN T.paymentType = 2 THEN 'SMS' WHEN T.paymentType = 3 THEN 'TC/TD' WHEN T.paymentType = 4 THEN 'SALDO' WHEN T.paymentType = 5 THEN 'ATM' ELSE '' END AS 'Método de Pago', T.licenseplate AS Placa, Z.name AS Parquimetro, T.totalamount AS Pago, T.transactionid, CONVERT_TZ(T.date, 'UTC', 'America/Mexico_City') AS 'Fecha de Transacción' FROM CARGOMOVIL_PD.PKM_TRANSACTION T JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z ON T.zoneid = Z.id WHERE U.userid = {userid_loc} AND T.date BETWEEN '{startDate_loc_pv}' AND '{endDate_loc_pv}' ORDER BY T.date DESC;"""
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en lastPVOperations: {e}"); return pd.DataFrame()


        @st.cache_data
        def pensionsUser(userid_loc, _conn_loc_internal):
            if not _conn_loc_internal: return pd.DataFrame()
            try:
                query = f"SELECT Z.parkinglotname, pp.phonenumber, pp.startdate, pp.enddate, pp.status, pp.description FROM CARGOMOVIL_PD.PKM_PARKING_LOT_LODGINGS pp JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON pp.parkinglotid = Z.id WHERE pp.userid = {userid_loc};"
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en pensionsUser: {e}"); return pd.DataFrame()


        @st.cache_data
        def errorsUser(userid_loc, _conn_loc_internal, startDate_loc_err, endDate_loc_err):
            if not (_conn_loc_internal and startDate_loc_err and endDate_loc_err): return pd.DataFrame()
            try:
                query = f"""SELECT e.id AS log_id, p.parkinglotname AS parking_name, e.userid AS user_id, JSON_UNQUOTE(JSON_EXTRACT(e.metadata, '$.user.username')) AS username, e.eventtype AS event_type, JSON_UNQUOTE(JSON_EXTRACT(e.metadata, '$.qrCode')) AS qrcode, JSON_EXTRACT(e.metadata, '$.error.code') AS error_code, JSON_EXTRACT(e.metadata, '$.error.status') AS error_status, JSON_UNQUOTE(JSON_EXTRACT(e.metadata, '$.error.message')) AS error_message, JSON_EXTRACT(e.metadata, '$.gateId') AS gate_id, CONVERT_TZ(e.eventtimestamp, 'UTC', 'America/Mexico_City') AS error_date FROM CARGOMOVIL_PD.PKM_PARKING_LOT_EVENTS e JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT p ON e.parkinglotid = p.id WHERE e.eventtype LIKE '%error%' AND e.userid = {userid_loc} AND e.eventtimestamp BETWEEN '{startDate_loc_err}' AND '{endDate_loc_err}' ORDER BY e.eventtimestamp DESC;"""
                return pd.read_sql_query(query, _conn_loc_internal)
            except Exception as e:
                st.error(f"Error en errorsUser: {e}"); return pd.DataFrame()


        if userid and start_date and end_date:  # Ensure all conditions met to display data
            colacount, colcards = st.columns(2)
            with colacount:
                st.subheader("Wallets del Usuario")
                account_data = accountUser(userid, _active_conn)
                if not account_data.empty:
                    st.data_editor(account_data, height=100, use_container_width=True, hide_index=True,
                                   num_rows="fixed")
                else:
                    st.info("No se encontró información de wallet.")
            with colcards:
                st.subheader("Tarjetas del Usuario")
                cards_data = cardsUser(userid, _active_conn)
                if not cards_data.empty:
                    st.data_editor(cards_data, height=150, use_container_width=True, hide_index=True,
                                   num_rows="dynamic")
                else:
                    st.info("No se encontraron tarjetas.")

            colveh, colpensions = st.columns(2)
            with colveh:
                st.subheader("Vehículos del Usuario")
                vehicles_data = vehicleUser(userid, _active_conn)
                if not vehicles_data.empty:
                    st.data_editor(vehicles_data, height=150, hide_index=True, use_container_width=True,
                                   num_rows="dynamic")
                else:
                    st.info("No se encontraron vehículos.")
            with colpensions:
                st.subheader("Pensiones del Usuario")
                pensions_data = pensionsUser(userid, _active_conn)
                if not pensions_data.empty:
                    st.data_editor(pensions_data, height=150, hide_index=True, use_container_width=True,
                                   num_rows="dynamic")
                else:
                    st.info("No se encontraron pensiones.")

            st.subheader("Operaciones del Usuario en ED")
            ed_operations = lastEdOperations(userid, _active_conn, start_date, end_date)
            if not ed_operations.empty:
                st.data_editor(ed_operations, height=300, hide_index=True, use_container_width=True)
            else:
                st.info(f"No se encontraron operaciones en ED en el rango seleccionado.")

            st.subheader("Operaciones del Usuario en PV")
            pv_operations = lastPVOperations(userid, _active_conn, start_date, end_date)
            if not pv_operations.empty:
                st.data_editor(pv_operations, height=300, hide_index=True, use_container_width=True)
            else:
                st.info(f"No se encontraron operaciones en PV en el rango seleccionado.")

            st.subheader("Movimientos del Usuario")
            movements_data = movementsUser(number, _active_conn)
            if not movements_data.empty:
                st.data_editor(movements_data, height=300, hide_index=True, use_container_width=True)
                try:
                    movements_data_copy = movements_data.copy()
                    movements_data_copy["TRANSACTIOND_DATE"] = pd.to_datetime(movements_data_copy["TRANSACTIOND_DATE"])
                    movements_data_copy["FINAL_FUNDS"] = pd.to_numeric(movements_data_copy["FINAL_FUNDS"],
                                                                       errors='coerce').fillna(0)
                    fig = px.line(
                        movements_data_copy.sort_values(by="TRANSACTIOND_DATE"),
                        x="TRANSACTIOND_DATE", y="FINAL_FUNDS", title="Wallet del Usuario",
                        labels={"TRANSACTIOND_DATE": "Fecha", "FINAL_FUNDS": "Fondos"}, height=400
                    )
                    fig.update_traces(line=dict(color='royalblue'), marker=dict(
                        color=movements_data_copy["FINAL_FUNDS"].apply(lambda x: "green" if x >= 0 else "crimson")))
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error al generar gráfico de movimientos: {e}")
            else:
                st.info("No se encontraron movimientos.")

            st.subheader("Errores del Usuario")
            errors_data = errorsUser(userid, _active_conn, start_date, end_date)
            if not errors_data.empty:
                st.data_editor(errors_data, height=300, hide_index=True, use_container_width=True)
            else:
                st.info("No se encontraron errores en el rango seleccionado.")

    elif _mypkey:  # If _active_conn is None but _mypkey was loaded (implies connection attempt failed)
        st.warning(
            "La conexión a la base de datos no está activa. Verifique los mensajes de error anteriores o intente recargar.")
    # If _mypkey is None, the error is shown at the top of the tab.


# --- Cleanup Function ---
@atexit.register
def cleanup():
    global _active_conn, _active_tunnel
    # print("DEBUG: Running cleanup function...") # For debugging
    try:
        if _active_conn:
            # print("DEBUG: Closing DB connection.")
            _active_conn.close()
            _active_conn = None  # Ensure it's None after closing
    except Exception as e:
        # print(f"DEBUG: Error closing DB connection: {e}")
        pass
    try:
        if _active_tunnel and _active_tunnel.is_active:
            # print("DEBUG: Stopping SSH tunnel.")
            _active_tunnel.stop()
            _active_tunnel = None  # Ensure it's None after stopping
    except Exception as e:
        # print(f"DEBUG: Error stopping SSH tunnel: {e}")
        pass