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
  agent-id="9914f7e8-fe6e-46ce-bdad-d13115af8a95"
  language-code="es"
  max-query-length="-1"
  allow-feedback="all">
  <df-messenger-chat-bubble
   chat-title="agent-flow-2317">
  </df-messenger-chat-bubble>
</df-messenger>
<style>
  df-messenger {
    z-index: 9999 !important;
    position: fixed;
    --df-messenger-font-color: #000;
    --df-messenger-font-family: Google Sans;
    --df-messenger-chat-background: #f3f6fc;
    --df-messenger-message-user-background: #d3e3fd;
    --df-messenger-message-bot-background: #fff;
    bottom: 16px;
    right: 16px;
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

coldate, colphone = st.columns(2)

with coldate:

    # Date input for the query
    d = st.date_input("Fecha de Consulta",
                  value=[datetime.date(2024, 1, 1), datetime.date.today()],
                  min_value=datetime.date(2024, 1, 1),
                  max_value=datetime.date.today(),
                  format="DD/MM/YYYY")
    d

    star_date = f'{d[0]} 00:00:00'
    end_date = f'{d[1]} 23:59:59'



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
tunnel = create_ssh_tunnel()
conn = create_db_connection(tunnel)

# Obtain the userid from the phone number provide in number variable
@st.cache_data
def useridLocate(phonenumber, _conn):
    try:
        query = f'''
        SELECT userid
        FROM CARGOMOVIL_PD.SEC_USER_PROFILE
        WHERE phonenumber = '{phonenumber}';
        '''

        df = pd.read_sql_query(query, _conn)

        if not df.empty:
            return df['userid'].iloc[0]

        else:
            print("User id not found for the provided phone number.")
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        print("Number not found or database error")
        return None

with colphone:
    default_phone = 000000000
    # Phone number for the query
    number = st.number_input("Ingresar el número del Usuario:",
                         value=default_phone,
                         step=1,
                         format="%d")

    if number:
        userid = useridLocate(number, conn)
        if userid:
            st.success(f"Usuario encontrado: {userid}")
        else:
            st.error("Usuario no encontrado.")

@st.cache_data
def accountUser(userid, _conn):
    query = f'''
        SELECT UP.firstname AS nombre, UP.lastname AS apellido, UP.phonenumber, UP.facebookemail, CDU.funds, CDU.currency
        FROM CARGOMOVIL_PD.CDX_USER_ACCOUNT CDU
        JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON CDU.userid = UP.userid
        WHERE UP.userid = {userid};
    '''
    return pd.read_sql_query(query, _conn)

@st.cache_data
def cardsUser(userid, _conn):
    query = f'''
    SELECT UP.phonenumber, SQ_UC.brand, SQ_UC.last_4, SQ_UC.card_status, 
    SQ_UC.gateway, SQ_UC.creation_date 
    FROM CARGOMOVIL_PD.uc_users_cards SQ_UC
    JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON SQ_UC.user_id = UP.userid
    WHERE UP.userid = {userid};
    '''
    return pd.read_sql_query(query, _conn)

@st.cache_data
def vehicleUser(userid, _conn):
    query = f'''
    SELECT V.ownerid AS userid, V.licenseplate, (CASE WHEN V.status = 1 THEN 'active' ELSE 'inactive' END) AS status,
    O.modelname, U.brandname
    FROM CARGOMOVIL_PD.PKM_VEHICLE V
    JOIN CARGOMOVIL_PD.PKM_VEHICLE_MODELS_CAT O ON V.modelid = O.id 
    JOIN CARGOMOVIL_PD.PKM_VEHICLE_BRANDS_CAT U ON O.brandid = U.id
    WHERE V.ownerid = {userid}
    ORDER BY status
    ;
    '''
    return pd.read_sql_query(query, _conn)


@st.cache_data
def lastEdOperations(userid, _conn, startDate=star_date, endDate=end_date):
    query = f'''
        SELECT U.userid, U.phonenumber, T.qrcode, T.transactionid, Z.parkinglotname,
               T.subtotal, T.tax, T.fee, T.total, 
               CASE
                WHEN T.paymentType = 1 THEN 'NAP'
                WHEN T.paymentType = 2 THEN 'SMS'
                WHEN T.paymentType = 3 THEN 'TC/TD'
                WHEN T.paymentType = 4 THEN 'SALDO'
                WHEN T.paymentType = 5 THEN 'ATM'
                ELSE ''
                END AS paymentType,
               TIMESTAMPDIFF(MINUTE, CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City'), 
                         CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City')) AS paidtimeminutes,
               CONVERT_TZ(T.paymentdate, 'UTC', 'America/Mexico_City') AS date,
               CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City') AS checkindate, 
               CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City') AS checkoutdate,
               (CASE WHEN PQR.isvalidated = 1 THEN 'Validated'
        WHEN PQR.isvalidated = 0 THEN 'No Validated' ELSE NULL END) AS promotionApplied,
        PCAT.description AS promotiontype, (CASE WHEN PQR.status = 1 THEN 'Open Cicle' ELSE 'Close Cicle' END) AS status
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

@st.cache_data
def movementsUser(phonenumber, _conn):
    query = f'''
    CALL usp_metabase_user_account_movements(
      2,
     NULL,
     NULL,
     '{phonenumber}'  --  = El número que introducen
   ,NULL);
    '''
    return pd.read_sql_query(query, _conn)

@st.cache_data
def lastPVOperations(userid, _conn, startDate=star_date, endDate=end_date):
    query = f'''
    SELECT U.userid, U.phonenumber, 
    CASE
                WHEN T.paymentType = 1 THEN 'NAP'
                WHEN T.paymentType = 2 THEN 'SMS'
                WHEN T.paymentType = 3 THEN 'TC/TD'
                WHEN T.paymentType = 4 THEN 'SALDO'
                WHEN T.paymentType = 5 THEN 'ATM'
                ELSE ''
                END AS paymentType,
    T.licenseplate, T.transactionid, Z.name, T.totalamount
    FROM CARGOMOVIL_PD.PKM_TRANSACTION T
    JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid
    JOIN CARGOMOVIL_PD.PKM_PARKING_METER_ZONE_CAT Z ON T.zoneid = Z.id
    WHERE U.userid = {userid} AND T.date BETWEEN '{startDate}' AND '{endDate}'
    '''
    return pd.read_sql_query(query, _conn)

@st.cache_data
def pensionsUser(userid, _conn):
    query = f'''
    SELECT Z.parkinglotname, pp.phonenumber, pp.startdate, pp.enddate, pp.status, pp.description
    FROM CARGOMOVIL_PD.PKM_PARKING_LOT_LODGINGS pp
    JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON pp.parkinglotid = Z.id
    WHERE pp.userid = {userid}
    '''
    return pd.read_sql_query(query, _conn)

@st.cache_data
def errorsUser(userid, _conn):
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
    AND e.eventtimestamp BETWEEN '{star_date}' AND '{end_date}'; 
    '''
    return pd.read_sql_query(query, _conn)


colacount, colcards = st.columns(2)

with colacount:
    st.header("Wallets del Usuario")
    st.data_editor(
    accountUser(userid, conn),
    use_container_width=True,
    hide_index=True,
    num_rows="fixed"
    )
with colcards:
    st.header("Tarjetas del Usuario")
    st.data_editor(
    cardsUser(userid, conn),
    use_container_width=True,
    hide_index=True,
    num_rows="fixed"
    )

colveh, colpensions = st.columns(2)

with colveh:
        st.header("Vehiculos del Usuario")
        st.data_editor(vehicleUser(userid, conn))

with colpensions:
        st.header("Pensiones del Usuario")
        st.data_editor(pensionsUser(userid, conn))



st.header("Operaciones del Usuario en ED")
st.data_editor(lastEdOperations(userid, conn, star_date, end_date))
st.header("Operaciones del Usuario en PV")
st.data_editor(lastPVOperations(userid, conn, star_date, end_date))

st.header("Movimientos del Usuario")
movements_data = movementsUser(number, conn)
st.data_editor(movements_data)

# Bar plot
fig = px.line(
    movements_data,
    x="TRANSACTIOND_DATE",
    y="FINAL_FUNDS",
    title="Wallet del Usuario",
    labels={"TRANSACTIOND_DATE": "Fecha de Movimiento", "FINAL_FUNDS": "Fondos Finales"},
    height=400
)

# Update bar colors based on the condition
fig.update_traces(
    marker_color=[
        "green" if funds >= 0 else "crimson" for funds in movements_data["FINAL_FUNDS"]
    ]
)

# Display the plot in Streamlit
st.plotly_chart(fig, use_container_width=True)


st.header("Errores del Usuario")
st.data_editor(errorsUser(userid, conn))

# Call to Dialogflow Agent
components.html(dialogflow_html, height=700, scrolling=True)

# Close the connection and tunnel when the app stops
def cleanup():
    conn.close()
    tunnel.stop()

# Register the cleanup function to run when the app stops
import atexit
atexit.register(cleanup)