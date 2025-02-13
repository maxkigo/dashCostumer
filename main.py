import pandas as pd
import streamlit as st
import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
import datetime
import tempfile
import os

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

# Date input for the query
d = st.date_input("Fecha de Consulta",
                  value=[datetime.date(2025, 1, 1), datetime.date.today()],
                  min_value=datetime.date(2024, 1, 1),
                  max_value=datetime.date.today(),
                  format="DD/MM/YYYY")

# Phone number for the query
number = st.number_input("Ingresar el número del Usuario:",
                         value=2213500061,
                         step=1,
                         format="%d")

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

@st.cache_data
def accountUser(phonenumber, _conn):
    query = f'''
        SELECT UP.firstname AS nombre, UP.lastname AS apellido, UP.phonenumber, UP.facebookemail, CDU.accountnumber, CDU.funds, CDU.currency
        FROM CARGOMOVIL_PD.CDX_USER_ACCOUNT CDU
        JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON CDU.userid = UP.userid
        WHERE UP.phonenumber = {phonenumber};
    '''
    return pd.read_sql_query(query, _conn)

@st.cache_data
def cardsUser(phonenumber, _conn):
    query = f'''
    SELECT UP.phonenumber, SQ_UC.brand, SQ_UC.last_4, SQ_UC.card_status, 
    SQ_UC.gateway, SQ_UC.creation_date 
    FROM CARGOMOVIL_PD.uc_users_cards SQ_UC
    JOIN CARGOMOVIL_PD.SEC_USER_PROFILE UP ON SQ_UC.user_id = UP.userid
    WHERE UP.phonenumber = {phonenumber};
    '''
    return pd.read_sql_query(query, _conn)


@st.cache_data
def lastEdOperations(phonenumber, _conn):
    query = f'''
        SELECT U.userid, U.phonenumber, T.transactionid, Z.parkinglotname,
               T.total, CONVERT_TZ(T.paymentdate, 'UTC', 'America/Mexico_City') AS date,
               CONVERT_TZ(I.checkindate, 'UTC', 'America/Mexico_City') AS checkindate, 
               CONVERT_TZ(O.checkoutdate, 'UTC', 'America/Mexico_City') AS checkoutdate
        FROM CARGOMOVIL_PD.PKM_SMART_QR_TRANSACTIONS T
        JOIN CARGOMOVIL_PD.PKM_SMART_QR_CHECKIN I ON T.checkinid = I.id
        JOIN CARGOMOVIL_PD.PKM_SMART_QR_CHECKOUT O ON T.checkoutid = O.id
        JOIN CARGOMOVIL_PD.PKM_PARKING_LOT_CAT Z ON T.parkinglotid = Z.id
        JOIN CARGOMOVIL_PD.SEC_USER_PROFILE U ON T.userid = U.userid
        WHERE U.phonenumber = {phonenumber} AND DATE(T.paymentdate) BETWEEN '{d[0]}' AND '{d[1]}'
        ORDER BY T.paymentdate DESC;
    '''
    return pd.read_sql_query(query, _conn)

colacount, colcards = st.columns(2)

with colacount:
    st.header("Wallets del Usuario")
    st.data_editor(
    accountUser(number, conn),
    use_container_width=True,
    hide_index=True,
    num_rows="fixed"
    )
with colcards:
    st.header("Tarjetas del Usuario")
    st.data_editor(
    cardsUser(number, conn),
    use_container_width=True,
    hide_index=True,
    num_rows="fixed"
    )


# Display the data in a table
st.table(lastEdOperations(number, conn))

# Close the connection and tunnel when the app stops
def cleanup():
    conn.close()
    tunnel.stop()

# Register the cleanup function to run when the app stops
import atexit
atexit.register(cleanup)
