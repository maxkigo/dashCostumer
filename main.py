import streamlit as st
import streamlit.components.v1 as components

# 1. Configurar el título de la página y el ícono
st.set_page_config(layout="wide", page_title="KIGO IA CUSTOMER SERVICE", page_icon="decorations/kigo-icon-adaptative.png")

# 2. Mostrar el logo en la parte superior de la página (opcional, pero lo mantenemos)
st.markdown(
    """
    <div style="text-align: center;">
        <img src="https://main.d1jmfkauesmhyk.amplifyapp.com/img/logos/logos.png" 
        alt="KIGO Logo" style="width: 25%; max-width: 30%; height: auto;">
    </div>
    """,
    unsafe_allow_html=True
)

# 3. Definir el HTML para el agente de Dialogflow
dialogflow_html = """
<link rel="stylesheet" href="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/themes/df-messenger-default.css">
<script src="https://www.gstatic.com/dialogflow-console/fast/df-messenger/prod/v1/df-messenger.js"></script>
<df-messenger
  project-id="kigo-ai-customer-support"
  agent-id="a6f52763-f642-4c75-838a-94402118179d"
  language-code="es"
  max-query-length="-1">
  <df-messenger-chat
    chat-title="KIGO AI Customer Support"> </df-messenger-chat>
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
    width: 350px; /* Ancho del chat flotante */
  }
</style>
"""

# 4. Mostrar un título principal para la página (opcional, ya que el chat es flotante)
# st.title("KIGO IA CUSTOMER SERVICE") # Puedes descomentar esto si quieres un título en la página principal
# st.markdown("### Asistente Virtual KIGO AI") # Texto opcional
# st.write("Interactúa con nuestro asistente virtual para resolver tus dudas.") # Texto opcional

# 5. Incrustar el componente HTML de Dialogflow
# El chat de Dialogflow se mostrará como un widget flotante en la esquina.
# El alto y scrolling del componente HTML se aplican al contenedor,
# pero el widget de Dialogflow tiene su propio posicionamiento fijo.
components.html(dialogflow_html, height=700, scrolling=False)

# Mensaje para indicar que la página está enfocada en el AI (opcional)
st.info("Bienvenido al servicio de KIGO IA. Utiliza el asistente de chat para tus consultas.")