import streamlit as st
import pandas as pd
import io
import tableauserverclient as tsc

st.set_page_config(page_title="Unificador de Archivos para Tableau", page_icon="📊", layout="wide")

def unificar_archivos_subidos(archivos, nombre_hoja):
    """
    Unifica los archivos subidos en un solo DataFrame
    """
    if not archivos:
        return None, "No se han seleccionado archivos."
    
    # Mostrar progreso
    progress_bar = st.progress(0)
    archivos_procesados = st.empty()
    
    # Lista para almacenar todos los dataframes
    dfs = []
    archivos_ok = 0
    
    # Procesar cada archivo
    for i, archivo in enumerate(archivos):
        try:
            # Obtener el nombre del archivo
            nombre_archivo = archivo.name
            st.write(f"Procesando: {nombre_archivo}")
            
            # Detectar la extensión para usar el motor adecuado
            extension = nombre_archivo.lower().split('.')[-1]
            if extension == 'xls':
                engine = 'xlrd'
            else:
                engine = None  # Pandas determinará automáticamente el motor para xlsx
            
            # Leer la hoja especificada de cada archivo
            df = pd.read_excel(archivo, sheet_name=nombre_hoja, engine=engine)
            
            # Añadir el dataframe a la lista
            dfs.append(df)
            
            archivos_ok += 1
            st.write(f"  -> Leídas {len(df)} filas")
            
            # Actualizar la barra de progreso
            progress_bar.progress((i + 1) / len(archivos))
            archivos_procesados.write(f"Archivos procesados: {i+1}/{len(archivos)}")
            
        except Exception as e:
            st.error(f"Error al procesar {nombre_archivo}: {str(e)}")
    
    # Verificar si se procesó al menos un archivo correctamente
    if not dfs:
        return None, "No se pudo procesar ningún archivo correctamente."
    
    # Concatenar todos los dataframes en uno solo
    st.write("\nFusionando todos los archivos...")
    df_final = pd.concat(dfs, ignore_index=True)
    
    return df_final, f"Se han unificado {archivos_ok} archivos con un total de {len(df_final)} filas."

def conectar_tableau_server(server_url, site_name, username, password, disable_ssl):
    """
    Conecta al servidor Tableau y devuelve el objeto de servidor autenticado
    """
    try:
        # Autenticación en Tableau Server
        tableau_auth = tsc.TableauAuth(username, password, site_id=site_name)
        
        # Crear objeto de servidor con detección automática de versión
        server = tsc.Server(server_url, use_server_version=True)
        
        # Configurar verificación SSL
        if disable_ssl:
            server.add_http_options({'verify': False})
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Iniciar sesión
        server.auth.sign_in(tableau_auth)
        
        return server, f"Conexión exitosa a Tableau Server. API versión: {server.version}"
    
    except Exception as e:
        return None, f"Error al conectar con Tableau Server: {str(e)}"

def buscar_fuente_datos(server, datasource_name):
    """
    Busca una fuente de datos en Tableau Server por su nombre
    """
    try:
        # Configurar filtro de búsqueda
        request_options = tsc.RequestOptions()
        request_options.filter.add(tsc.Filter(tsc.RequestOptions.Field.Name, 
                                            tsc.RequestOptions.Operator.Equals, 
                                            datasource_name))
        
        # Ejecutar búsqueda
        all_datasources, pagination_item = server.datasources.get(request_options)
        
        if not all_datasources:
            return None, f"No se encontró la fuente de datos '{datasource_name}'."
        
        # Tomar la primera fuente de datos que coincida
        datasource = all_datasources[0]
        
        return datasource, f"Fuente de datos encontrada con ID: {datasource.id}"
    
    except Exception as e:
        return None, f"Error al buscar la fuente de datos: {str(e)}"

def obtener_fuentes_datos_disponibles(server):
    """
    Obtiene una lista de las fuentes de datos disponibles en el servidor
    """
    try:
        all_datasources, pagination_item = server.datasources.get()
        return [ds.name for ds in all_datasources]
    except:
        return []

# Interfaz principal de la aplicación
def main():
    st.title("🔄 Unificador de Archivos para Tableau")
    
    # Crear pestañas para las diferentes secciones
    tab1, tab2, tab3 = st.tabs(["1️⃣ Unificar Archivos", "2️⃣ Conectar a Tableau", "3️⃣ Instrucciones"])
    
    with tab1:
        st.header("Unificación de Archivos")
        
        st.write("### Subir archivos")
        st.write("Selecciona los archivos Excel que deseas unificar:")
        
        # Selector de archivos
        uploaded_files = st.file_uploader(
            "Arrastra aquí los archivos o haz clic para seleccionarlos", 
            type=["xls", "xlsx"], 
            accept_multiple_files=True
        )
        
        # Campos adicionales
        nombre_hoja = st.text_input("Nombre de la hoja a leer:", 
                                   value="Itemization",
                                   help="Nombre exacto de la hoja que se debe leer en cada archivo Excel")
        
        nombre_hoja_salida = st.text_input("Nombre de la hoja de salida:", 
                                         value="Datos_Unificados",
                                         help="Nombre de la hoja en el archivo unificado")
        
        unificar_button = st.button("Unificar Archivos")
        
        if unificar_button:
            if not uploaded_files:
                st.error("Por favor, sube al menos un archivo.")
            elif not nombre_hoja:
                st.error("Por favor, especifica el nombre de la hoja a leer.")
            else:
                with st.spinner("Unificando archivos..."):
                    # Ejecutar la unificación con los archivos subidos
                    df_unificado, mensaje = unificar_archivos_subidos(uploaded_files, nombre_hoja)
                    
                    if df_unificado is not None:
                        st.success(mensaje)
                        
                        # Mostrar una vista previa de los datos
                        st.write("### Vista previa de los datos unificados:")
                        st.dataframe(df_unificado.head(10))
                        
                        # Guardar en la sesión para usar en la siguiente pestaña
                        st.session_state.df_unificado = df_unificado
                        
                        # Preparar archivo para descarga
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            df_unificado.to_excel(writer, sheet_name=nombre_hoja_salida, index=False)
                        
                        excel_buffer.seek(0)
                        st.session_state.excel_buffer = excel_buffer
                        st.session_state.nombre_archivo = "Archivo_Unificado.xlsx"
                        
                        # Mostrar botón para descargar el archivo
                        st.download_button(
                            label="📥 Descargar archivo unificado",
                            data=excel_buffer,
                            file_name="Archivo_Unificado.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.error(mensaje)
    
    with tab2:
        st.header("Conexión a Tableau Server")
        
        # Verificar si ya existe un archivo unificado
        if not hasattr(st.session_state, 'df_unificado'):
            st.warning("⚠️ Primero debes unificar los archivos en la pestaña anterior.")
        else:
            st.success(f"✅ Datos unificados listos para subir a Tableau Server")
            
            # Formulario para conectar a Tableau
            with st.form("tableau_form"):
                st.write("### Configuración de Tableau Server")
                
                tableau_server = st.text_input("URL del Tableau Server:", 
                                             placeholder="https://tableausrv.junta-andalucia.es",
                                             help="URL completa del servidor Tableau")
                
                sitio_predeterminado = st.checkbox("Es el sitio predeterminado", value=True, 
                                                 help="Marcar si es el sitio predeterminado, sino debes especificar el ID del sitio")
                
                if not sitio_predeterminado:
                    nombre_sitio = st.text_input("ID del sitio:")
                else:
                    nombre_sitio = ""
                
                usar_dominio = st.checkbox("Usar dominio de Active Directory", value=True,
                                         help="Marcar si necesitas especificar un dominio para la autenticación")
                
                if usar_dominio:
                    dominio = st.text_input("Dominio:", placeholder="JUNTADEANDALUCIA")
                    usuario_base = st.text_input("Nombre de usuario (sin dominio):")
                    if dominio and usuario_base:
                        usuario = f"{dominio}\\{usuario_base}"
                    else:
                        usuario = ""
                else:
                    usuario = st.text_input("Nombre de usuario:")
                
                contraseña = st.text_input("Contraseña:", type="password")
                
                deshabilitar_ssl = st.checkbox("Deshabilitar verificación SSL", value=True,
                                             help="Marca esta opción si hay problemas con certificados SSL")
                
                nombre_fuente_datos = st.text_input("Nombre exacto de la fuente de datos a actualizar:",
                                                  help="Nombre exacto de la fuente de datos en Tableau Server")
                
                submit_tableau = st.form_submit_button("Conectar a Tableau Server")
            
            if submit_tableau:
                if not tableau_server or not usuario or not contraseña or not nombre_fuente_datos:
                    st.error("Por favor, completa todos los campos requeridos.")
                else:
                    with st.spinner("Conectando a Tableau Server..."):
                        # Intentar conectar al servidor
                        server, mensaje = conectar_tableau_server(
                            tableau_server, nombre_sitio, usuario, contraseña, deshabilitar_ssl
                        )
                        
                        if server is not None:
                            st.success(mensaje)
                            
                            # Guardar en sesión
                            st.session_state.server = server
                            
                            # Buscar la fuente de datos
                            st.write(f"Buscando fuente de datos: '{nombre_fuente_datos}'")
                            datasource, ds_mensaje = buscar_fuente_datos(server, nombre_fuente_datos)
                            
                            if datasource is not None:
                                st.success(ds_mensaje)
                                
                                # Verificar la versión de la API
                                if float(server.version) < 2.8:
                                    st.warning(f"⚠️ La versión de API {server.version} no soporta actualización automática de extractos (requiere 2.8+)")
                                    st.info("Se requiere actualización manual a través de la interfaz web")
                                    
                                    # Ofrecer descarga para actualización manual
                                    if hasattr(st.session_state, 'excel_buffer'):
                                        st.write("### Instrucciones para actualización manual:")
                                        st.code(f"""
1. Descarga el archivo unificado usando el botón de abajo
2. Inicia sesión en Tableau Server: {tableau_server}
3. Navega a la fuente de datos: {nombre_fuente_datos}
4. Selecciona 'Actualizar ahora' o 'Reemplazar fuente de datos'
5. Sube el archivo unificado descargado
                                        """)
                                        
                                        # Re-mostrar el botón de descarga
                                        st.download_button(
                                            label="📥 Descargar archivo para actualización manual",
                                            data=st.session_state.excel_buffer,
                                            file_name=st.session_state.nombre_archivo,
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                        )
                                else:
                                    # Intentar actualización automática
                                    st.write("Solicitando actualización de extracción...")
                                    try:
                                        job = server.datasources.refresh(datasource.id)
                                        st.success(f"✅ Trabajo de actualización iniciado con ID: {job.id}")
                                    except Exception as e:
                                        st.error(f"Error al solicitar la actualización: {str(e)}")
                                        st.info("Se requiere actualización manual a través de la interfaz web")
                                        
                                        # Ofrecer descarga para actualización manual
                                        if hasattr(st.session_state, 'excel_buffer'):
                                            st.write("### Instrucciones para actualización manual:")
                                            st.code(f"""
1. Descarga el archivo unificado usando el botón de abajo
2. Inicia sesión en Tableau Server: {tableau_server}
3. Navega a la fuente de datos: {nombre_fuente_datos}
4. Selecciona 'Actualizar ahora' o 'Reemplazar fuente de datos'
5. Sube el archivo unificado descargado
                                            """)
                                            
                                            # Re-mostrar el botón de descarga
                                            st.download_button(
                                                label="📥 Descargar archivo para actualización manual",
                                                data=st.session_state.excel_buffer,
                                                file_name=st.session_state.nombre_archivo,
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                            )
                            else:
                                st.error(ds_mensaje)
                                
                                # Mostrar fuentes de datos disponibles
                                fuentes_disponibles = obtener_fuentes_datos_disponibles(server)
                                if fuentes_disponibles:
                                    st.write("### Fuentes de datos disponibles:")
                                    for i, ds in enumerate(fuentes_disponibles[:10]):
                                        st.write(f"  {i+1}. {ds}")
                                    if len(fuentes_disponibles) > 10:
                                        st.write(f"  ... y {len(fuentes_disponibles)-10} más")
                        else:
                            st.error(mensaje)
            
            # Cerrar sesión cuando se abandona la aplicación
            if hasattr(st.session_state, 'server'):
                try:
                    st.session_state.server.auth.sign_out()
                except:
                    pass
    
    with tab3:
        st.header("Instrucciones de Uso")
        
        st.write("""
        ### Guía paso a paso
        
        Esta aplicación te permite unificar múltiples archivos Excel en uno solo y actualizarlo en Tableau Server.
        
        #### Pestaña 1: Unificar Archivos
        
        1. **Subir archivos**: Arrastra o selecciona los archivos Excel que deseas unificar
        2. **Nombre de la hoja**: Indica el nombre exacto de la hoja que deseas leer en cada archivo
        3. **Nombre de la hoja de salida**: Define cómo se llamará la hoja en el archivo unificado
        4. **Unificar**: Haz clic en el botón "Unificar Archivos" y espera a que se procesen
        5. **Descargar**: Una vez completado, podrás descargar el archivo unificado
        
        #### Pestaña 2: Conectar a Tableau
        
        1. **URL del Tableau Server**: Introduce la URL completa del servidor Tableau
        2. **Configuración del sitio**: Marca si es el sitio predeterminado o introduce el ID del sitio
        3. **Autenticación**: Configura cómo te vas a autenticar (con o sin dominio)
        4. **Nombre de la fuente de datos**: Introduce el nombre exacto de la fuente de datos a actualizar
        5. **Conectar**: Haz clic en "Conectar a Tableau Server" para intentar la actualización
        
        ### Notas importantes
        
        - La aplicación procesa los archivos Excel en el navegador, no necesitas rutas de directorio locales
        - Puedes descargar el archivo unificado para guardarlo en tu computadora
        - La actualización automática solo funciona si el servidor Tableau tiene una versión de API 2.8 o superior
        - En caso de error, sigue las instrucciones para actualización manual
        """)
        
        st.info("Si tienes problemas para conectar a Tableau Server, verifica la URL y las credenciales. Si persisten los problemas con certificados SSL, marca la opción 'Deshabilitar verificación SSL'.")

if __name__ == "__main__":
    main()
