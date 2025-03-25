import streamlit as st
import pandas as pd
import glob
import os
import tableauserverclient as tsc
import tempfile
import shutil
from pathlib import Path

st.set_page_config(page_title="Unificador de Archivos para Tableau", page_icon="📊", layout="wide")

def unificar_archivos(directorio, patron_archivos, nombre_hoja):
    """
    Unifica los archivos que coinciden con el patrón en el directorio especificado
    """
    # Crear la ruta completa con el patrón
    ruta_completa = os.path.join(directorio, patron_archivos)
    
    # Buscar todos los archivos que coincidan con el patrón
    archivos = glob.glob(ruta_completa)
    
    if not archivos:
        return None, f"No se encontraron archivos que coincidan con el patrón '{patron_archivos}' en el directorio seleccionado."
    
    # Mostrar progreso
    progress_bar = st.progress(0)
    archivos_procesados = st.empty()
    
    # Lista para almacenar todos los dataframes
    dfs = []
    archivos_ok = 0
    
    # Procesar cada archivo
    for i, archivo in enumerate(archivos):
        try:
            # Obtener solo el nombre del archivo para el registro
            nombre_archivo = os.path.basename(archivo)
            st.write(f"Procesando: {nombre_archivo}")
            
            # Detectar la extensión para usar el motor adecuado
            extension = os.path.splitext(archivo)[1].lower()
            if extension == '.xls':
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

def guardar_archivo_unificado(df, directorio, nombre_archivo="Archivo_Unificado.xlsx", nombre_hoja="Datos"):
    """
    Guarda el DataFrame en un archivo Excel
    """
    if df is None:
        return None, "No hay datos para guardar."
    
    # Ruta completa del archivo de salida
    archivo_salida = os.path.join(directorio, nombre_archivo)
    
    try:
        # Usar opciones más seguras para guardar
        with pd.ExcelWriter(
            archivo_salida,
            engine='openpyxl',
            mode='w'  # Sobrescribir si existe
        ) as writer:
            df.to_excel(writer, sheet_name=nombre_hoja, index=False)
            
        return archivo_salida, f"Archivo unificado guardado exitosamente en: {archivo_salida}"
    except Exception as e:
        return None, f"Error al guardar el archivo: {str(e)}"

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
        
        # Formulario para unificar archivos
        with st.form("unificar_form"):
            st.write("### Configuración de archivos")
            
            directorio = st.text_input("Directorio donde están los archivos:", 
                                      help="Introduce la ruta completa al directorio donde se encuentran los archivos (ej. C:\\Datos\\MisArchivos)")
            
            patron_archivos = st.text_input("Patrón de archivos a unificar:", 
                                           placeholder="ej. OpsCenter_*.xls",
                                           help="Introduce el patrón para identificar los archivos a unificar (ej. reporte_*.xlsx)")
            
            nombre_hoja = st.text_input("Nombre de la hoja a leer:", 
                                       value="Itemization",
                                       help="Nombre exacto de la hoja que se debe leer en cada archivo Excel")
            
            nombre_salida = st.text_input("Nombre del archivo unificado:", 
                                        value="Archivo_Unificado.xlsx",
                                        help="Nombre del archivo de salida donde se guardarán todos los datos")
            
            nombre_hoja_salida = st.text_input("Nombre de la hoja de salida:", 
                                             value="Datos_Unificados",
                                             help="Nombre de la hoja en el archivo de salida")
            
            submit_button = st.form_submit_button("Unificar Archivos")
        
        if submit_button:
            if not directorio or not patron_archivos or not nombre_hoja:
                st.error("Por favor, completa todos los campos requeridos.")
            else:
                with st.spinner("Unificando archivos..."):
                    # Ejecutar la unificación
                    df_unificado, mensaje = unificar_archivos(directorio, patron_archivos, nombre_hoja)
                    
                    if df_unificado is not None:
                        st.success(mensaje)
                        
                        # Mostrar una vista previa de los datos
                        st.write("### Vista previa de los datos unificados:")
                        st.dataframe(df_unificado.head(10))
                        
                        # Guardar el archivo unificado
                        archivo_guardado, msg_guardado = guardar_archivo_unificado(
                            df_unificado, directorio, nombre_salida, nombre_hoja_salida
                        )
                        
                        if archivo_guardado:
                            st.success(msg_guardado)
                            
                            # Guardar en la sesión para usar en la siguiente pestaña
                            st.session_state.df_unificado = df_unificado
                            st.session_state.archivo_unificado = archivo_guardado
                            
                            # Mostrar botón para descargar el archivo
                            with open(archivo_guardado, "rb") as file:
                                btn = st.download_button(
                                    label="Descargar archivo unificado",
                                    data=file,
                                    file_name=nombre_salida,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                )
                        else:
                            st.error(msg_guardado)
                    else:
                        st.error(mensaje)
    
    with tab2:
        st.header("Conexión a Tableau Server")
        
        # Verificar si ya existe un archivo unificado
        if not hasattr(st.session_state, 'archivo_unificado'):
            st.warning("⚠️ Primero debes unificar los archivos en la pestaña anterior.")
        else:
            st.success(f"✅ Archivo unificado listo: {os.path.basename(st.session_state.archivo_unificado)}")
            
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
                                    
                                    # Mostrar instrucciones
                                    st.write("### Instrucciones para actualización manual:")
                                    st.code(f"""
1. Inicia sesión en Tableau Server: {tableau_server}
2. Navega a la fuente de datos: {nombre_fuente_datos}
3. Selecciona 'Actualizar ahora' o 'Reemplazar fuente de datos'
4. Sube el archivo unificado: {os.path.basename(st.session_state.archivo_unificado)}
   (Ubicación completa: {st.session_state.archivo_unificado})
                                    """)
                                else:
                                    # Intentar actualización automática
                                    st.write("Solicitando actualización de extracción...")
                                    try:
                                        job = server.datasources.refresh(datasource.id)
                                        st.success(f"✅ Trabajo de actualización iniciado con ID: {job.id}")
                                    except Exception as e:
                                        st.error(f"Error al solicitar la actualización: {str(e)}")
                                        st.info("Se requiere actualización manual a través de la interfaz web")
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
        
        1. **Directorio**: Introduce la ruta completa donde están los archivos (por ejemplo, `C:\\Usuarios\\MiUsuario\\Documentos\\Datos`)
        2. **Patrón de archivos**: Introduce un patrón para seleccionar los archivos (por ejemplo, `OpsCenter_*.xls` seleccionará todos los archivos .xls que empiecen con "OpsCenter_")
        3. **Nombre de la hoja**: Indica el nombre exacto de la hoja que deseas leer en cada archivo
        4. **Nombre del archivo unificado**: Define cómo se llamará el archivo final
        5. **Nombre de la hoja de salida**: Define cómo se llamará la hoja en el archivo unificado
        
        #### Pestaña 2: Conectar a Tableau
        
        1. **URL del Tableau Server**: Introduce la URL completa del servidor Tableau
        2. **Configuración del sitio**: Marca si es el sitio predeterminado o introduce el ID del sitio
        3. **Autenticación**: Configura cómo te vas a autenticar (con o sin dominio)
        4. **Nombre de la fuente de datos**: Introduce el nombre exacto de la fuente de datos a actualizar
        
        ### Notas importantes
        
        - La aplicación guardará el archivo unificado en el mismo directorio donde están los archivos originales
        - La actualización automática solo funciona si el servidor Tableau tiene una versión de API 2.8 o superior
        - En caso de error, sigue las instrucciones para actualización manual
        """)
        
        st.info("Si tienes problemas para conectar a Tableau Server, verifica la URL y las credenciales. Si persisten los problemas con certificados SSL, marca la opción 'Deshabilitar verificación SSL'.")

if __name__ == "__main__":
    main()
