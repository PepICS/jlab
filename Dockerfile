# Usa la imagen base de JupyterLab (o cualquier imagen oficial de Jupyter)
FROM jupyter/datascience-notebook:latest

# Instalar FreeTDS y controladores ODBC (necesario para conectar a sybase (centricity) des de python)
USER root
RUN apt-get update && apt-get install -y \
    freetds-bin \
    freetds-dev \
    tdsodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Crear o configurar /etc/odbcinst.ini para registrar FreeTDS
RUN printf '[FreeTDS]\nDescription=FreeTDS ODBC driver for Sybase and SQL Server\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nUsageCount=1\n' > /etc/odbcinst.ini

# Copiar el paquete db_connections al contenedor
COPY ./db_connections /home/jovyan/db_connections

# Temporalmente usar root para poder cambiar los permisos
USER root

# Asegurarse de que el usuario jovyan tenga permisos en el directorio del paquete
RUN chown -R jovyan:users /home/jovyan/db_connections

# Cambiar permisos del directorio de trabajo (por seguridad)
RUN chmod -R 775 /home/jovyan/db_connections

# Cambiar de nuevo a jovyan para las siguientes instrucciones
USER jovyan

# Instalar las herramientas necesarias para construir el paquete
RUN pip install build

# Construir el paquete usando pyproject.toml
RUN python -m build /home/jovyan/db_connections

# Instalar el paquete desde el archivo .whl generado
RUN pip install /home/jovyan/db_connections/dist/*.whl

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /home/jovyan

# Comando para iniciar JupyterLab
CMD ["start.sh", "jupyter", "lab"]