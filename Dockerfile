FROM jupyter/datascience-notebook:latest

# Instalem FreeTDS i controladors ODBC per poder conectar-nos a les sybase de ccc
USER root
RUN apt-get update && apt-get install -y \
    freetds-bin \
    freetds-dev \
    tdsodbc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Creem/configurem /etc/odbcinst.ini per registrar FreeTDS
RUN printf '[FreeTDS]\nDescription=FreeTDS ODBC driver for Sybase and SQL Server\nDriver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nUsageCount=1\n' > /etc/odbcinst.ini

# Copiem el package db_connections al contenidor
COPY ./db_connections /home/jovyan/db_connections

# Temporalment fem servir root per poder canviar permisos
USER root

# Asegurem que l'usuari jovyan tingui permisos en el directori del package
RUN chown -R jovyan:users /home/jovyan/db_connections

# Canviem permisos del directori de treball (per seguretat)
RUN chmod -R 775 /home/jovyan/db_connections

# Canviem de nou a jovyan per les instruccions següents
USER jovyan

# Instalem les eines necesaries per construir el package
RUN pip install build

# Construim el pacakge mitjançant pyproject.toml
RUN python -m build /home/jovyan/db_connections

# Instalem el package des de l'arxiu .whl generat
RUN pip install /home/jovyan/db_connections/dist/*.whl

# Establim el directori de treball dins el contenidor
WORKDIR /home/jovyan

# Iniciem JupyterLab
CMD ["start.sh", "jupyter", "lab"]