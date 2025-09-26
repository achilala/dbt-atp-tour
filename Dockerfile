FROM openjdk:26-slim

ENV MB_PLUGINS_DIR=/home/plugins/

ADD https://downloads.metabase.com/v0.56.6.x/metabase.jar /home
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/0.4.1/duckdb.metabase-driver.jar /home/plugins/

RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar

CMD ["java", "-jar", "/home/metabase.jar"]