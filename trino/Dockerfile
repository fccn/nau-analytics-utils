FROM trinodb/trino:478

# Copia o driver JDBC do MySQL para o plugin do Iceberg
# (assume que o ficheiro .jar está ao lado deste Dockerfile)
ADD /trino/mysql-connector-j.jar /usr/lib/trino/plugin/iceberg/