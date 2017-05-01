# nontao
Montar arquitectura de datos:

1- Instalar la Plataforma de Datos Hortonworks (HDP 2.5.0) siguiendo el manual de la propia web:
https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.5.0/bk_command-line-installation/content/ch_getting_ready_chapter.html

2- Activar el servicio NiFi:
https://github.com/abajwa-hw/ambari-nifi-service

3-Conectar los puertos de salida de NiFi a Spark para poder procesar los datos:
https://community.hortonworks.com/articles/84631/hdf-21-nifi-site-to-site-direct-streaming-to-spark.html

4- Instalar y configurar el conector de MongoDB para Hortonworks
https://docs.mongodb.com/ecosystem/tools/hadoop/
https://www.mongodb.com/blog/post/using-mongodb-hadoop-spark-part-1-introduction-setup
