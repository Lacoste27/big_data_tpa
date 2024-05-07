library(RJDBC)
library(ggplot2)
library(cluster)
hive_jdbc_jar <- "C:/Vm/INSTALL_MV_BIGDATA_BOX/lib/hive-jdbc-3.1.3-standalone.jar"
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000/"
drv <- JDBC(hive_driver, hive_jdbc_jar)
conn <- dbConnect(drv, hive_url, "oracle", "welcome1")
client <- dbGetQuery(conn, "SELECT * FROM concessionaire.client_ext")
immatriculation  <- dbGetQuery(conn, "SELECT TOP 100000 * FROM concessionaire.immatriculation_ext")
View(client)
