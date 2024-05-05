library(RJDBC)
hive_jdbc_jar <- "C:/Vm/INSTALL_MV_BIGDATA_BOX/lib/hive-jdbc-3.1.3-standalone.jar"
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000"
drv <- JDBC(hive_driver, hive_jdbc_jar)
conn <- dbConnect(drv, hive_url, "oracle", "welcome1")
marketing <- dbGetQuery(conn, "SELECT * FROM concessionaire.marketing_ext")
catalogue <- dbGetQuery(conn, "SELECT * FROM concessionaire.catalogue_ext")
View(catalogue)
table(catalogue$catalogue_ext.marque)
pie(table(catalogue$catalogue_ext.occasion), main = "Repartition des classes")
qplot(catalogue$catalogue_ext.marque, data=catalogue, fill=catalogue$catalogue_ext.occasion)
table(catalogue$catalogue_ext.occasion)
