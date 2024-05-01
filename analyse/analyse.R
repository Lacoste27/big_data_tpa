library(JDBC)
hive_jdbc_jar <- "/usr/local/hive/jdbc/hive-jdbc-3.1.3-standalone.jar"
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000"
drv <- JDBC(hive_driver, hive_jdbc_jar)
conn <- dbConnect(drv, hive_url, "vagrant", "")
show_databases <- dbGetQuery(conn, "show databases")
print(show_databases);
