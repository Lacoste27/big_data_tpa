library(RJDBC)
library(ggplot2)
library(ggfortify)
library(RColorBrewer)
library(dplyr)
library(rpart)
library(rpart.plot)
library(cluster)
library(ggplot2)

hive_jdbc_jar <- "C:/Vm/INSTALL_MV_BIGDATA_BOX/lib/hive-jdbc-3.1.3-standalone.jar"
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000/"
drv <- JDBC(hive_driver, hive_jdbc_jar, "`")
conn <- dbConnect(drv, hive_url, "oracle", "welcome1")

query <- "SELECT * FROM concessionaire.v_clients"
clients <- dbGetQuery(conn, query)

mode(clients)

colnames(clients) <- sub("^v_clients\\.", "", colnames(clients))

clients$clients_nbenfantacharge <- factor(as.factor(clients$clients_nbenfantacharge), ordered=TRUE)
clients$clients_situationfamiliale <- factor(as.factor(clients$clients_situationfamiliale), ordered=TRUE)
clients$clients_sexe <- factor(as.factor(clients$clients_sexe), ordered=TRUE)

clients$immatriculation_nbportes <- factor(as.factor(clients$immatriculation_nbportes), ordered=TRUE)
clients$immatriculation_longueur <- factor(as.factor(clients$immatriculation_longueur), ordered=TRUE)
clients$immatriculation_nbportes <- factor(as.factor(clients$immatriculation_nbportes), ordered=TRUE)
clients$immatriculation_occasion <- factor(as.factor(clients$immatriculation_occasion), ordered=TRUE)

clients <- clients[,-10]

client_echantillon <- clients[1:25000,]

dmatrix <- daisy(client_echantillon)
km4 <- kmeans(dmatrix, 3)

qplot(immatriculation_nbportes, as.factor(km4$cluster), data=client_echantillon, color=km4$cluster) + geom_jitter(width = 0.1, height = 0.1)
qplot(immatriculation_longueur, as.factor(km4$cluster), data=client_echantillon, color=km4$cluster) + geom_jitter(width = 0.1, height = 0.1)

table(km4$cluster, client_echantillon$immatriculation_nbportes)
table(km4$cluster, client_echantillon$immatriculation_longueur )

client_echantillon$cluster <- km4$cluster

vehicle_categories <- c("Citadine", "Berline", "Familiale", "SUV", "Sportive")

client_echantillon$categorie <- vehicle_categories[client_echantillon$cluster]
