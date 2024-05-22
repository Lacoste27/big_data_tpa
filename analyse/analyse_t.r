library(RJDBC)
library(ggplot2)
library(ggfortify)
library(RColorBrewer)
library(dplyr)
library(rpart)
library(rpart.plot)

hive_jdbc_jar <- "C:/Vm/INSTALL_MV_BIGDATA_BOX/lib/hive-jdbc-3.1.3-standalone.jar"
hive_driver <- "org.apache.hive.jdbc.HiveDriver"
hive_url <- "jdbc:hive2://localhost:10000/"
drv <- JDBC(hive_driver, hive_jdbc_jar, "`")
conn <- dbConnect(drv, hive_url, "oracle", "welcome1")

query <- "SELECT * FROM concessionaire.client_immatriculation"
result <- dbGetQuery(conn, query)
clients_imma_df <- as.data.frame(result)
colnames(clients_imma_df) <- sub("^v_clients\\.", "", colnames(clients_imma_df))


# Check for columns with zero variance
zero_var_cols <- colnames(clients_imma_df)[apply(clients_imma_df, 2, var) == 0]

# Remove columns with zero variance
clients_imma_df_filtered <- clients_imma_df[, !(names(clients_imma_df) %in% zero_var_cols)]

selected_variables <- c(
    "immatriculation_puissance",
    "immatriculation_nbportes",
    "immatriculation_prix",
    "clients_age",
    "clients_sexe",
    "clients_situationfamiliale",
    "clients_nbenfantsacharge",
    "clients_deuxiemevoiture"
)
# Créer un nouveau dataframe avec seulement les variables sélectionnées
data_for_clustering <- clients_imma_df_filtered[selected_variables]

# Scale numeric columns
clients_imma_df_scaled <- scale(data_for_clustering[, sapply(data_for_clustering, is.numeric)])

wss <- (nrow(clients_imma_df_scaled) - 1) * sum(apply(clients_imma_df_scaled, 2, var))
for (i in 2:15) wss[i] <- sum(kmeans(clients_imma_df_scaled, centers = i)$withinss)
plot(1:15, wss, type = "b", xlab = "Nombre de clusters", ylab = "Somme des carrés")

# Supposons que le nombre optimal de clusters est 3
set.seed(123) # Pour la reproductibilité
kmeans_result <- kmeans(clients_imma_df_scaled, centers = 5)
clients_imma_df$cluster <- kmeans_result$cluster

# Affichage des caractéristiques moyennes par cluster
aggregate(clients_imma_df_scaled, by = list(cluster = kmeans_result$cluster), FUN = mean)

# Utilisation de PCA pour la visualisation
pca_result <- prcomp(clients_imma_df_scaled)
ggplot(clients_imma_df, aes(x = pca_result$x[, 1], y = pca_result$x[, 2], color = factor(kmeans_result$cluster))) +
    geom_point() +
    theme_minimal() +
    labs(x = "PC1", y = "PC2", color = "Cluster")

clients_imma_df

print(names(clients_imma_df))

mean_puissance_nbportes <- clients_imma_df %>%
    group_by(cluster) %>%
    summarise(
        mean_puissance = mean(immatriculation_puissance),
        mean_nbportes = mean(immatriculation_nbportes),
        mean_nbplaces = mean(immatriculation_nbplaces),
        mean_prix = mean(immatriculation_prix)
    )

# Print the result
print(mean_puissance_nbportes)

dominant_situation_familiale <- clients_imma_df %>%
    group_by(cluster, clients_situationfamiliale) %>%
    summarise(count = n()) %>%
    top_n(1, count) %>%
    select(cluster, clients_situationfamiliale)

print(dominant_situation_familiale)

# Define the vehicle categories corresponding to each cluster
vehicle_categories <- c("Citadine", "Berline", "Citadine", "SUV", "Sportive")

# Map the vehicle categories to the clusters in the DataFrame
clients_imma_df$vehicle_category <- vehicle_categories[clients_imma_df$cluster]

# Display the first few rows to verify the mapping
head(clients_imma_df)

# Selecting and printing the 'marque', 'nom', and 'vehicle_category' columns
selected_columns <- clients_imma_df[, c("immatriculation_marque", "immatriculation_nom", "vehicle_category")]
print(selected_columns)

ggplot(clients_imma_df, aes(x = factor(cluster), fill = vehicle_category)) +
    geom_bar() +
    theme_minimal() +
    labs(x = "Cluster", y = "Nombre de clients", fill = "Catégorie de véhicules") +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))

query <- "SELECT * FROM clients"
result <- dbGetQuery(conn, query)
clients_df <- as.data.frame(result)
colnames(clients_df) <- sub("^clients\\.", "", colnames(clients_df))
clients_df

clients_df_ <- merge(clients_df, clients_imma_df[, c("clients_immatriculation", "vehicle_category")], by.x = "immatriculation", by.y = "clients_immatriculation", all.x = TRUE)
clients_df_
print(names(clients_df_))

# Supposons que clients_df_ est votre DataFrame initial
# Sélection des caractéristiques des clients pour l'apprentissage, en excluant 'vehicle_category'
client_features <- clients_df_[, !(names(clients_df_) %in% c("immatriculation", "deuxiemevoiture"))]

# Vérification de la structure de client_features pour s'assurer que 'vehicle_category' est exclu
print(names(client_features))

str(clients_df_)

set.seed(123) # Pour la reproductibilité
indices <- sample(1:nrow(client_features), 0.7 * nrow(client_features))
train_set <- client_features[indices, ]
test_set <- client_features[-indices, ]

print(nrow(train_set))
print(nrow(test_set))

model <- rpart(vehicle_category ~ ., data = train_set, method = "class")

# Affichage du modèle
print(model)
prp(model)
# Prédiction sur l'ensemble de test
predictions <- predict(model, newdata = test_set, type = "class")

# Calcul de la matrice de confusion
confusion_matrix <- table(Predicted = predictions, Actual = test_set$vehicle_category)

# Affichage de la matrice de confusion
print(confusion_matrix)

# Calcul de l'exactitude
accuracy <- sum(diag(confusion_matrix)) / sum(confusion_matrix)
print(paste("Accuracy:", accuracy))


query <- "SELECT * FROM marketing_ext"
result <- dbGetQuery(conn, query)
marketing_df <- as.data.frame(result)
marketing_df
colnames(marketing_df) <- sub("^marketing_ext\\.", "", colnames(marketing_df))
marketing_df
