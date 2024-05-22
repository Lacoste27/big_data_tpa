# Projet TPA

## Alimentation

- Alimentation du data lake (imporation des données `Immatriculation` et `Client` dans mongodb)

```bash
# création de la base voiture dans mongodb
> use voiture

# création de la collection client
> db.createCollection('client');
> exit

# imporation du donnée client_3.csv et client_12.csv avec mongoimport
mongoimport --db voiture  --collection client  --type csv --headerline --file /vagrant/BigData/Groupe_TPA_13/Clients_12.csv

mongoimport --db voiture  --collection client  --type csv --headerline --file /vagrant/BigData/Groupe_TPA_13/Clients_3.csv
```
- Alimentation du data lake (imporation des données `Marketing.csv` dans Oracle NoSql)
```bash
# connexion à la base avec kv base kvstore
java -jar $KVHOME/lib/sql.jar -helper-hosts localhost:5000 -store kvstore
```

```sql
-- Création de la table Marketing dans Oracle Nosql
execute 'create table marketing(
id INTEGER,
age  INTEGER ,  
sexe   STRING, 
taux  INTEGER, 
situationFamiliale  STRING, 
nbEnfantsAcharge     INTEGER, 
has2emeVoiture     Boolean, 
PRIMARY  KEY (id))';
```

```java
// Load.java

package marketing;

import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.KVStore;
import oracle.kv.FaultException;
import oracle.kv.StatementResult;
import oracle.kv.table.TableAPI;
import oracle.kv.table.Table;
import oracle.kv.table.Row;
import oracle.kv.table.PrimaryKey;
import oracle.kv.ConsistencyException;
import oracle.kv.RequestTimeoutException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Load {
    private final KVStore store;
    private final String storename = "kvstore";
    private final String host = "localhost";
    private final String port = "5000";
    private final String table = "marketing";

    public Load() {
        store = KVStoreFactory.getStore(new KVStoreConfig(storename, host + ":" + port));
    }

    public static void main(String[] args) throws IOException {
        String filepath = "";

        if(args.length != 0){
            filepath = args[0];
        } else {
            System.out.println("Veuillez mentionner le chemin du fichier Marketing.csv");
        }

        Load load = new Load();

        ArrayList<Marketing> list = load.loadMarketingFromCsvFile(filepath);
        load.dropMarketingTable();
        load.createMarketingTable();

        System.out.println("------------- add new marketing --------------");

        for (Marketing marketing : list) {
            load.insertMarketing(marketing);
        }
    }

    public void insertMarketing(Marketing marketing) {
        try {

            TableAPI tableApi = store.getTableAPI();
            Table table = tableApi.getTable("marketing");

            Row row = table.createRow();

            marketing.marketingToRow(row);

            tableApi.put(row, null, null);

        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }

    public void createMarketingTable() {
        String statement = " create table marketing("
                + "  id INTEGER,"
                + "  age  INTEGER ,"
                + "  sexe   STRING,"
                + "  taux  INTEGER,"
                + "  situationFamiliale  STRING,"
                + "  nbEnfantsAcharge     INTEGER,"
                + "  has2emeVoiture     Boolean,"
                + "  PRIMARY  KEY (id)) ";
        TableAPI tableAPI = store.getTableAPI();
        StatementResult result = null;

        try {
            /*
             * Add a table to the database.
             * Execute this statement asynchronously.
             */

            result = store.executeSync(statement);
            showResult(result, statement);

        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }

    public void dropMarketingTable() {
        String statement = "drop table marketing";
        TableAPI tableAPI = store.getTableAPI();
        StatementResult result = null;

        try {
            /*
             * delete a table to the database.
             * Execute this statement asynchronously.
             */

            result = store.executeSync(statement);
            showResult(result, statement);
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid statement:\n" + e.getMessage());
        } catch (FaultException e) {
            System.out.println("Statement couldn't be executed, please retry: " + e);
        }
    }

    public void showResult(StatementResult result, String statement) {
        if (result.isSuccessful()) {
            System.out.println("Statement was successful:\n\t" +
                    statement);
            System.out.println("Results:\n\t" + result.getInfo());
        } else if (result.isCancelled()) {
            System.out.println("Statement was cancelled:\n\t" +
                    statement);
        } else {
            /*
             * statement was not successful: may be in error, or may still
             * be in progress.
             */
            if (result.isDone()) {
                System.out.println("Statement failed:\n\t" + statement);
                System.out.println("Problem:\n\t" +
                        result.getErrorMessage());
            } else {

                System.out.println("Statement in progress:\n\t" +
                        statement);
                System.out.println("Status:\n\t" + result.getInfo());
            }
        }
    }

    private ArrayList<Marketing> loadMarketingFromCsvFile(String filename) throws IOException {
        InputStreamReader ipsr;
        BufferedReader br = null;
        InputStream ips;
        String ligne;

        ArrayList<Marketing> result = new ArrayList<>();
        try {

            ips = new FileInputStream(filename);
            ipsr = new InputStreamReader(ips);
            br = new BufferedReader(ipsr);

            int id = 0;
            int line = 0;

            while ((ligne = br.readLine()) != null) {
                if (line != 0) {
                    ArrayList<String> marketingRecord = new ArrayList<String>();
                    StringTokenizer val = new StringTokenizer(ligne, ",");

                    while (val.hasMoreTokens()) {
                        marketingRecord.add(val.nextToken().toString());
                    }

                    int age = Integer.parseInt(marketingRecord.get(0));
                    String sexe = marketingRecord.get(1);
                    int taux = Integer.parseInt(marketingRecord.get(2));
                    String situationFamiliale = marketingRecord.get(3);
                    int nbEnfantsAcharge = Integer.parseInt(marketingRecord.get(4));
                    boolean has2emeVoiture = Boolean.parseBoolean(marketingRecord.get(5));

                    Marketing marketing = new Marketing();
                    marketing.setId(id);
                    marketing.setAge(age);
                    marketing.setSexe(sexe);
                    marketing.setTaux(taux);
                    marketing.setSituationFamiliale(situationFamiliale);
                    marketing.setNbEnfantAcharge(nbEnfantsAcharge);
                    marketing.setHas2emeVoiture(has2emeVoiture);

                    result.add(marketing);
                }

                line++;
                id++;
            }

            return result;
        } catch (Exception e) {
            throw e;
        }
    }
}
```

```java
// Marketing.java

package marketing;

import oracle.kv.table.Row;

/**
 * Marketing
 */
public class Marketing {
    private int id;
    private int age;
    private String sexe;
    private int taux;
    private String situationFamiliale;
    private int nbEnfantAcharge;
    private boolean has2emeVoiture;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getSexe() {
        return sexe;
    }

    public void setSexe(String sexe) {
        this.sexe = sexe;
    }

    public int getTaux() {
        return taux;
    }

    public void setTaux(int taux) {
        this.taux = taux;
    }

    public String getSituationFamiliale() {
        return situationFamiliale;
    }

    public void setSituationFamiliale(String situationFamiliale) {
        this.situationFamiliale = situationFamiliale;
    }

    public int getNbEnfantAcharge() {
        return nbEnfantAcharge;
    }

    public void setNbEnfantAcharge(int nbEnfantAcharge) {
        this.nbEnfantAcharge = nbEnfantAcharge;
    }

    public boolean isHas2emeVoiture() {
        return has2emeVoiture;
    }

    public void setHas2emeVoiture(boolean has2emeVoiture) {
        this.has2emeVoiture = has2emeVoiture;
    }

    public void marketingToRow(Row row) {
        row.put("id", this.getId());
        row.put("age", this.getAge());
        row.put("sexe", this.getSexe());
        row.put("taux", this.getTaux());
        row.put("situationFamiliale", this.getSituationFamiliale());
        row.put("nbEnfantsAcharge", this.getNbEnfantAcharge());
        row.put("has2emeVoiture", this.isHas2emeVoiture());
    }
}
```

```bash
# add variable
export PROJETHOME=/vagrant/BigData/Projet

# compilation du programme pour l'importation du donnée marketing
javac -g -cp $KVHOME/lib/kvclient.jar:$PROJETHOME $PROJETHOME/marketing/Marketing.java
javac -g -cp $KVHOME/lib/kvclient.jar:$PROJETHOME $PROJETHOME/marketing/Load.java

# exécution 
java -Xmx256m -Xms256m  -cp $KVHOME/lib/kvclient.jar:$PROJETHOME marketing.Load
```

- Alimentation du data lake (imporation des données `Immatriculation.csv`, `CO2.csv` et `Catalogue.csv` dans HDFS)

```bash
hadoop fs -mkdir tpa                                                                                # création du dossier tpa 
hadoop fs -mkdir tpa/immatriculation                                                                # création du dossier tpa/immatriculation 

hadoop fs -put /vagrant/BigData/Groupe_TPA_13/Immatriculations.csv tpa/immatriculation               # copier le fichier Immatriculation dans le dossier tpa dans hdfs
hadoop fs -put /vagrant/BigData/Groupe_TPA_13/CO2.csv tpa                                           # copier le fichier CO2.csv dans le dossier tpa dans hdfs
hadoop fs -put /vagrant/BigData/Groupe_TPA_13/Catalogue.csv tpa                                     # copier le fichier Catalogue.csv dans le dossier tpa dans hdfs          
```

-  Création des tables externes (MARKETING_EXT) HIVE pointant vers les tables physiques Oracle Nosql
```bash
# start hadoop hive
nohup hive --service metastore > /dev/null &
nohup hiveserver2 > /dev/null &

# enter on beeline
beeline

# connect to hive
!connect jdbc:hive2://localhost:10000
Enter username for jdbc:hive2://localhost:10000: oracle
Enter password for jdbc:hive2://localhost:10000: ******** (welcome1)
```

```sql
-- création de la base de donnée concecssionaire
CREATE DATABASE IF NOT EXISTS concessionaire;
```

```sql
USE concessionaire;

DROP TABLE MARKETING_EXT;

-- Création de la table externe MARKETING_EXT
CREATE EXTERNAL TABLE  MARKETING_EXT  (
    id int, 
    age int, 
    sexe string,
    taux int,
    situationFamiliale string,
    nbEnfantsAcharge int,
    has2emeVoiture boolean
)
STORED BY 'oracle.kv.hadoop.hive.table.TableStorageHandler'
TBLPROPERTIES (
"oracle.kv.kvstore" = "kvstore",
"oracle.kv.hosts" = "localhost:5000", 
"oracle.kv.hadoop.hosts" = "localhost/127.0.0.1", 
"oracle.kv.tableName" = "marketing");
```

-  Création des tables externes (IMMATRICULATION_EXT) HIVE pointant vers les tables physiques Mongodb
```bash
# start hadoop hive
nohup hive --service metastore > /dev/null &
nohup hiveserver2 > /dev/null &

# enter on beeline
beeline

# connect to hive
!connect jdbc:hive2://localhost:10000/
Enter username for jdbc:hive2://localhost:10000: oracle
Enter password for jdbc:hive2://localhost:10000: ******** (welcome1)
```

```sql
-- Création de la table externe IMMATRICULATION_EXT
USE concessionaire;

DROP TABLE IMMATRICULATION_EXT;

CREATE EXTERNAL TABLE  IMMATRICULATION_EXT  (
    immatriculation string,
    marque string,
    nom string,
    puissance int,
    longueur string,
    nbPlaces int,
    nbPortes int,
    couleur string,
    occasion string,
    prix int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'tpa/immatriculation';

```
- Transformed catalog data with `catalogue/catalogue.py`
```python
#!/usr/bin/env python
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , regexp_replace , split ,initcap , upper , avg , trim , lit , coalesce , round


# ## 1 - Creating spark session 
spark = SparkSession.builder.appName("Catalogue_CO2").getOrCreate()

# ## 2 - Importing data from hdfs 
co2_df = spark.read.csv("tpa/CO2", header=True, inferSchema=True)
catalog_df = spark.read.csv("tpa/Catalogue", header=True, inferSchema=True)


# ## 3 - Processing CO2
# fix Bonus / Malus data by removing everything after the euro sign 
co2_df = co2_df.withColumn("Bonus / Malus", regexp_replace("Bonus / Malus", r'€.*', '€'))
# remove all data that have invalide values 
co2_df = co2_df.filter(col("Bonus / Malus") != "-")


# add create Marque colunm from Marque / Model by taking the first word 
co2_df = co2_df.withColumn("Marque", split(co2_df["Marque / Modele"], " ")[0])
# create Model colunm from Marque / Model col by taking the 
co2_df = co2_df.withColumn("Model", split(co2_df["Marque / Modele"], " ")[1])
# delete the marque / model col as it 
co2_df = co2_df.drop("Marque / Modele")


# we take all the marques catalogue
marques_catalogue = catalog_df.select("marque").distinct().withColumnRenamed("marque", "Marque")
# we capitalise them in order to be able to compared it with data from co2
marques_catalogue = marques_catalogue.withColumn("Marque", upper(col("Marque")))

# We filter CO2 to only have data having marques in Catalogues 
co2_df = co2_df.alias("co2").join(marques_catalogue.alias("marques"), "Marque")

# we process Bonus / Malus and Cout energie to be able to perform operations as number
# remove euro , + , white spaces 
processed_co2 = co2_df.withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '€', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '€', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\\+', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '\u00A0', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\u00A0', ''))
# we convert Bonus / Malus , Cout energie to float 
processed_co2 = processed_co2.withColumn('Bonus / Malus', processed_co2['Bonus / Malus'].cast('float')) \
    .withColumn('Cout energie', processed_co2['Cout energie'].cast('float'))

# we process Bonus / Malus and Cout energie to be able to perform operations as number
# remove euro , + , white spaces 
processed_co2 = co2_df.withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '€', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '€', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\\+', '')) \
    .withColumn('Cout energie', regexp_replace(col('Cout energie'), '\u00A0', '')) \
    .withColumn('Bonus / Malus', regexp_replace(col('Bonus / Malus'), '\u00A0', ''))
# we convert Bonus / Malus , Cout energie to float 
processed_co2 = processed_co2.withColumn('Bonus / Malus', processed_co2['Bonus / Malus'].cast('float')) \
    .withColumn('Cout energie', processed_co2['Cout energie'].cast('float'))

# We take the mean of Bonus / Malus , Regets CO2 , Cout Energie for each brand 
moyennes_par_marque = processed_co2.groupBy('Marque').agg(
    round(avg('Bonus / Malus'), 2).alias('Bonus / Malus'),
    round(avg('Rejets CO2 g/km'), 2).alias('Rejets CO2 g/km'),
    round(avg('Cout Energie'), 2).alias('Cout Energie')
)

# we take the mean of Bonus / Malus , Rehet CO2 and Cout Energie of all brand 
moyenne_toutes_marques_df = processed_co2.groupBy().agg(
    round(avg("Bonus / Malus"),2).alias("Moyenne_Bonus_Malus"),
    round(avg("Rejets CO2 g/km"),2).alias("Moyenne_Rejets_CO2"),
    round(avg("Cout Energie"),2).alias("Moyenne_Cout_Energie")
)

# we add column from bonus / malus , rejets co2 and cout energie into catalog
# we leave values null for brandes that are not in co2 
df_resultat = catalog_df.alias("catalogue").join(
    moyennes_par_marque.withColumnRenamed("Marque", "Marque_moyenne").select("Marque_moyenne", "Bonus / Malus", "Rejets CO2 g/km", "Cout Energie"),
    (upper(col("catalogue.marque")) == col("Marque_moyenne")),
    "left_outer"
).drop("Marque_moyenne")

moyenne_values = moyenne_toutes_marques_df.first()
# we set the values missing brand in co2 to the mean of all brands
updated_catalog_df = df_resultat.withColumn("Bonus / Malus", coalesce(df_resultat["Bonus / Malus"], lit(moyenne_values["Moyenne_Bonus_Malus"]))) \
    .withColumn("Rejets CO2 g/km", coalesce(df_resultat["Rejets CO2 g/km"], lit(moyenne_values["Moyenne_Rejets_CO2"]))) \
    .withColumn("Cout Energie", coalesce(df_resultat["Cout Energie"], lit(moyenne_values["Moyenne_Cout_Energie"])))
updated_catalog_df.show()

updated_catalog_df.repartition(1).write.csv("tpa/transformed_catalog" , mode="overwrite" , header=True)


spark.stop()

print("-------- catalog transfomed ----------")
```
- Création du table externes (CATALOGUE_EXT) HIVE pointant vers le fichier physiques HDFS
```sql
USE concessionaire;

DROP TABLE CATALOGUE_EXT;

CREATE EXTERNAL TABLE  CATALOGUE_EXT  (
    marque string, 
    nom string,  
    puissance string, 
    longueur  string,
    nbPlaces int,
    nbPortes int,
    couleur string,
    occasion string,
    prix int,
    bonusMalus float,
    rejetsCO2 float,
    coutEnergie float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 'tpa/transformed_catalog';
```

- Création de la table interne client dans HIVE
```sql
CREATE TABLE  CLIENTS  (
    age int,
    sexe string,
    taux int,
    situationFamiliale string,
    nbEnfantsACharge int,
    deuxiemeVoiture boolean,
    immatriculation string
);
```

## Analyse

- Création d'un view `client_immatriculation` pour la fusion des données client et immatriculation
```sql
CREATE VIEW v_clients AS SELECT c.age clients_age, c.sexe clients_sexe, c.taux clients_taux , c.situationFamiliale clients_situationfamiliale, c.nbenfantsacharge clients_nbenfantacharge , c.deuxiemeVoiture clients_deuxiemevoiture, i.marque immatriculation_marque, i.nom immatriculation_nom, i.puissance immatriculation_puissance, i.longueur immatriculation_longueur, i.nbplaces immatriculation_nbplaces, i.nbportes immatriculation_nbportes , i.couleur immatriculation_couleur, i.occasion immatriculation_occasion, i.prix immatriculation_prix, i.immatriculation FROM clients AS C JOIN immatriculation_ext i ON(i.immatriculation=c.immatriculation);
```

- Insértion des résultats dans Oracle depuis R
``` bash
# connect to oracle database 

sudo -su oracle
sqlplus /nolog
```

```sql
-- se connecter avec compte system pour créer un utilisateur mbds
connect system@ORCLPDB/Welcome1

-- création de l'utilisateur MBDS
create user MBDS identified by PassMbds
default tablespace users
temporary tablespace temp;

grant dba to MBDS;

-- ALTER USER MBDS QUOTA UNLIMITED ON USERS;

revoke unlimited tablespace from MBDS;

-- se connecter avec le compte MBDS
connect MBDS@ORCLPDB1/PassMbds;

-- création de la table de résultat des prédictions
CREATE Table Marketing_Result (
    age  INTEGER,  
    sexe   VARCHAR(5), 
    taux  INTEGER, 
    situationFamiliale  VARCHAR(50), 
    nbEnfantsAcharge     INTEGER, 
    has2emeVoiture     VARCHAR(10),
    categorie VARCHAR(50)
);


INSERT INTO Marketing_Result VALUES(10, 'F', 230, 'Célibataire',2, 'true','Familiale');
```

```R
library('ROracle')
driver <- dbDriver("Oracle")
ORCL <-"
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))
    (CONNECT_DATA =
      (SERVICE_NAME = ORCLPDB)
    )
  )
"
connection <- dbConnect(driver, username = "MBDS", password = "PassMbds",dbname=ORCL) 
```
