# Projet TPA

## Etape :

- Alimentation du data lake (imporation des données `Immatriculation` et `Client` dans mongodb)

```bash
# création de la base voiture dans mongodb
> use voiture

# création de la collection immatriculation
> db.createCollection('immatriculation');

# création de la collection client
> db.createCollection('client');
> exit

# imporation du donnée immatriculation.csv avec mongoimport
mongoimport --db voiture  --collection immatriculation  --type csv --headerline --file /vagrant/BigData/Groupe_TPA_13/Immatriculations.csv

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

- Alimentation du data lake (imporation des données `CO2.csv` et `Catalogue.csv` dans HDFS)

```bash
hadoop fs -mkdir tpa                                                    # création du dossier tpa  

hadoop fs -put /vagrant/BigData/Groupe_TPA_13/CO2.csv tpa               # copier le fichier CO2.csv dans le dossier tpa dans hdfs
hadoop fs -put /vagrant/BigData/Groupe_TPA_13/Catalogue.csv tpa         # copier le fichier Catalogue.csv dans le dossier tpa dans hdfs          
```