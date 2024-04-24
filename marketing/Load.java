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