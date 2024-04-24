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