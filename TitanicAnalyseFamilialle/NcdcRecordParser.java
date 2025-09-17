// cc NcdcRecordParserV2 A class for parsing weather records in NCDC format
package TitanicAnalyseFamilialle;
import org.apache.hadoop.io.Text;

// vv NcdcRecordParserV2
public class NcdcRecordParser {
  //Cette constante est utilisée pour identifier les enregistrements de température manquants. 
  //Si la température est égale à 9999, cela signifie qu'il n'y a pas de donnée valide.
  private static final int MISSING_TEMPERATURE = 9999;
  
  private String year;
  private int airTemperature;
  private String quality;
  
  
  private String nom; 
  private String sexe;
  private String age;
  
  private int survie;
  private int classe;
 
  private int sibsp;
  private int parch;
  

  
  public void parse(String record) {
	  
	  
	  String line = record.toString().trim();
	  
	  /*
    year = record.substring(15, 19);
    String airTemperatureString;
    */
    
      // Ignorer la ligne d'en-tête
      if (line.startsWith("PassengerId")) {
          return;
      }
      
      
      /*// pour le moment j'ai enleve ca pour voir si ca fonctionne 
      // Parser CSV en tenant compte des guillemets
      String[] fields = parseCSVLine(record.trim());
            if (fields.length >= 8) {
      */
       
      String[] fields = parseCSVLigne(line);
      
      if (fields.length >= 8) {
          try { 
      
    String survieString; 
    String classeString;
    String classeAge;
    String sibspString;
    String parchString;
    
    /*
 // Colonnes importantes pour l'analyse familiale
    int survived = Integer.parseInt(fields[1].trim());  // Survived
    int pclass = Integer.parseInt(fields[2].trim());    // Pclass
    String name = fields[3].trim();                     // Name
    String sex = fields[4].trim();                      // Sex
    String ageStr = fields[5].trim();                   // Age
    int sibSp = Integer.parseInt(fields[6].trim());     // SibSp (siblings/spouses)
    int parch = Integer.parseInt(fields[7].trim());     // Parch (parents/children)
    */

    
    /*
    survie = Integer.parseInt(fields[1].trim());
    classe =  Integer.parseInt(fields[2].trim());
    nom =  fields[3].trim();
    sexe = fields[4].trim();  
    age = fields[5].trim(); 
    sibsp = Integer.parseInt(fields[6].trim());
    parch = Integer.parseInt(fields[7].trim());
    
        */ 
    
    age = record.substring(5);
    
    survieString = record.substring(1).trim(); 
    classeString = record.substring(2).trim();
    sibspString = record.substring(6).trim();
    parchString = record.substring(7).trim();
     
      
    classe = Integer.parseInt(classeString);
    //age = Integer.parseInt(classeAge);  //chercher comment supprimer ceci pour qu on puisse recuper lage en string    
    sibsp = Integer.parseInt(sibspString);
    parch = Integer.parseInt(parchString);
    nom = record.substring(3).trim();
    sexe = record.substring(4).trim();    
    
    survie = Integer.parseInt(survieString);
    

    
    
    
          } catch (NumberFormatException e) {
              // Initialiser avec des valeurs par défaut si parsing échoue
        	  
        	  //ignorer les lignes mal formees 
        	  
        	  /*
              survie = 0;
              classe = 3;
              nom = "Unknown";
              sexe = "unknown";
              age = "";
              sibsp = 0;
              parch = 0;
              */ 
        	  
        	  
          }
          
      }
    
    
  }
  //Cette méthode est une surcharge de la méthode précédente
//La méthode convertit simplement l'objet Text en une chaîne de caractères et appelle 
//la méthode parse(String record) pour analyser l'enregistrement.
  public void parse(Text record) {
    parse(record.toString());
  }
  
  /*
//La qualité de l'enregistrement doit correspondre à un caractère dans le jeu de caractères [01459]. 
//Cela garantit que l'enregistrement est valide (les valeurs possibles pour la qualité sont 0, 1, 4, 5, ou 9, selon le format NCDC).
  public boolean isValidTemperature() {
    return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
  }
  
  */
  
  
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  
 
  public String getSexe() {
	    return sexe;
	  }

  
  public int getSurvie() {
	    return survie;
	  }
	  
  
  public int getClasse() {
	    return classe;
	  }

  
  
  /*
  public String getAge() {
	    return age;
	  }
	  
	  
	  */ 
  
  public int getAge() {
      try {
          return age.isEmpty() ? -1 : (int) Double.parseDouble(age);
      } catch (NumberFormatException e) {
          return -1; // Âge inconnu
      }
  }
  
  public String getAgeString() {
      return age;
  }
  

  public String getNom() {
	    return nom;
	  }

  public int getSisp() {
	    return sibsp;
	  }
  
  public int getParch() {
	    return parch;
	  }

  
  
 
  
   // pour le moment je vais enlever ceci et voir chercher comment obtenir les filds ou parser un 
  // dossier csv 
  private String[] parseCSVLigne(String line) {
      java.util.List<String> result = new java.util.ArrayList<>();
      boolean inQuotes = false;
      StringBuilder field = new StringBuilder();
      
      for (int i = 0; i < line.length(); i++) {
          char c = line.charAt(i);
          
          if (c == '"') {
              inQuotes = !inQuotes;
          } else if (c == ',' && !inQuotes) {
              result.add(field.toString());
              field.setLength(0);
          } else {
              field.append(c);
          }
      }
      result.add(field.toString());
      
      return result.toArray(new String[0]);
  }
  
   
  
  

  
}
// ^^ NcdcRecordParserV2
