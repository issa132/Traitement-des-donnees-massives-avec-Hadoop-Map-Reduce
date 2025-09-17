// cc NcdcRecordParserV2 A class for parsing weather records in NCDC format
package TitanicSurvie;
import org.apache.hadoop.io.Text;

// vv NcdcRecordParserV2
public class NcdcRecordParser {
	
	
	/*
  //Cette constante est utilisée pour identifier les enregistrements de température manquants. 
  //Si la température est égale à 9999, cela signifie qu'il n'y a pas de donnée valide.
  private static final int MISSING_TEMPERATURE = 9999;
  
  private String year;
  private int airTemperature;
  private String quality;
  
 
  */
	 private String sexe;
	  private int survie;
	    private boolean validRecord = false;
	    
	    
  public void parse(String record) {
	  
	  /*
    year = record.substring(15, 19);
   
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
// Si la température contient un signe plus (+), on doit l'ignorer car la méthode 
// Integer.parseInt() ne gère pas bien les signes au début des nombres dans certaines versions de Java
    if (record.charAt(87) == '+') { 
      airTemperatureString = record.substring(88, 92);
    } else {
      airTemperatureString = record.substring(87, 92);
    }
//La température est ensuite convertie en un entier (int) après avoir été extraite.
    airTemperature = Integer.parseInt(airTemperatureString);
    quality = record.substring(92, 93);
    
    */
	  
	  try {
          // Nettoyer la ligne
          record = record.trim();
          
          // Ignorer les lignes vides ou l'en-tête
          if (record.isEmpty() || record.toLowerCase().contains("survived") || 
              record.toLowerCase().contains("sex")) {
              validRecord = false;
              return;
          }
          // Diviser par virgule (simple split)
          String[] fields = record.split(",");  // chercher comment mettre parsecsvline
          
          // Vérifier qu'on a au moins les champs nécessaires
          if (fields.length < 5) {  // Au minimum: PassengerId,Survived,Pclass,Name,Sex
              validRecord = false;
              return;
          }
          
          // Récupérer les champs (positions typiques dans le CSV Titanic)
          // Position 1: Survived (0 ou 1)
          // Position 4: Sex (male/female)
          
          
          // jai change ceci 
          /*
          String survieStr = cleanField(fields[1]);  // 2ème colonne
          sexe = cleanField(fields[4]);              // 5ème colonne
          */ 
          String survieStr = fields[1].trim();  // 2ème colonne
          sexe =  fields[4].trim();              // 5ème colonne
          
          
          // Valider et convertir survie
          if (survieStr.equals("0") || survieStr.equals("1")) {
              survie = Integer.parseInt(survieStr);
              validRecord = true;
          } else {
              validRecord = false;
          }
          
      } catch (Exception e) {
          validRecord = false;
          System.err.println("Erreur parsing ligne: " + record);
      }  
          
	  /*
    String airTemperatureString;
	String survieString;
    survieString = record.substring(1); 
    sexe = record.substring(4);
    survie = Integer.parseInt(survieString);
    */
	  
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
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  */
  
  public String getSexe() {
	    return sexe;
	  }

	  public int getSurvie() {
	    return survie;
	  }
	  
	// Ajoutez cette méthode dans votre classe NcdcRecordParser
	  private String cleanField(String field) {
	      if (field == null) return "";
	      return field.trim().replace("\"", "");
	  }
	// Vérifier si l'enregistrement est valide
	    public boolean isValidRecord() {
	        return validRecord;
	    }
	    
	  
}
// ^^ NcdcRecordParserV2
