package TitanicAnalyseFamilialle;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//public class TitanicClassAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
public class TitanicAnalyseFamilleMapper extends Mapper<LongWritable, Text, Text, Text>  {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private Text outputkey = new Text();
    //private IntWritable outputvalue = new IntWritable();
    private Text outputvalue = new Text();
    
    
    private Text cleSegmente = new Text();
    private Text InfoPassenger = new Text(); 
    private Text typeFamille = new Text();
  
    
     
    @Override
    protected void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
    	
        String line = value.toString().trim();
        
        // Ignorer la ligne d'en-tête
        if (line.startsWith("PassengerId") || line.isEmpty()) {
            return;
        }
        
        try {
            // Parser le CSV (attention aux virgules dans les guillemets)
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            
            if (fields.length >= 7) {
            	
            	

                
                
        // for (Text value : values) {
      // ici il faut faire une boucle tant que nous avons les valeurs... 
       //parser.parse(value);
       
    
      //if (parser.isValidTemperature()) {
    	  
        ///*[*/context.write(new IntPair(parser.getYearInt(),
            //parser.getAirTemperature()), NullWritable.get());/*]*/ 
        
        ///*[*/context.write(new Text(parser.getYearInt()), 
        		//new IntWritable(parser.getAirTemperature()));/*]*/
    	  
    	  /*
    	 String year = parser.getYear();
    	 int temperature = parser.getAirTemperature();
    	 
    	  
    	 String sexe = parser.getSexe();
    	     	
      
    	 int survie = parser.getSurvie();

    	 int classe = parser.getClasse();
 
    	 String age = parser.getAgeString();
    	 
    	 int sibSp = parser.getSisp();
    	 
    	 int parch = parser.getParch();
    	 
    	 String sexe = parser.getSexe();
    	 
    	 String nom = parser.getNom();
          */
    	 
         String sexe = fields[4].replace("\"", "").trim(); // Colonne Sex
         int survie = Integer.parseInt(fields[1].trim());   // Colonne Survived
         int classe = Integer.parseInt(fields[2].trim());
         String age =  fields[5].trim();
         int sibSp = Integer.parseInt(fields[6].trim());
         int parch = Integer.parseInt(fields[7].trim());
         String nom = fields[3].trim();		 
          
          
          
           
         
         //age = Integer.parseInt(classeAge);  //chercher comment supprimer ceci pour qu on puisse recuper lage en string    
         
         
         /*
         parch = Integer.parseInt(parchString);
         nom = record.substring(3).trim();
        
         
         */
         
         
    	 //Text outputkey = new Text();
    	 
    	 
    	 /*
    	 outputkey.set(String.valueOf(classe));
    	 outputvalue.set(String.valueOf(age));
    	 
    	 */ 
    	 
    	 /*
         String ageIntervalle;
         ageIntervalle = getAgeGroup(Integer.parseInt(age));
 
         if (ageIntervalle == null ||ageIntervalle ) {
             ageGroup = "Unknown";
         } else {
             double age = Double.parseDouble(ageStr);
             ageIntervalle = getAgeGroup(age);
         }
         
         */
         
    	 
    	 /*
         //String age = "Unknown";
         if (age != null && !age.isEmpty()) {
             try {
                 int ageInt = Integer.parseInt(age);  // Convertir l'âge en entier
                 age = getAgeGroup(ageInt); // Déterminer l'intervalle d'âge
             } catch (NumberFormatException e) {
                 // Si l'âge n'est pas un nombre valide, on garde "Unknown"
            	 age = "Unknown";
             }
         }
         
         */
         
         /*
        
         // Créer la clé composite Classe-Âge
         String cle_composite = "Class" + classe + "_" + ageIntervalle;
         cleSegmente.set(cle_composite);
         
         // Valeur: survived status pour calculer les statistiques
         InfoPassenger.set(survie + "");
         
         
         */
         

         // Calculer la taille de la famille
         int grandeurFamille = sibSp + parch + 1; // +1 pour le passager lui-même
         
         // Déterminer le type de famille
         String categorieFamille = getCategoryFamille(grandeurFamille, sibSp, parch);
         
         // Déterminer le rôle dans la famille basé sur le nom et l'âge
         String roleFamille = getRoleFamille(nom, sexe, age, sibSp, parch);
         
         // Créer la clé composite
         String cle_composite = categorieFamille + "_" + roleFamille;
         typeFamille.set(cle_composite);
         
         // Données du passager: survived,pclass,sex,age,familySize
         String info = String.format("%d,%d,%s,%s,%d", 
                                    survie, classe, sexe, 
                                    (age == null || age.isEmpty()) ? "Unknown" : age,  //cherche l'explication
                                    		grandeurFamille);
         
         InfoPassenger.set(info);
         //ceci represente cle(typeFamille)-value(InfoPassenger) 
         context.write(typeFamille, InfoPassenger);

            }
        } catch (Exception e) {
            System.err.println("Erreur parsing ligne: " + line + " - " + e.getMessage());
        }
          
         
         //context.write(cleSegmente, InfoPassenger);

    	 
    	 
    	 
    	 /*
    	  * 
 		 outputkey.set(sexe);
    	 outputvalue.set(survie);
    	 
    	 outputkey.set(year);
    	 outputvalue.set(temperature);
    	 context.write(outputkey, outputvalue);
    	 */
    	 
    	 
    	 
        
      //}
    }
   
   
    /*
    private String getAgeGroup(String age) {
    	if (!age.isEmpty()) {
         try {
            	double ageDouble = Double.parseDouble(age);

        if (ageDouble < 13) {
            return "Enfant";
        } else if (ageDouble < 20) {
            return "Ado";
        } else if (ageDouble < 35) {
            return "Jeune";
        } else if (ageDouble < 55) {
            return "MoyenAge";
        } else if(ageDouble >= 55){
            return "Viellard";
        } else {
            return "Age manquant ou non connu";
        }
        }catch (NumberFormatException e) 
        {
        	
        }
    }
		return age;
  
  }
  
  
 */
    
    
    
    
    
    
      
    
    private String getCategoryFamille(int grandeurFamille, int sibSp, int parch) {
        if (grandeurFamille == 1) {
            return "Seul";
        } else if (grandeurFamille == 2) {
            return "Petit";
        } else if (grandeurFamille <= 4) {
            return "Moyen";
        } else {
            return "Grande";
        }
    }
    
    
    
    private String getRoleFamille(String nom, String sexe, String ageIntervalle, int sibSp, int parch) {
        // Extraire le titre du nom
    	
        if (nom == null) nom = "";
        if (sexe == null) sexe = "";
        if (ageIntervalle == null) ageIntervalle = "";
        
        String title = extraireTitre(nom);
        
   	 if (nom == null || nom.isEmpty()) {
	        return "Inconnu";  // Retourne une chaîne vide si le nom est null ou vide
	    }
   	 
   	 
        if (ageIntervalle != null && !ageIntervalle.isEmpty()) {
            try {
                double age = Double.parseDouble(ageIntervalle);
                
                // Enfant
                if (age < 18) {
                    return "Enfant";
                }
                
                // Adulte avec enfants
                if (parch > 0 && age >= 25) {
                    return "Parent";
                }
                
                // Marié sans enfants ou avec conjoint
                if (sibSp > 0) {
                    return "Epoux";
                }
                
                // Adulte seul
                return "Adulte";
                
            } catch (NumberFormatException e) {
                // Si l'âge n'est pas parsable, utiliser le titre
            }
        }
        
        // Classification basée sur le titre si l'âge n'est pas disponible
        if (title.contains("Master") || title.contains("Miss") && sibSp == 0) {
            return "Enfant";
        } else if (title.contains("Mrs") || title.contains("Mr") && sibSp > 0) {
            return "Epoux";
        } else {
            return "Adulte";
        }
    }
    
    /**
     * Extraire le titre du nom
     */
    private String extraireTitre(String nom) {
    	 if (nom == null || nom.isEmpty()) {
    	        return "";  // Retourne une chaîne vide si le nom est null ou vide
    	    }
    	
        String[] parts = nom.split(",");
        if (parts.length > 1) {
            String secondPart = parts[1].trim();
            if (secondPart.contains(".")) {
                return secondPart.substring(0, secondPart.indexOf(".")).trim();
            }
        }
        return "";
    }
    
    
}


 

 


 
	
	




