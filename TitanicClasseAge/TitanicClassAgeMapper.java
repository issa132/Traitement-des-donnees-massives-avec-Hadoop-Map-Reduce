package TitanicClasseAge;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//public class TitanicClassAgeMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
public class TitanicClassAgeMapper extends Mapper<LongWritable, Text, Text, Text>  {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private Text outputkey = new Text();
    //private IntWritable outputvalue = new IntWritable();
    private Text outputvalue = new Text();
    
    
    private Text cleSegmente = new Text();
    private Text InfoPassenger = new Text();
    
     
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
            
            if (fields.length >= 3) {  
        
      
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
    	     	 */
      
      /*
    	 int survie = parser.getSurvie();

    	 int classe = parser.getClasse();
 
    	 int age = parser.getAge();
    	 
    	 */
      
    	 
    	 //Text outputkey = new Text();
    	 
 
    
    	    
    	    

    	         int survie = Integer.parseInt(fields[1].trim());   // Colonne Survived
    	         int classe = Integer.parseInt(fields[2].trim());
    	         //int age = Integer.parseInt(fields[5].trim());
    	         String age =  fields[5].trim();
;		
    	         
    	         
    	    
            //String sexe = fields[4].replace("\"", "").trim(); // Colonne Sex

    	 outputkey.set(String.valueOf(classe));
    	 outputvalue.set(String.valueOf(age));
    	 
         //outputkey.set(classe);
         //outputvalue.set(age);
    	 
    	 
         String ageIntervalle;
         ageIntervalle = getAgeGroup(age);
         
         /*
         if (ageIntervalle == null ||ageIntervalle ) {
             ageGroup = "Unknown";
         } else {
             double age = Double.parseDouble(ageStr);
             ageIntervalle = getAgeGroup(age);
         }
         
         */
         
    	 
    	 /*
         String ageIntervalle;
         if (age.isEmpty()) {
             ageGroup = "Unknown";
         } else {
             double age = Double.parseDouble(ageStr);
             ageIntervalle = getAgeGroup(age);
         }
         
         */
         
        
         // Créer la clé composite Classe-Âge
         String cle_composite = "Class" + classe + "_" + ageIntervalle;
         cleSegmente.set(cle_composite);
         
         // Valeur: survived status pour calculer les statistiques
         InfoPassenger.set(survie + "");
         
         context.write(cleSegmente, InfoPassenger);

    	 
            }
        } catch (Exception e) {
            System.err.println("Erreur parsing ligne: " + line + " - " + e.getMessage());
        }
    	 
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
    
}


 

;


 
	
	




