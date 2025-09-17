package TitanicAnalyseFamilialle;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;



import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;






public class TitanicAnalyseFamilleReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text resultat = new Text();
	
	//La classe étend la classe Reducer de Hadoop. Cela signifie qu'elle doit implémenter la méthode 
	//reduce qui est utilisée pour traiter les paires clé/valeur issues de la phase de mappage.

	@Override
	//public void reduce(Text key, Iterable<IntWritable> values,
	public void reduce(Text key, Iterable<Text> values,
	    Context context)
	    throws IOException, InterruptedException {
	// Cette ligne initialise maxValue avec la plus petite valeur possible d'un entier (Integer.MIN_VALUE). 
	//Cela sert de base pour comparer les températures et trouver la température maximale.   
	  //int maxValue = Integer.MIN_VALUE;
	  // essayer aussi avec IntWritable , essayer de remplacer avec text aussi 
	  // IntWritable statistique = new statistique();
	  int statistique ;
	  
	//Une boucle qui parcourt toutes les températures (IntWritable) associées à l'année donnée
	//À chaque itération, on compare la température actuelle (value.get()) à la température maximale 
	//enregistrée jusqu'à présent (maxValue). La méthode Math.max() renvoie la plus grande des deux valeurs,
	//et on met à jour maxValue avec cette température maximale.

	  int nombreHomme = 0; 
	  int nombreFemme = 0; 
	  int nombreTotalVoyageur = 0;
	  int survivant = 0; 
      int nombreClass1  = 0;
      int nombreClass2 = 0;
      int nombreClass3 = 0;
      double totalAge = 0;
      int nombreAge = 0;
      int totalGrandeurFamille = 0;
	  
	  /*
	  for (IntWritable value : values) {
	    maxValue = Math.max(maxValue, value.get());
	  }
	  */
	  
	  
	  
	  //for (IntWritable value : values) {
	  for (Text value : values) {
		  //for (Text key : keys) {
		  
		   
          String[] data = value.toString().split(",");
          if (data.length >= 5) {
        	  nombreTotalVoyageur++;
        	  
        	  
              
              // Survie
              if (data[0].equals("1")) {
            	  survivant++;
              }
              
              // Classe
              int classePassager = Integer.parseInt(data[1]);
              if (classePassager == 1) nombreClass1++;
              else if (classePassager == 2) nombreClass2++;
              else if (classePassager == 3) nombreClass3++;
              
              // Sexe
              if (data[2].equals("male")) {
            	  nombreHomme++;
              } else {
            	  nombreFemme++;
              }
              
              // Âge
              if (!data[3].equals("Unknown")) {
                  try {
                      totalAge += Double.parseDouble(data[3]);
                      nombreAge++;
                  } catch (NumberFormatException e) {
                      // Ignorer
                  }
              }
              
              /*
             //int grandeurFamilleReducer = Integer.parseInt(data[4]);
              // Taille de famille
              totalGrandeurFamille += Integer.parseInt(data[4]);
              
              String grandeurFamilleReducer = (data[4]);
              if (isNumeric(grandeurFamilleReducer)) {
                  try {
                 	 int tailleFamille = Integer.parseInt(grandeurFamilleReducer);
                
               // Taille de famille
               //totalGrandeurFamille += Integer.parseInt(data[4]);
                 	 
                 	 totalGrandeurFamille += tailleFamille;
                 	  } 
                  catch (NumberFormatException e) {
                 	 
                  }
              
          }
          */
              
              
              
              
              String tailleStr = data[4].trim();
              if (isNumeric(tailleStr)) {
                  try {
                      int tailleFamille = Integer.parseInt(tailleStr);
                      if (tailleFamille > 0 && tailleFamille <= 20) { // Validation raisonnable
                          totalGrandeurFamille += tailleFamille;
                      }
                  } catch (NumberFormatException e) {
                      System.err.println("Taille famille non valide ignorée: " + tailleStr);
                  }
              } else {
                  System.err.println("Taille famille non numérique ignorée: " + tailleStr);
              }

      
	  
               
          /*    
		  nombreTotalVoyageur ++;
		  
          int survecu = Integer.parseInt(value.toString());
          if (survecu == 1) {
        	  survivant++;
          }
          
          */
        	  
        	  
		  
		  /*
     	  if(value.get() == 1){
			  
			  survivant ++;
		  }
		  if(value.get() == 1 && key.get() == male){
			  survieFemme = +1;
		  }else
		  {
			  survieHomme = +1;
		  } 
		  */
	  }
	  
	  /*
      // Calculer le taux de survie
      double tauxDeSurvie = nombreTotalVoyageur > 0 ? 
          (double) survivant / nombreTotalVoyageur * 100 : 0;
      */
          
          /*
      // Formater le résultat
      String resultString = String.format("Passagers: %d, Survivant: %d, Taux de Survie: %.1f%%", 
    		  nombreTotalVoyageur, survivant, tauxDeSurvie);
      
      resultat.set(resultString);
      context.write(key, resultat);
      */
      
	  }
      
      
      // Calculer les statistiques
      double tauxSurvie = nombreTotalVoyageur > 0 ? (double) survivant / nombreTotalVoyageur * 100 : 0;
      double moyenneAge = nombreAge > 0 ? totalAge / nombreAge : 0;
      double moyenneTailleFamille = nombreTotalVoyageur > 0 ? (double) totalGrandeurFamille / nombreTotalVoyageur : 0;
      
      // Formater le résultat
      String resultString = String.format(
          "Count: %d | Survived: %d (%.1f%%) | M/F: %d/%d | Class1/2/3: %d/%d/%d | AvgAge: %.1f | AvgFamSize: %.1f",
          nombreTotalVoyageur, survivant, tauxSurvie, 
          nombreHomme, nombreFemme, 
          nombreClass1, nombreClass2, nombreClass3,
          moyenneAge, moyenneTailleFamille
      );
      
      resultat.set(resultString);
      context.write(key, resultat);      
      
 
  
	  
	  /*
	  // taux de survie	  
	  double tauxSurvie = nombreTotalVoyageur > 0 ? (double) survivant / nombreTotalVoyageur * 100 : 0;
	  
	  // resultat
	  String resulatPrint = String.format("Total: %d, Survivant: %d, Pourcentage: %.2f%%", nombreTotalVoyageur, survivant,tauxSurvie );
	  resultat.set(resulatPrint);
	  
	  //en effet key represente soit femme ou homme
	  context.write(key,resultat);
	  
	  */
	  
	  
	  /*
	  context.write(key, new IntWritable(survieFemme));
	  #context.write(key, new IntWritable(survieHomme));
	  
	  #context.write(key, new IntWritable(maxValue));
	  */

	  
	
	

}

	// Méthode utilitaire pour vérifier si une chaîne est numérique
    private boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}

 

 


 
 


