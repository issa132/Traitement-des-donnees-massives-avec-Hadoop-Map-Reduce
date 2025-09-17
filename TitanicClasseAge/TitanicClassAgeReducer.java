package TitanicClasseAge;

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






public class TitanicClassAgeReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text resultat = new Text();
	
	//La classe étend la classe Reducer de Hadoop. Cela signifie qu'elle doit implémenter la méthode 
	//reduce qui est utilisée pour traiter les paires clé/valeur issues de la phase de mappage.

	@Override
	//public void reduce(Text key, Iterable<IntWritable> values,
	public void reduce(Text key, Iterable<Text> values,
	    Context context)
	    throws IOException, InterruptedException {
		
		  try {
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

	  int survieHomme = 0; 
	  int survieFemme = 0; 
	  int nombreTotalVoyageur = 0;
	  int survivant = 0; 
	  
	  /*
	  for (IntWritable value : values) {
	    maxValue = Math.max(maxValue, value.get());
	  }
	  */
	  
	  
	  
	  //for (IntWritable value : values) {
	  for (Text value : values) {
		  //for (Text key : keys) {
		  
		  nombreTotalVoyageur ++;
		  
          int survecu = Integer.parseInt(value.toString());
          if (survecu == 1) {
        	  survivant++;
          }
		  
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
	  
	  
      // Calculer le taux de survie
      double tauxDeSurvie = nombreTotalVoyageur > 0 ? 
          (double) survivant / nombreTotalVoyageur * 100 : 0;
      
      // Formater le résultat
      String resultString = String.format("Passagers: %d, Survivant: %d, Taux de Survie: %.1f%%", 
    		  nombreTotalVoyageur, survivant, tauxDeSurvie);
      
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
	  
		  } catch (Exception e) {
			   System.err.println("Erreur dans le mapper: " +e.getMessage());
			   e.printStackTrace();
			  
		       } 
	  


	  
	}
	

}

 

 


 
 


