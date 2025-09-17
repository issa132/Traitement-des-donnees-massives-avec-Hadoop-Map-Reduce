package TitanicSurvie;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SurvieParSexeMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
    private Text outputkey = new Text();
    private IntWritable outputvalue = new IntWritable();
    
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
            
            if (fields.length >= 5) {
                String sexe = fields[4].replace("\"", "").trim(); // Colonne Sex
                int survie = Integer.parseInt(fields[1].trim());   // Colonne Survived
                
                outputkey.set(sexe);
                outputvalue.set(survie);
                
                context.write(outputkey, outputvalue);
            }
        } catch (Exception e) {
            System.err.println("Erreur parsing ligne: " + line + " - " + e.getMessage());
        }
    }
}