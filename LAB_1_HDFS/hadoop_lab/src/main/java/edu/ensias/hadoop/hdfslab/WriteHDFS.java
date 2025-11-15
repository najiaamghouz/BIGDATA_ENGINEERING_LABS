package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    
    public static void main(String[] args) throws IOException {
        
        // Vérifier les arguments
        if (args.length < 2) {
            System.err.println("Usage: WriteHDFS <chemin_fichier> <texte_a_ecrire>");
            System.err.println("Exemple: hadoop jar WriteHDFS.jar /user/root/input/bonjour.txt 'Hello World'");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path filepath = new Path(args[0]);
        
        try {
            // Vérifier si le fichier existe déjà
            if (fs.exists(filepath)) {
                System.out.println("Le fichier existe déjà : " + args[0]);
                System.out.println("Suppression du fichier existant...");
                fs.delete(filepath, false);
            }
            
            // Créer le fichier et écrire dedans
            FSDataOutputStream outStream = fs.create(filepath);
            outStream.writeUTF("Bonjour tout le monde !");
            outStream.writeUTF(args[1]);
            outStream.close();
            
            System.out.println("✓ Fichier créé avec succès : " + args[0]);
            System.out.println("✓ Contenu écrit :");
            System.out.println("  - Bonjour tout le monde !");
            System.out.println("  - " + args[1]);
            
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture du fichier : " + e.getMessage());
            e.printStackTrace();
        } finally {
            fs.close();
        }
    }
}