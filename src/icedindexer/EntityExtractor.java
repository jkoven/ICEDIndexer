/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package icedindexer;

/**
 *
 * @author jkoven
 */
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import java.util.HashMap;
import java.util.List;

public class EntityExtractor {

    private AbstractSequenceClassifier<CoreLabel> classifier;

    public EntityExtractor(String baseDir) {
        classifier
                = CRFClassifier.getClassifierNoExceptions(baseDir + "/datafiles/english.muc.7class.distsim.crf.ser.gz");
    }

    public HashMap<String, String> extract(String text) {
        HashMap<String, String> entityMap = new HashMap();
        List<List<CoreLabel>> out = classifier.classify(text);
        for (List<CoreLabel> sentence : out) {
            for (CoreLabel word : sentence) {
//                System.out.print(word.word() + '/' + word.get(CoreAnnotations.AnswerAnnotation.class) + ' ');
                if (!word.get(CoreAnnotations.AnswerAnnotation.class).equalsIgnoreCase("O")) {
                    if (entityMap.containsKey(word.get(CoreAnnotations.AnswerAnnotation.class))) {
                       entityMap.put(word.get(CoreAnnotations.AnswerAnnotation.class), 
                               entityMap.get(word.get(CoreAnnotations.AnswerAnnotation.class)) 
                                + " " + word.word());
                    } else {
                        entityMap.put(word.get(CoreAnnotations.AnswerAnnotation.class), word.word());
                    }
                }
            }
        }
//        for (String s : entityMap.keySet()){
//            System.out.println("here");
//            System.out.println(s + " " + entityMap.get(s));
//        }
        return entityMap;
    }
}
