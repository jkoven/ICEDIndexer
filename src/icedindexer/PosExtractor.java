/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package icedindexer;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import java.util.HashMap;

/**
 *
 * @author jkoven
 */
public class PosExtractor {

    MaxentTagger tagger;

    public PosExtractor(String baseDir) {
        tagger = new MaxentTagger(baseDir + "/english-left3words-distsim.tagger");
    }

    public HashMap<String, String> ExtractPos(String text) {
        HashMap<String, String> taggedWords = new HashMap<>();
        taggedWords.put("noun", "");
        taggedWords.put("verb", "");
        String taggedText = tagger.tagString(text.replaceAll("_", " "));
        for (String wordPair : taggedText.split(" ")) {
            String[] tWord = wordPair.split("_");
            if (tWord.length == 2) {
                switch (tWord[1].substring(0, 1)) {
                    case "N":
                        taggedWords.put("noun", taggedWords.get("noun") + " " + tWord[0]);
                        break;
                    case "V":
                        taggedWords.put("verb", taggedWords.get("verb") + " " + tWord[0]);
                        break;
                    default:
                        break;
                }
            }
        }
        return taggedWords;
    }

}
