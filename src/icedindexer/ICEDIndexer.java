/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package icedindexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import javax.swing.JFileChooser;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 *
 * @author jkoven
 */
public class ICEDIndexer {

    public static String bmessagePath;
    public static String bindexPath;
    public static String bhashIndexPath;
    public static String blinkIndexPath;
    public static String bworkingIndexPath;
    public static String battachmentPath;
    public static File baseDir = null;
//    public static String bmessagePath = "/Users/jkoven/NetBeansProjects/IVEST/mymail/messages";
//    public static String bindexPath = "/Users/jkoven/NetBeansProjects/IVEST/mymail/index";
//    public static String bhashIndexPath = "/Users/jkoven/NetBeansProjects/IVEST/mymail/hashindex";
//    public static String bworkingIndexPath = "/Users/jkoven/NetBeansProjects/IVEST/mymail/workingindex";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        try {
            System.setOut(new PrintStream(new File("output-file.txt")));
            System.setErr(new PrintStream(new File("error-file.txt")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        File sourceFile = new File(ICEDIndexer.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        try {
            BufferedReader credReader = new BufferedReader(new FileReader(new File(sourceFile.getParent(),"datafiles/basedirectory.txt")));
            baseDir = new File(passwordDecode(credReader.readLine().trim()));
            credReader.close();
        } catch (IOException ex) {

        }
        JFileChooser fc;
        if (baseDir == null) {
            fc = new JFileChooser();
        } else {
            fc = new JFileChooser(baseDir);
        }
        fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int resultVal = fc.showOpenDialog(null);
        if (resultVal != JFileChooser.APPROVE_OPTION) {
            System.exit(0);
        }
        baseDir = fc.getSelectedFile();
        try {
            FileWriter credOut = new FileWriter(new File(sourceFile.getParent(),"datafiles/basedirectory.txt"));
            credOut.write(passwordEncode(baseDir.getParent()) + "\n");
            credOut.close();
        } catch (IOException e) {
            System.err.println("Cant write to the base directory file");
        }
        bindexPath = new File(baseDir, "index").toString();
        bhashIndexPath = new File(baseDir, "hashindex").toString();
        blinkIndexPath = new File(baseDir, "linkindex").toString();
        bmessagePath = new File(baseDir, "messages").toString();
        bworkingIndexPath = new File(baseDir, "workingindex").toString();
        battachmentPath = new File(baseDir, "attachments").toString();
        String[] indexArgs = {"-index",
            bindexPath,
            "-docs", bmessagePath,
            "-hashindex", bhashIndexPath,
            "-linkindex", blinkIndexPath,
            "-attachments", battachmentPath,
            "-basedirectory", baseDir.toString(),
            "-workingindex", bworkingIndexPath};
        IndexFiles.indexMain(indexArgs, true);
        IndexFiles.indexMain(indexArgs, false);

//        IndexSearch.search(bindexPath);
        System.exit(0);
    }

    private static String passwordEncode(String password) {
        try {
            // only the first 8 Bytes of the constructor argument are used
// as material for generating the keySpec
            DESKeySpec keySpec = new DESKeySpec("NotReallySecure".getBytes("UTF8"));
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            SecretKey key = keyFactory.generateSecret(keySpec);
            sun.misc.BASE64Encoder base64encoder = new BASE64Encoder();
            sun.misc.BASE64Decoder base64decoder = new BASE64Decoder();
// ENCODE plainTextPassword String
            byte[] cleartext = password.getBytes("UTF8");

            Cipher cipher = Cipher.getInstance("DES"); // cipher is not thread safe
            cipher.init(Cipher.ENCRYPT_MODE, key);
            String encrypedPwd = base64encoder.encode(cipher.doFinal(cleartext));
            // now you can store it
            return (encrypedPwd);

        } catch (InvalidKeyException | UnsupportedEncodingException |
                NoSuchAlgorithmException | InvalidKeySpecException |
                NoSuchPaddingException | IllegalBlockSizeException |
                BadPaddingException ex) {
            ex.printStackTrace();
        }
        return (null);
    }

    private static String passwordDecode(String password) {
        try {
            // only the first 8 Bytes of the constructor argument are used
// as material for generating the keySpec
            DESKeySpec keySpec = new DESKeySpec("NotReallySecure".getBytes("UTF8"));
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
            SecretKey key = keyFactory.generateSecret(keySpec);
            sun.misc.BASE64Encoder base64encoder = new BASE64Encoder();
            sun.misc.BASE64Decoder base64decoder = new BASE64Decoder();
// DECODE encryptedPwd String
            byte[] encrypedPwdBytes = base64decoder.decodeBuffer(password);

            Cipher cipher = Cipher.getInstance("DES");// cipher is not thread safe
            cipher.init(Cipher.DECRYPT_MODE, key);
            byte[] plainTextPwdBytes = (cipher.doFinal(encrypedPwdBytes));
            return (new String(plainTextPwdBytes));
        } catch (InvalidKeyException | UnsupportedEncodingException |
                NoSuchAlgorithmException | InvalidKeySpecException |
                NoSuchPaddingException | IllegalBlockSizeException |
                BadPaddingException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return (null);
    }

}
