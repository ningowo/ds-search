package team.dsys.dssearch.util;

import team.dsys.dssearch.rpc.Transaction;

import java.io.*;
import java.util.HashMap;

public class FileUtil {

    // trans - id type(put/delete) k v
    public static void addTransFromMap(String path, String name, Transaction trans) {
        // get transaction record
        HashMap<Integer, Transaction> transMap = readTransMap(path, name);

        // make change
        if (transMap == null) {
            transMap = new HashMap<>();
        }
        transMap.put(trans.transId, trans);

        // save change
        writeTransMap(path, name, transMap);
    }

    public static void removeTransFromMap(String path, String name, Transaction trans) {
        // get transaction record
        HashMap<Integer, Transaction> transMap = readTransMap(path, name);

        // make change
        if (transMap == null) {
            return;
        }
        transMap.remove(trans.transId);

        // save change
        writeTransMap(path, name, transMap);
    }
    @SuppressWarnings("unchecked")
    private static HashMap<Integer, Transaction> readTransMap(String path, String name) {
        File file = new File(path + name);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        HashMap<Integer, Transaction> transMap = null;

        try (
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis))
        {
            transMap = (HashMap<Integer, Transaction>) ois.readObject();
            return transMap;
        } catch (EOFException e) {
            // reach end of the file
            return transMap;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Overwrite the log file
     */
    @SuppressWarnings("unchecked")
    private static void writeTransMap(String path, String name, HashMap<Integer, Transaction> transMap) {
        File file = new File(path + name);
        try (FileOutputStream fos = new FileOutputStream(file);
             ObjectOutputStream oos = new ObjectOutputStream(fos))
        {
            oos.writeObject(transMap);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
