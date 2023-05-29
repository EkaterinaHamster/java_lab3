package kat.hamster;

import kat.hamster.dbWorker.DbWorker;

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("You must specify data file name and serialize file name!");
            return;
        }

        String sourceFile = args[0];

        DbWorker.populateFromFile(sourceFile);
        DbWorker.demoQuery();
        DbWorker.dirtyReadDemo();
    }
}