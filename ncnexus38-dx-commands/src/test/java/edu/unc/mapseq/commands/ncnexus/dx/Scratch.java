package edu.unc.mapseq.commands.ncnexus.dx;

import org.junit.Test;

public class Scratch {

    @Test
    public void test() {

        for (int dx = 1; dx < 37; ++dx) {
            for (int version = 1; version < 29; ++version) {
                System.out.printf("dx: %d, version: %d%n", dx, version);
            }
        }

    }

}
