package edu.uci.ics.pregelix.example.util;

public class Test {

    public static void main(String[] args) {
        long dpid = 0x0000000100000001L;
        int hashValue = (int) dpid ^ (Integer.reverse((int) (dpid >>> 32)) >>> 1);
        System.out.println(Integer.toHexString(hashValue));
    }
}
