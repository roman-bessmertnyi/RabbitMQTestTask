package util;

public class LargeStringGenerator {

    public static String generateLargeString(int megabytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < megabytes * 1024 * 1024; i++) {
            sb.append(" "); // Append spaces to generate large string
        }
        return sb.toString();
    }
}
