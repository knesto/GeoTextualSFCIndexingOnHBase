package gr.unipi.geotextualindex.sfc;

import com.github.davidmoten.guavamini.Preconditions;

public class GeoUtil {

    public static long[] scale3DPoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat, int text, int mintext, int maxtext,
                                    long max) {
        long x = scale(( lon - minLon) / (maxLon - minLon), max);
        long y = scale(( lat - minLat) / (maxLat - minLat), max);
        long z = scale(((long) text - mintext) / (maxtext - mintext), max);
        return new long[] { x, y, z };
    }

    public static long[] scale2DPoint(double lon, double minLon, double maxLon, double lat, double minLat, double maxLat, long maxOrdinates) {
        long x = scale(( lon - minLon) / (maxLon - minLon), maxOrdinates);
        long y = scale(( lat - minLat) / (maxLat - minLat), maxOrdinates);
        //System.out.println(x);
        //System.out.println(y);
        return new long[] { x, y};
    }

    private static long scale(double d, long max) {

        Preconditions.checkArgument(((Double.compare(d,0) > 0) || (Double.compare(d,0) == 0)) && ((Double.compare(d,1) < 0) || (Double.compare(d,1) == 0)));
        if (d == 1) {
            return max;
        } else {
            return Math.round(Math.floor(d * (max + 1)));
        }
    }

}
