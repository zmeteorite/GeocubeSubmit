package whu.edu.cn.core.vector.grid;

import com.google.common.base.Preconditions;

/**
 * This class specifies the details of how the XY coordinates are transformed
 */
public final strictfp class Transformer {

    public static double minX = 0; // minim X coordinate
    public static double maxX = 0; // maximum X coordinate
    public static double minY = 0; // minim Y coordinate
    public static double maxY = 0; // maximum Y coordinate
    public static double lengthX = 0; // length of X side
    public static double lengthY = 0; // length of Y side

    public static void init(double minX, double maxX, double minY, double maxY) {
        Preconditions.checkArgument(minX <= maxX);
        Preconditions.checkArgument(minY <= maxY);
        Transformer.minX = minX;
        Transformer.maxX = maxX;
        Transformer.minY = minY;
        Transformer.maxY = maxY;
        Transformer.lengthX = maxX - minX;
        Transformer.lengthY = maxY - minY;
    }

    public static double xToS(double x) {
        Preconditions.checkArgument(Transformer.minX <= x && x <= Transformer.maxX);
        return (x - Transformer.minX) / Transformer.lengthX;
    }

    public static double yToT(double y) {
        Preconditions.checkArgument(Transformer.minY <= y && y <= Transformer.maxY);
        return (y - Transformer.minY) / Transformer.lengthY;
    }

    public static double sToX(double s) {
        Preconditions.checkArgument(0 <= s && s <= 1);
        return Transformer.minX + s * Transformer.lengthX;
    }

    public static double tToY(double t) {
        Preconditions.checkArgument(0 <= t && t <= 1);
        return Transformer.minY + t * Transformer.lengthY;
    }

    public static double getWidth(int level) {
        return StrictMath.scalb(Transformer.lengthX, -level);
    }

    public static double getHeight(int level) {
        return StrictMath.scalb(Transformer.lengthY, -level);
    }

    public static int getLevel(double width, double height) {
        if (width <= 0 || height <= 0) {
            return CellId.MAX_LEVEL;
        }
        return (int) Math.min(
                StrictMath.floor(StrictMath.log(Transformer.lengthX / width) / StrictMath.log(2)),
                StrictMath.floor(StrictMath.log(Transformer.lengthY / height) / StrictMath.log(2)));
    }
}
