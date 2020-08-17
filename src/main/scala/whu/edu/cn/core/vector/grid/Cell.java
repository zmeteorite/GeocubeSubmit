package whu.edu.cn.core.vector.grid;

import whu.edu.cn.core.vector.util.MutableInteger;
import org.locationtech.jts.geom.*;

/**
 * An Cell is an that represents a cell. Unlike CellId,
 * it supports efficient containment and intersection tests.
 * However, it is also a more expensive representation.
 */
public final strictfp class Cell {

    byte level;
    byte orientation;
    CellId cellId;
    double minX;
    double maxX;
    double minY;
    double maxY;

    /**
     * Default constructor used only internally.
     */
    Cell() {
    }

    /**
     * An S2Cell always corresponds to a particular S2CellId. The other
     * constructors are just convenience methods.
     */
    public Cell(CellId id) {
        init(id);
    }

    // This is a static method in order to provide named parameters.
    public static Cell fromPosLevel(byte pos, int level) {
        return new Cell(CellId.fromPosLevel(pos, level));
    }

    // Convenience methods.
    public Cell(Point p) {
        init(CellId.fromPoint(p));
    }

    public CellId id() {
        return cellId;
    }

    public byte level() {
        return level;
    }


    public byte orientation() {
        return orientation;
    }

    public double minX() {
        return minX;
    }

    public double maxX() {
        return maxX;
    }

    public double minY() {
        return minY;
    }

    public double maxY() {
        return maxY;
    }

    public boolean isLeaf() {
        return level == CellId.MAX_LEVEL;
    }

    public boolean mayIntersect(Cell cell) {
        return cellId.intersects(cell.cellId);
    }

    public boolean mayIntersect(Geometry region) {
        Polygon polygon = new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(minX, minY),
                new Coordinate(minX, maxY),
                new Coordinate(maxX, maxY),
                new Coordinate(maxX, minY),
                new Coordinate(minX, minY)
        });
        return polygon.intersects(region);
    }

    public boolean contains(Cell cell) {
        return cellId.contains(cell.cellId);
    }

    public boolean within(Geometry region) {
        Polygon polygon = new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(minX, minY),
                new Coordinate(minX, maxY),
                new Coordinate(maxX, maxY),
                new Coordinate(maxX, minY),
                new Coordinate(minX, minY)
        });
        return polygon.within(region);
    }

    /**
     * Return the inward-facing normal of the great circle passing through the
     * edge from vertex k to vertex k+1 (mod 4). The normals returned by
     * GetEdgeRaw are not necessarily unit length.
     * <p>
     * If this is not a leaf cell, set children[0..3] to the four children of
     * this cell (in traversal order) and return true. Otherwise returns false.
     * This method is equivalent to the following:
     * <p>
     * for (pos=0, id=child_begin(); id != child_end(); id = id.next(), ++pos)
     * children[i] = S2Cell(id);
     * <p>
     * except that it is more than two times faster.
     */
    public boolean subdivide(Cell children[]) {
        // This function is equivalent to just iterating over the child cell ids
        // and calling the S2Cell constructor, but it is about 2.5 times faster.

        if (cellId.isLeaf()) {
            return false;
        }

        double childX = (maxX - minX) / 2;
        double childY = (maxY - minY) / 2;
        double centX = (maxX + minX) / 2;
        double centY = (maxY + minY) / 2;

        // Create four children with the appropriate bounds.
        CellId id = cellId.childBegin();
        for (int pos = 0; pos < 4; ++pos, id = id.next()) {
            Cell child = children[pos];
            child.orientation = (byte) (orientation ^ CellId.posToOrientation(pos));
            child.level = (byte) (level + 1);
            child.cellId = id;
            int ij = CellId.posToIJ(orientation, pos);
            double offsetX = 0.0;
            double offsetY = 0.0;
            if (ij == 1 || ij == 3) {
                offsetY = childY;
            }
            if (ij == 2 || ij == 3) {
                offsetX = childX;
            }
            child.minX = minX + offsetX;
            child.maxX = centX + offsetX;
            child.minY = minY + offsetY;
            child.maxY = centY + offsetY;
        }
        return true;
    }

    private void init(CellId id) {
        cellId = id;
        MutableInteger mI = new MutableInteger(0);
        MutableInteger mJ = new MutableInteger(0);
        MutableInteger mOrientation = new MutableInteger(0);

        id.toIJOrientation(mI, mJ, mOrientation);
        orientation = (byte) mOrientation.intValue(); // Compress int to a byte.
        level = (byte) id.level();
        int cellSize = 1 << (CellId.MAX_LEVEL - level);
        // Compute the cell bounds in scaled (i,j) coordinates.
        int iLow = mI.intValue() & -cellSize;
        int iHigh = iLow + cellSize - 1;
        int jLow = mJ.intValue() & -cellSize;
        int jHigh = jLow + cellSize - 1;
        // Compute the cell bounds in (x, y) coordinates
        minX = Transformer.sToX(CellId.ijToST(iLow));
        maxX = Transformer.sToX(CellId.ijToST(iHigh));
        minY = Transformer.tToY(CellId.ijToST(jLow));
        maxY = Transformer.tToY(CellId.ijToST(jHigh));
    }

    public Polygon toPolygon() {
        return new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(this.minX(), this.minY()),
                new Coordinate(this.minX(), this.maxY()),
                new Coordinate(this.maxX(), this.maxY()),
                new Coordinate(this.maxX(), this.minY()),
                new Coordinate(this.minX(), this.minY())
        });
    }

    @Override
    public String toString() {
        return "Cell{" +
                "level=" + level +
                ", orientation=" + orientation +
                ", cellId=" + cellId +
                ", minX=" + minX +
                ", maxX=" + maxX +
                ", minY=" + minY +
                ", maxY=" + maxY +
                '}';
    }
}


