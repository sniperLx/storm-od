package cn.edu.cqu.od;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by lab on 2016/6/1.
 *
 * @author lx
 */

/**
 * This class store a car's OD
 */
public class ODListNode implements Serializable {
    private static final long serialVersionUID = 8500099510282703756L;

    private String eid;
    private HashMap<PathInfoNode, PathInfoNode> odHashmap;

    public ODListNode(String eid, HashMap<PathInfoNode, PathInfoNode> odHashmap) {
        this.eid = eid;
        this.odHashmap = odHashmap;
    }

    public String toString() {
        return "ODListNode["
                + "eid=" + eid
                + ", odHashmap=" + odHashmap
                + "]";
    }

    public String getEid() {
        return eid;
    }

    public HashMap<PathInfoNode, PathInfoNode> getOdHashmap() {
        return odHashmap;
    }
}
