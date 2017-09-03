package cn.edu.cqu.od;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by lab on 2016/4/26.
 *
 * @author lx
 */

/**
 * It implements interface serializable is necessary,
 * as it will be transfer in network.
 * This class is used to describe a rfid line.
 */
public class PathInfoNode implements Serializable {
    private static final long serialVersionUID = -2712478562829444864L;

    //    private String eid;
    private String ip;
    private Date passTime;
    private String car_type;
    private FLAG flag = FLAG.OTHER;

    public PathInfoNode(String ip, Date time, FLAG flag, String car_type) {
        this.ip = ip;
        this.passTime = time;
        this.flag = flag;
        this.car_type = car_type;
    }

    public PathInfoNode(String ip, Date time, String car_type) {
        this(ip, time, FLAG.OTHER, car_type);
    }

    public PathInfoNode() {
    }

    public void setFlag(FLAG flag) {
        this.flag = flag;
    }

    public FLAG getFlag() {
        return this.flag;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return this.ip;
    }

    public void setPassTime(Date passTime) {
        this.passTime = passTime;
    }

    public Date getPassTime() {
        return this.passTime;
    }

    public void setCar_type(String car_type) {
        this.car_type = car_type;
    }

    public String getCar_type() {
        return car_type;
    }

    public String toString() {
        return "RFID_Data[ip=" + ip
                + ", passTime=" + passTime
                + ", flag=" + flag
                + ", car_type" + car_type
                + "]";
    }
}