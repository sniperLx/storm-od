package cn.edu.cqu.od;

/**
 * Created by lab on 2016/6/1.
 *
 * @author lx
 */

/**
 * Use flag to dedicate this rfid's state.
 * In a OD, when a rfid is the 'O', we set
 * it state to be ORIGINATION. And when a rfid
 * is the 'D', we set it to DESTINATION, otherwise OTHER.
 */
public enum FLAG {
    ORIGINATION, DESTINATION, OTHER
}
