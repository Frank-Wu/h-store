/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */
package edu.brown.benchmark.airline.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 0"
)    
public class UpdateReservation extends VoltProcedure {
    
    public final SQLStmt CheckSeat = new SQLStmt(
        "SELECT R_ID " +
        "  FROM " + AirlineConstants.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ? and R_SEAT = ?"
    );

    public final SQLStmt CheckCustomer = new SQLStmt(
        "SELECT R_ID " + 
        "  FROM " + AirlineConstants.TABLENAME_RESERVATION +
        " WHERE R_ID = ? and R_C_ID = ?");

    private static final String BASE_SQL = "UPDATE " + AirlineConstants.TABLENAME_RESERVATION +
                                           "   SET R_SEAT = ?, %s = ? " +
                                           " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?";
    
    public final SQLStmt ReserveSeat0 = new SQLStmt(String.format(BASE_SQL, "R_IATTR00"));
    public final SQLStmt ReserveSeat1 = new SQLStmt(String.format(BASE_SQL, "R_IATTR01"));
    public final SQLStmt ReserveSeat2 = new SQLStmt(String.format(BASE_SQL, "R_IATTR02"));
    public final SQLStmt ReserveSeat3 = new SQLStmt(String.format(BASE_SQL, "R_IATTR03"));

    public static final int NUM_UPDATES = 4;
    public final SQLStmt ReserveSeats[] = {
        ReserveSeat0,
        ReserveSeat1,
        ReserveSeat2,
        ReserveSeat3,
    };
    
    public VoltTable[] run(long r_id, long c_id, long f_id, long seatnum, long attr_idx, long attr_val) {
        assert(attr_idx >= 0);
        assert(attr_idx < ReserveSeats.length);
        
        // check if the seat is occupied
        // check if the customer has multiple seats on this flight
        voltQueueSQL(CheckSeat, f_id, seatnum);
        voltQueueSQL(CheckCustomer, f_id, c_id);
        final VoltTable[] results = voltExecuteSQL();
        
        assert(results.length == 2);
        if (results[0].getRowCount() > 0) {
            throw new VoltAbortException("Seat reservation conflict");
        }
        if (results[1].getRowCount() > 1) {
            throw new VoltAbortException("Customer owns multiple reservations");
        }
       
        // update the seat reservation for the customer
        voltQueueSQL(ReserveSeats[(int)attr_idx], seatnum, attr_val, r_id, c_id, f_id);
        VoltTable[] updates = voltExecuteSQL();
        assert updates.length == 1;
        return updates;
    } 
}
