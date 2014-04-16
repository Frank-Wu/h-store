/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

//
// Initializes the database, pushing the list of contestants and documenting domain data (Area codes and States).
//

package edu.brown.benchmark.biker.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

@ProcInfo (
singlePartition = false
)
public class Initialize extends VoltProcedure
{
    // Check if the database has already been initialized
    public final SQLStmt checkStmt = new SQLStmt("SELECT COUNT(*) FROM stations;");

    // Inserts a station
    public final SQLStmt insertAStationStmt = new SQLStmt("INSERT INTO stations VALUES (?,?);");

    // Inserts a dock
    public final SQLStmt insertADockStmt = new SQLStmt("INSERT INTO docks VALUES (?,?,?);");


    public static final String[] locations = new String[] {
        "Portland","Cambridge"};




    // Domain data: matching lists of stationids and their
    // locations
    public static final int[] stationids = new int[]{1,2};


    // Domain data: matching lists of dockids and their
    // associated stationids and bikeids of bikes in the
    // docks
    public static final int[] dockids = new int[]{100,101,200,201};

    public static final int[] dockstationids = new int[]{1,1,2,2};

    public static final int[] bikeids = new int[] {1001, 1002, 1003, 1004};









    public long run() { // int maxContestants, String contestants) {

        voltQueueSQL(checkStmt);
        long existingStationCount = voltExecuteSQL()[0].asScalarLong();

        // if the data is initialized, return the contestant count
        if (existingStationCount != 0)
            return existingStationCount;

        // initialize the data
        for (int i=0; i < stationids.length; i++) {
            voltQueueSQL(insertAStationStmt, stationids[i], locations[i]);
            voltExecuteSQL();
        }

        for (int i=0; i < dockids.length; i++) {
            voltQueueSQL(insertADockStmt, dockids[i], dockstationids[i], bikeids[i]);
            voltExecuteSQL();
        }

        voltQueueSQL(checkStmt);
        long endingStationCount = voltExecuteSQL()[0].asScalarLong();
        return endingStationCount;
    }
}
