/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * 'Software'), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import spock.lang.*
import groovy.json.*

class TestSqlCmd extends Specification {

    @Shared def sqlFile = new File( '../shared/sql.txt' )
    @Shared def slurper = new JsonSlurper()
    @Shared def response //stores expected response so that it may be accessed by all helper methods
    @Shared def sqlList = []

    def setupSpec() {
        //Move file contents to memory
        def primeList = { file, list ->
            if ( file.size() != 0 ) {
                file.eachLine { line ->
                    if ( ! line.trim().startsWith( '#' ) ) {
                        list.add( line )
                    }
                }
            }
        }
        primeList( sqlFile, sqlList )
    }

    //TEST SQL Queries
    @Unroll
    //performs this method for each item in testName
    def '#testName'() {
        try {
            setup: 'open new query'
            response = resTmp
            def query = "echo $input".execute()
            def sqlProc = "../../../bin/sqlcmd --output-format=csv".execute()

            and: 'execute SQL query'
            def sout = new ByteArrayOutputStream()
            def serr = new ByteArrayOutputStream()

            when: 'query is executed'
            query.pipeTo( sqlProc ) //pipe query stdout to sqlproc stdin
            query.stdin.flush()
            sqlProc.stdin.flush()

            then: 'gather output from SQLCommand'
            sqlProc.waitForProcessOutput( sout, serr )

            expect: 'the output to match the expected results'
            checkResponse( input, sout.toString(), serr.toString() )

        }
        finally {
            //clear tables--this is inelegant and bad
            def delProc = '../../../bin/sqlcmd'.execute()
            ( 'echo delete from partitioned_table'.execute() ).pipeTo( delProc )
            delProc.waitForProcessOutput()
        }

        where: "list of inputs to test and expected responses"
        line << sqlList
        iter = slurper.parseText( line )
        testName = iter.testName
        input = iter.sqlCmd
        resTmp = iter.response
    }

    def checkResponse(def input, def output, def error) {
        if ( error.size() == 0 ) {
            /*output is of form:
            inputEcho
            results
            \n
            \n
            modified tuples
             */
            def cleanedOutput = []
            //****Removing ' \r' is temporary work around for ENG-5009****
            output = output.replaceAll( ' \r', '' )
            output.eachLine {
                line ->
                    if ( line.size() != 0 ) {
                        cleanedOutput.add( line.trim() )
                    }
            }
            assert cleanedOutput.head() == input.trim() //first item should be sql query
            //remove 'metadata' from tables. we still need to use the header metadata so don't run sqlcmd with --ouput-skip-metadata
            def modified = cleanedOutput.find { line ->
                line.contains( 'rows(s) modified' )
            }
            cleanedOutput.minus( modified ) //cleanedOutput is now only query and result CSV
            if ( cleanedOutput.tail().size() > 0 ) {
                makeAndCheckTable( cleanedOutput.tail() ) //should only get here on SELECTs
            }
            else {
                response.result.modified_tuples == ( modified.last() - '(' ).minus( ' row(s) modified)' )
            }
        }
        else {
            //make sure an error was expected
            assert error.trim() != 'Connection refused' //: "You must first connect to a server to issue SQL queries"
            response.result == 'ERROR'
        }
    }

    def makeAndCheckTable(def resultTable) {
        def headers = resultTable[0].tokenize( ',' ).collect { header ->
            header.toLowerCase()
        }
        def rows = resultTable.tail().collect { row ->
            row.tokenize( ',' )
        }
        //map representation of results. headers as keys.
        def table = ( 0..headers.size() - 1 ).collectEntries { index ->
            [headers[index], rows.collect { row -> row[index] }]
        }
        def checkColumn = { column ->
            table[column] == response.result."$column"
        }
        headers.every( checkColumn ) //only returns true if all elements meet condition
    }
}
