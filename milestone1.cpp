//
// Created by Nathan Nishi on 4/9/21.
//
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
using namespace hsql;


string exSelect(const SelectStatement *stmt){
   return ("SELECT");

}

string exInsert(const InsertStatement * stmt){
    return ("INSERT");
}

string exCreate(const CreateStatement *stmt){
   return ("CREATE");
}
string execute(const SQLStatement *stmt) {
    switch (stmt->type()) {
        case kStmtSelect:
            return exSelect((const SelectStatement *) stmt);
        case kStmtInsert:
            return exInsert((const InsertStatement *) stmt);
        case kStmtCreate:
            return exCreate((const CreateStatement *) stmt);
        default:
            return "Not implemented";
    }
}

int main(int argc, char **argv) {

    char *envDir = argv[1];
    DbEnv env(0U);
    env.set_message_stream(&cout);
    env.set_error_stream(&cerr);
    while (true) {
        cout << "SQL> ";
        string stmt;
        getline(cin, stmt);
        if (stmt == "quit") break;
        SQLParserResult *parseTree = SQLParser::parseSQLString(stmt);
        for (uint i = 0; i < parseTree->size(); ++i) {
            cout << execute(parseTree->getStatement(i)) << endl;
        }
    }
    return EXIT_SUCCESS;
}


