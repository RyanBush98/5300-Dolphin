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

string operatorExpressionToString(const Expr *expr);

string columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch (col->type) {
    	case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

string expressionToString(const Expr *expr){
    	string ret;
    	switch (expr->type) {
        case kExprStar:
            ret += "*";
            break;
        case kExprColumnRef:
            if (expr->table != NULL)
                ret += string(expr->table) + ".";
        case kExprLiteralString:
            ret += expr->name;
            break;
        case kExprLiteralFloat:
            ret += to_string(expr->fval);
            break;
        case kExprLiteralInt:
            ret += to_string(expr->ival);
            break;
        case kExprFunctionRef:
            ret += string(expr->name) + "?" + expr->expr->name;
            break;
        default:
            ret += "???";  // in case there are exprssion types we don't know about here
            break;
    }

}
string exSelect(const SelectStatement *stmt){
   //select expressiosn stored in selectList
  string buildSelect = "SELECT ";
  bool seenComma = false;
  for (Expr *expr : *stmt->selectList){  
     if (seenComma) {
      buildSelect += expressionToString(expr);
      seenComma = true; 
     }
  }
    buildSelect += " FROM " + tableRefInfoToString(stmt->fromTable); 
    
   return (buildSelect);

}

string exInsert(const InsertStatement * stmt){
    return ("INSERT");
}

string exCreate(const CreateStatement *stmt){
  string buildCreate = "CREATE TABLE ";
  buildCreate += string(stmt->tableName) + " (";
  bool commaSeen = false;
  for (ColumnDefinition * col : *stmt->columns) {
    if (commaSeen) buildCreate += ", ";
    buildCreate += columnDefinitionToString(col);
    commaSeen = true;
  }
  buildCreate += ")";
  return (buildCreate);
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


