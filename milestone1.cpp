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
            ret += "???";  // in case there are exprssion types we don't know about here                                                                                                                    $
            break;
    }

}

string operatorExpressionToString(const Expr *expr) {
    if (expr == NULL)
        return "null";

    string ret;
    // Unary prefix operator: NOT
    if (expr->opType == Expr::NOT)
        ret += "NOT ";

    // Left-hand side of expression
    ret += expressionToString(expr->expr) + " ";

    // Operator itself
    switch (expr->opType) {
        case Expr::SIMPLE_OP:
            ret += expr->opChar;
            break;
        case Expr::AND:
            ret += "AND";
            break;
        case Expr::OR:
            ret += "OR";
            break;
        default:
            break; // e.g., for NOT
    }

    // Right-hand side of expression (only present for binary operators)
    if (expr->expr2 != NULL)
        ret += " " + expressionToString(expr->expr2);
    return ret;
}


/**
 * Convert the hyrise TableRef AST back into the equivalent SQL
 * @param table  table reference AST to unparse
 * @return       SQL equivalent to *table
 */
string tableRefInfoToString(const TableRef *table) {
    string ret;
    switch (table->type) {
        case kTableSelect:
            ret += "kTableSelect FIXME"; // FIXME
            break;
        case kTableName:
            ret += table->name;
            if (table->alias != NULL)
                ret += string(" AS ") + table->alias;
            break;
        case kTableJoin:
            ret += tableRefInfoToString(table->join->left);
            switch (table->join->type) {
                case kJoinCross:
                case kJoinInner:
                    ret += " JOIN ";
                    break;
                case kJoinOuter:
                case kJoinLeftOuter:
                case kJoinLeft:
                    ret += " LEFT JOIN ";
                    break;
                case kJoinRightOuter:
                case kJoinRight:
                    ret += " RIGHT JOIN ";
                    break;
                case kJoinNatural:
                    ret += " NATURAL JOIN ";
                    break;
            }
            ret += tableRefInfoToString(table->join->right);
            if (table->join->condition != NULL)
                ret += " ON " + expressionToString(table->join->condition);
            break;
        case kTableCrossProduct:
            bool doComma = false;
            for (TableRef *tbl : *table->list) {
                if (doComma)
                    ret += ", ";
                ret += tableRefInfoToString(tbl);
                doComma = true;
            }
            break;
    }
    return ret;
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


