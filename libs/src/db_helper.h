#ifndef __DB_HELPER_H__
#define __DB_HELPER_H__

#include <vector>
#include <pqxx/pqxx>
#include <pthread.h>
#include "string.h"
#include <iostream>

/* enum describing the type of database */
enum db_type {
    DB_POSTGRESS = 0,
    DB_OTHER = 1
};

/* Factory desgin pattern implemented here */

/* An interface for a database connection */
class dbConnection {
    
public:
    virtual int initialize(const char*, const char*, const char*, const char*, const char*) = 0;
    virtual int db_insert(const char*) = 0;
    virtual int db_update(const char*) = 0;
    virtual int db_select(const char*, std::vector<std::string>*) = 0;
    virtual int db_delete(const char*) = 0;
};

/* Class describing a postGres db connection */
class dbPostgress : public dbConnection
{
    char *db_name;          // the name of the database 
    char *user_name;        // username to connect to db
    char *password;         // password to connect to db
    char *hostaddr;         // the host ip of the db
    char *port;             // the host port of db
    
    pthread_mutex_t _lock;  // a simple lock
    bool isInitialized;     // check if initiaze is called, need to call before other operations
    
public:
    dbPostgress();
    ~dbPostgress();
    
    /* Initialization of db 
     * @param1      the db name
     * @param2      the user name
     * @param3      the password
     * @param4      the host ip
     * @param5      the host port
     * return a value greater than zero on success
     * */
    int initialize(const char* , const char* , const char* , const char*, const char*);
    
    /* Insert operation currently not supported */
    int db_insert(const char* sql_query);
    
    /* Insert operation currently not supported */
    int db_update(const char* sql_query);
    
    /* Select operation 
     * @param       sql_query   the query to run
     * @param       rmq_list    A vector of strings with all the rmq ip/ports
     * */
    int db_select(const char* sql_query, std::vector<std::string> *rmq_list);
    
    /* Insert operation currently not supported */
    int db_delete(const char* sql_query);
};

/*db factory class used to create different db factory*/
class dbFactory
{
public:
    dbConnection* create_db_connection(db_type _type);
};

#endif