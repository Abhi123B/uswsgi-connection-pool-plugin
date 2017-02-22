#include "db_helper.h"
#include "parser_defs.h"
#include "uws_pg_config.h"

using namespace std;
using namespace pqxx;

dbPostgress::dbPostgress()
{
    db_name = NULL;
    user_name = NULL;
    password = NULL;
    hostaddr = NULL;
    port = NULL;
    pthread_mutex_init(&_lock, NULL);
    isInitialized = false;
}
    
dbPostgress::~dbPostgress()
{
    if(db_name != NULL)
        delete[] db_name;
    if(user_name != NULL)
        delete[] user_name;
    if(password != NULL)
        delete[] password;
    if(hostaddr != NULL)
        delete[] hostaddr;
    if(port != NULL)
        delete[] port;
    pthread_mutex_destroy(&_lock);
}
    
int dbPostgress::initialize(const char* n, const char* u, const char* p, const char* h, const char* o)
{
    int len = 0, ret = 1;
    
    pthread_mutex_lock(&_lock);
    
    if(isInitialized)
    {
        pthread_mutex_unlock(&_lock);
        return ret;
    }
    
    len = strlen(n);
    db_name = new char[len+1];
    if(!db_name)
        ret = -1;
    memcpy (db_name , n, len +1);
    
    len = strlen(u);
    user_name = new char[len+1];
    if(!user_name)
        ret = -1;
    memcpy (user_name , u, len +1);
    
    len = strlen(p);
    password = new char[len+1];
    if(!password)
        ret = -1;
    memcpy (password , p, len +1);
    
    len = strlen(h);
    hostaddr = new char[len+1];
    if(!hostaddr)
        ret = -1;
    memcpy (hostaddr , h, len +1);
    
    len = strlen(o);
    port = new char[len+1];
    if(!port)
        ret = -1;
    memcpy (port , o, len +1);
    
    isInitialized = true;
    pthread_mutex_unlock(&_lock);
    
    return ret;
}
    
int dbPostgress::db_insert(const char* sql_query)
{
    return -1;
}
    
int dbPostgress::db_update(const char* sql_query)
{
    return -1;
}
    
int dbPostgress::db_select(const char* sql_query, std::vector<std::string> *rmq_list )
{
    char connectStr[UWS_LARGE_BUFFER_SIZE];
    char rmqcstr[UWS_LARGE_BUFFER_SIZE];
    string _id, ips, ch;
    int pos = 0;
    char buf[UWS_SMALL_BUFFER_SIZE];
    
    if(!isInitialized || !sql_query)
        return -1;
        
    sprintf(connectStr, "dbname=%s user=%s password=%s hostaddr=%s port=%s",db_name,user_name,password,hostaddr,port);
    
    connection con(connectStr);
    
    if (con.is_open()) {
        cout << "Opened database successfully: " << con.dbname() << endl;
    } else {
        cerr << "Can't open database" << endl;
        return -1;
    }
    
    nontransaction ntran(con);
    
    result res( ntran.exec( sql_query));

    for (result::const_iterator c = res.begin(); c != res.end(); ++c) {
            pos = 0;
            _id = c[1].as<string>();
            ch = c[3].as<string>();
            ips = c[0].as<string>();
            if(ch.compare("t") != 0)
                continue;
            while(parse_rmq_ip_string_db(ips.c_str(), ips.length(), ',', pos, buf) > 0)
            {
                sprintf(rmqcstr, "%s%d,knowlus:knowlus@%s/knowlus", _id.c_str(), pos, buf);
                rmq_list->push_back(rmqcstr);
                pos++;
            }
      }
    
    con.disconnect();
    
    return 1;
}
    
int dbPostgress::db_delete(const char* sql_query)
{
    return -1;
}

dbConnection* dbFactory::create_db_connection(db_type _type)
{
    if(_type == DB_POSTGRESS)
        return new dbPostgress();
    else
        return NULL;
}