#include "uwsgi.h"
#include <stdlib.h>
#include <stdio.h>
#include <map>
#include "amqp_conn_pool.h"
#include <string.h>
#include "uws_pg_config.h"
#include "parser_defs.h"
#include "zmqServer.h"
#include <pthread.h>
#include "db_helper.h"
#include <vector>

static int _flag = UWS_ON;
static zmqServer *_server = NULL;
static pthread_mutex_t _lock; 

struct SOptions
{
    char* config_file;
} options;

struct uwsgi_option options_cfg[] =
{
    {(char*)"plugin-cfg-file", required_argument, 0, (char*)"config file for rabbitmq servers", uwsgi_opt_set_str, &options.config_file, 0},
    { 0 }
};
    
void rw_create_lock(void)
{
    if(_flag == UWS_OFF)
    {
        pthread_mutex_init(&_lock, NULL);
        _flag = UWS_ON;
    }
}

//not thread safe
void rw_delete_lock(void)
{
    if(_flag == UWS_ON)
    {
        pthread_mutex_destroy(&_lock);
        _flag = UWS_OFF;
    }
}

static void *startS(void *ser)
{
	zmqServer *s = NULL;
	s = (zmqServer*)ser;
	s->startServer();
	return 0;
}
    
void _deinit()
{
    uwsgi_log("+++++ %s\n", __FUNCTION__);
    
    pthread_mutex_lock(&_lock);
    
    if(_flag == UWS_OFF)
    {
       pthread_mutex_unlock(&_lock); 
       return;
    }
    _server->stopServer();
    
    pthread_mutex_unlock(&_lock);
    
    usleep(UWS_COOLDOWN_TIME);
    
    if(_server != NULL)
        delete _server;
        
    _server = NULL;
}

int _init()
{
    uwsgi_log("+++++ %s config_file=%s\n", __FUNCTION__, options.config_file);
    if (!options.config_file)
    {
        uwsgi_log("The --plugin-cfg-file commandline argument is mandatory!\n");
        exit(1);
    }
    
    std::string _line, dum;
    int ret = 0, cnt = 0;
    pthread_t h;
    dbFactory *factory = NULL;
    dbConnection *conn = NULL;
    std::vector<std::string> vec;
    std::vector<std::string>::iterator v;
    char db_name[UWS_MAX_CONFIG_SIZE];
    char db_user_name[UWS_MAX_CONFIG_SIZE];
    char db_password[UWS_MAX_CONFIG_SIZE];
    char db_host_ip[UWS_MAX_CONFIG_SIZE];
    char db_host_port[UWS_MAX_CONFIG_SIZE];
    char zmq_sock[UWS_MAX_CONFIG_SIZE];
    char no_connections[UWS_MAX_CONFIG_SIZE];
    
    if(getConfig(options.config_file, UWS_KRM_DB_NAME , db_name) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_KRM_DB_NAME, options.config_file);
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_KRM_DB_USERNAME , db_user_name) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_KRM_DB_USERNAME, options.config_file);
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_KRM_DB_PASSWORD , db_password) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_KRM_DB_PASSWORD, options.config_file);
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_KRM_DB_HOST_ADDRESS , db_host_ip) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_KRM_DB_HOST_ADDRESS, options.config_file);
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_KRM_DB_PORT , db_host_port) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_KRM_DB_NAME, options.config_file);
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_NO_OF_CONNECTIONS , no_connections) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_NO_OF_CONNECTIONS, options.config_file);
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_ZMQ_SOCKET_ADDRESS , zmq_sock) <= 0)
    {
        uwsgi_log("%s key not found in %s, please check!!!\n", UWS_ZMQ_SOCKET_ADDRESS, options.config_file);
        exit(1);
    }
    
    uwsgi_log("\n%s:%s:%s:%s:%s:%s:%s\n", db_name, db_password, db_user_name, db_host_ip, db_host_port, no_connections, zmq_sock);
    
    if(_server == NULL)
    {
        _server = new zmqServer(zmq_sock, NULL , atoi(no_connections));
    }
    
    if(_server == NULL)
    {
        uwsgi_log("Out of memory!!!\n");
        exit(1);
    }
    
    factory = new dbFactory();
    conn = factory->create_db_connection(DB_POSTGRESS);
    
    if(!factory || !conn)
    {
        uwsgi_log("Out of memory!!!\n");
        exit(1);
    }
    
    conn->initialize("krm","krm","krm","127.0.0.1","5432");
    
    conn->db_select("select * from knowlus_knowlusdetails;", &vec);
    
    v = vec.begin();
    while( v != vec.end()) {
        dum = *v;
        uwsgi_log("conn str: %s\n", dum.c_str());
        ret = _server->addConnection(dum.c_str());
        if(ret != 1) {
            uwsgi_log("Error adding connection[%s] to pool factory!\n", dum.c_str());
            continue;
        }
        else {
            cnt++;
        }
        v++;
    }
    
    if(cnt == 0)
    {
        uwsgi_log("No connections were created , exiting!!!\n");
        exit(1);
    }
    
	ret = pthread_create(&h, NULL, startS, _server);
	if (ret != 0) {
		uwsgi_log("error in pthread_create: %d\n", ret);
	}
    
    rw_create_lock();
    atexit(_deinit);
    
    return UWSGI_OK;
}

struct SPluginConfig : public uwsgi_plugin
{
    SPluginConfig()
    {
        memset(this, 0, sizeof(*this));
        name = "uws_conn_pool";
        // request = _request;
        // Optional, set this only if you want commandline arguments from uwsgi.
        options = options_cfg;
        init = _init;
        //deinit = _deinit;
    }
};

SPluginConfig uws_conn_pool_plugin __attribute__((visibility("default")));