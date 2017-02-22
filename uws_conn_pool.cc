#include <uwsgi.h>
#include <stdlib.h>
#include <stdio.h>
#include <map>
#include <amqp_conn_pool.h>
#include <string.h>
#include <uws_pg_config.h>
#include <parser_defs.h>
#include <zmqServer.h>
#include <file_helper.h>
#include <vector>
#include <pthread.h>

#if UWS_ENABLE_DB_SUPPORT
    #include "db_helper.h"
#endif

static int _flag = UWS_ON;
static zmqServer *_server = NULL;
static zmqClient *_client = NULL;

struct SOptions
{
    char* config_file;
} options;

struct uwsgi_option options_cfg[] =
{
    {(char*)"plugin-cfg-file", required_argument, 0, (char*)"config file for rabbitmq servers", uwsgi_opt_set_str, &options.config_file, 0},
    { 0 }
};

static void *startS(void *ser)
{
	zmqServer *s = NULL;
	s = (zmqServer*)ser;
	s->startServer();
	return 0;
}
    
/*In uwsgi master mode this callback is called just before the master process exits*/     
void _master_cleanup()
{
    uwsgi_log("+++++ %s cleaning server %p\n", __FUNCTION__, _server);
    
    if(_flag == UWS_OFF)
    {
       return;
    }
    _server->stopServer();
    
    usleep(UWS_COOLDOWN_TIME);
    
    if(_server != NULL)
        delete _server;
        
    _server = NULL;
    _flag = UWS_OFF;
}

/* Whenever a new worker process is forked this callback is called.
 * In Master mode , master process works as the administator while the worker process actually process incoming requests
 * */
void _post_fork()
{
    uwsgi_log("+++++ %s config_file=%s\n", __FUNCTION__, options.config_file);
    if (!options.config_file)
    {
        uwsgi_log("The --plugin-cfg-file commandline argument is mandatory!\n");
        exit(1);
    }
    
    char zmq_sock[UWS_MAX_CONFIG_SIZE];
    int res = 0;
    
    if(_client)
    {
        // _post_fork called twice this should not happen still applying this failsafe
        return;
    }
    
    // create zmq client sender 
    if(getConfig(options.config_file, UWS_ZMQ_SOCKET_ADDRESS , zmq_sock) <= 0)
    {
        uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_ZMQ_SOCKET_ADDRESS, options.config_file, UWS_DEF_ZMQ_SOCK_ADDR);
        memcpy(zmq_sock,UWS_DEF_ZMQ_SOCK_ADDR,strlen(UWS_DEF_ZMQ_SOCK_ADDR)+1);
    }
    
    _client = new zmqClient(zmq_sock);
    if(!_client) {
        uwsgi_log("Out of memory!!!\n");
        exit(1);
    }
    
    res = _client->startClient(DEF_POLL_TIMEOUT);
    if(res != SUCCESS){
        uwsgi_log("Error starting client code : %d\n", res);
        exit(1);
    }
}

/*this callback called whenever a worker exits*/
void _atexit()
{
    uwsgi_log("+++++ %s cleaning client=%p\n", __FUNCTION__, _client);
    if(_client)
    {
        _client->stopClient();
        delete _client;
        _client = NULL;
    }
}

/* this callback is called when master process starts*/
int _init()
{
    uwsgi_log("+++++ %s config_file=%s\n", __FUNCTION__, options.config_file);
    if (!options.config_file)
    {
        uwsgi_log("The --plugin-cfg-file commandline argument is mandatory!\n");
        exit(1);
    }
    
    std::string _line, dum;
    int ret = 0, cnt = 0, db_flag = 0;
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
    char use_db[UWS_MAX_CONFIG_SIZE];
    char use_custom_ex[UWS_MAX_CONFIG_SIZE];
    int _flag = 0;
    
    ::_flag = UWS_ON;
    
    if(_server)
    {
        // _init called twice this should not happen still applying this failsafe
        return UWSGI_OK;
    }
    
    // create zmq server listener 
    if(getConfig(options.config_file, UWS_ZMQ_SOCKET_ADDRESS , zmq_sock) <= 0)
    {
        uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_ZMQ_SOCKET_ADDRESS, options.config_file, UWS_DEF_ZMQ_SOCK_ADDR);
        memcpy(zmq_sock,UWS_DEF_ZMQ_SOCK_ADDR,strlen(UWS_DEF_ZMQ_SOCK_ADDR)+1);
    }
    
    if(getConfig(options.config_file, UWS_NO_OF_CONNECTIONS , no_connections) <= 0)
    {
        uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_NO_OF_CONNECTIONS, options.config_file, UWS_DEF_RMQ_CONNS);
        memcpy(no_connections,UWS_DEF_RMQ_CONNS,strlen(UWS_DEF_RMQ_CONNS)+1);
    }
    
    _server = new zmqServer(zmq_sock, NULL , atoi(no_connections));
    
    if(_server == NULL)
    {
        uwsgi_log("Out of memory!!!\n");
        exit(1);
    }
    
    if(getConfig(options.config_file, UWS_USE_DB , use_db) > 0)
    {
        if(strcmp(use_db,"true") == 0) {
            db_flag = 1;
        }
    }
    
    if(getConfig(options.config_file, UWS_CUSTOM_EXCHANGE , use_custom_ex) > 0)
    {
        if(strcmp(use_custom_ex,"true") == 0) {
            _flag = 1;
        }
    }
    
    if(db_flag) {
#if UWS_ENABLE_DB_SUPPORT
        if(getConfig(options.config_file, UWS_KRM_DB_NAME , db_name) <= 0)
        {
            uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_KRM_DB_NAME, options.config_file, UWS_DEF_DB_NAME);
            memcpy(db_name,UWS_DEF_DB_NAME,strlen(UWS_DEF_DB_NAME)+1);
        }
    
        if(getConfig(options.config_file, UWS_KRM_DB_USERNAME , db_user_name) <= 0)
        {
            uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_KRM_DB_USERNAME, options.config_file,UWS_DEF_DB_USER_NAME);
            memcpy(db_user_name,UWS_DEF_DB_USER_NAME,strlen(UWS_DEF_DB_USER_NAME)+1);
        }
    
        if(getConfig(options.config_file, UWS_KRM_DB_PASSWORD , db_password) <= 0)
        {
            uwsgi_log("%s key not found in %s, using default val %s.\n\n", UWS_KRM_DB_PASSWORD, options.config_file,UWS_DEF_DB_PASS);
            memcpy(db_password,UWS_DEF_DB_PASS,strlen(UWS_DEF_DB_PASS)+1);
        }
    
        if(getConfig(options.config_file, UWS_KRM_DB_HOST_ADDRESS , db_host_ip) <= 0)
        {
            uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_KRM_DB_HOST_ADDRESS, options.config_file,UWS_DEF_DB_HOST_ADDR);
            memcpy(db_host_ip,UWS_DEF_DB_HOST_ADDR,strlen(UWS_DEF_DB_HOST_ADDR)+1);
        }
    
        if(getConfig(options.config_file, UWS_KRM_DB_PORT , db_host_port) <= 0)
        {
            uwsgi_log("%s key not found in %s, using default val %s.\n", UWS_KRM_DB_NAME, options.config_file, UWS_DEF_DB_HOST_PORT);
            memcpy(db_host_port,UWS_DEF_DB_HOST_PORT,strlen(UWS_DEF_DB_HOST_PORT)+1);
        }
    
        factory = new dbFactory();
        if(!factory) {
            uwsgi_log("Out of memory!!!\n");
            exit(1);
        }
    
        conn = factory->create_db_connection(DB_POSTGRESS);
        if(!conn) {
            uwsgi_log("Out of memory!!!\n");
            exit(1);
        }
    
        conn->initialize(db_name,db_user_name,db_password,db_host_ip,db_host_port);
    
        conn->db_select("select rbmq_ip, id, name , is_active from knowlus_knowlusdetails;", &vec);
    
        v = vec.begin();
        while( v != vec.end()) {
            dum = *v;
            uwsgi_log("conn str: %s\n", dum.c_str());
            ret = _server->addConnection(dum.c_str(), (_flag == 1));
            if(ret != 1) {
                uwsgi_log("Error adding connection[%s] to pool factory!\n", dum.c_str());
                continue;
            }
            else {
                cnt++;
            }
            v++;
        }
#else
#error "Database support is not added"
#endif   
    }
    
    else {
        get_rmq_conn_str(options.config_file, UWS_RMQ_CONN_STR, &vec);
        v = vec.begin();
        while( v != vec.end()) {
            dum = *v;
            uwsgi_log("conn str: %s\n", dum.c_str());
            ret = _server->addConnection(dum.c_str(), (_flag == 1));
            if(ret != 1) {
                uwsgi_log("Error adding connection[%s] to pool factory!\n", dum.c_str());
                continue;
            }
            else {
                cnt++;
            }
            v++;
        }
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
    
    return UWSGI_OK;
}

/*this is called whenever a request is recieved by uwsgi*/
int _request(struct wsgi_request *wsgi_req)
{
     uwsgi_log("+++++ %s\n", __FUNCTION__);
    
     if (uwsgi_parse_vars(wsgi_req)) {
             return -1;
     }

     ssize_t body_len = 0;
     int resp = CLI_ERR_FAULTED_STATE, len = 0;
     char buff[UWS_LARGE_BUFFER_SIZE];
     
     len = strlen(ZMQ_INTERNAL_ERR_MSG);
     memcpy(buff , ZMQ_INTERNAL_ERR_MSG ,len);

     // get the body
     char *body = uwsgi_request_body_read(wsgi_req, 0 , &body_len);
     if (body != NULL)
     {
         if(_client)
         {
             resp = _client->requestReply((void*)body, body_len, buff, UWS_LARGE_BUFFER_SIZE, &len);
             if(resp == REQUEST_TIMEOUT)
             {
                 len = strlen(ZMQ_TIMEOUT_MSG);
                 memcpy(buff , ZMQ_TIMEOUT_MSG ,len);
             }
         }
     }
    
     // send status
     if (uwsgi_response_prepare_headers(wsgi_req, "200 OK", 6)) return -1;
     // send content_type
     if (uwsgi_response_add_content_type(wsgi_req, "text/plain", 10)) return -1;     
     // send the body
     if (uwsgi_response_write_body_do(wsgi_req, buff, len)) return -1;

     return UWSGI_OK;
}

/* A stucture reprenting the plugin, i.e the exposed shared object for uwsgi to load*/
struct SPluginConfig : public uwsgi_plugin
{
    SPluginConfig()
    {
        memset(this, 0, sizeof(*this));
        name = "uws_conn_pool";
        options = options_cfg;
        init = _init;
        request = _request;
        atexit = _atexit;
        master_cleanup = _master_cleanup;
        post_fork = _post_fork;
    }
};

SPluginConfig uws_conn_pool_plugin __attribute__((visibility("default")));
