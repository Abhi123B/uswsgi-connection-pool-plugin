#ifndef __UWS_PG_CONFIG_H__
#define __UWS_PG_CONFIG_H__

enum connType{
    AMQP_CONN,
    OTHER_CONN
};

/*universal definations*/
#define UWS_SUCCESS                             1
#define UWS_FAILURE                             0
#define UWS_PARTIAL_SUCCESS                     2
#define UWS_MAXX_ERROR_MSG_SIZE                 100
#define UWS_ERR_OUT_OF_MEMORY                   -1
#define UWS_ERR_OUT_OF_MEMORY_MSG               "OUT OF MEMORY!!!"
#define UWS_INFINITE                            1000*60*60*10
#define UWS_COOLDOWN_TIME                       2*1000*1000
#define UWS_ON                                  1
#define UWS_OFF                                 0
#define UWS_CONCURRENT_CONN                     5
#define UWS_KRM_DB_NAME                         "DB_NAME"
#define UWS_KRM_DB_USERNAME                     "DB_USERNAME"
#define UWS_KRM_DB_PASSWORD                     "DB_PASSWORD"
#define UWS_KRM_DB_HOST_ADDRESS                 "DB_HOST_ADDRESS"
#define UWS_KRM_DB_PORT                         "DB_PORT"
#define UWS_NO_OF_CONNECTIONS                   "NO_OF_CONNECTIOS"
#define UWS_ZMQ_SOCKET_ADDRESS                  "ZMQ_SOCK_ADDRESS"
#define UWS_USE_DB                              "USE_DB"
#define UWS_RMQ_CONN_STR                        "RMQ_CONN_STR"
#define UWS_CUSTOM_EXCHANGE                     "CUSTOM_EX_FLAG"
#define UWS_MAX_CONFIG_SIZE                     128
#define UWS_SMALL_BUFFER_SIZE                   128
#define UWS_MEDIUM_BUFFER_SIZE                  256
#define UWS_LARGE_BUFFER_SIZE                   512
#define UWS_DEF_ZMQ_SOCK_ADDR                   "ipc:///tmp/feeds/0"
#define UWS_DEF_RMQ_CONNS                       "10"
#define UWS_DEF_DB_NAME                         "name-db"
#define UWS_DEF_DB_USER_NAME                    "uname-db"
#define UWS_DEF_DB_PASS                         "pass-db"
#define UWS_DEF_DB_HOST_ADDR                    "127.0.0.1"
#define UWS_DEF_DB_HOST_PORT                    "12345"
#define UWS_ENABLE_DB_SUPPORT                   1

/* AMQP connection pool related definations */
#define CP_MAXX_CONN_ID_SIZE                    128
#define CP_MAXX_ERROR_MSG_SIZE                  128
#define CP_MAXX_CONNECTIONS                     100
#define CP_MAXX_AMQP_CHANNELS                   100
#define CP_MAXX_FAILURES                        5
#define CP_ERR_MAXX_CONN_REACHED                -101
#define CP_ERR_MAXX_CONN_REACHED_MSG            "Maximum no of allowed connections reached."
#define CP_ERR_CONN_DEAD                        -102
#define CP_ERR_CONN_DEAD_MSG                    "Connection to server faulted or dead."
#define CP_ERR_QUEUE_NAME_NULL                  -103
#define CP_ERR_QUEUE_NAME_NULL_MSG              "AMQP queue name cannot be NULL."
#define CP_ERR_MAXX_AMQP_CHANNELS_REACHED       -104
#define CP_ERR_MAXX_AMQP_CHANNELS_REACHED_MSG   "Maximum no of allowed channels per amqp connection reached."
#define CP_ERR_CHANNEL_UNKNOWN_ERROR            -105
#define CP_ERR_CHANNEL_UNKNOWN_ERROR_MSG        "Unknown error occured in amqp channel."
#define CP_ERR_CONNECTION_TYPE_UNSUPPORTED      -106
#define CP_ERR_CONNECTION_TYPE_UNSUPPORTED_MSG  "The connection type is not supported."
#define CP_ERR_CONN_STR_NULL                    -107
#define CP_ERR_CONN_STR_NULL_MSG                "The connection string cannot be NULL."
#define CP_ERR_CONN_ID_NULL                     -108
#define CP_ERR_CONN_ID_NULL_MSG                 "The connection id cannot be NULL."
#define CP_ERR_CONN_NOT_FOUND                   -109
#define CP_ERR_CONN_NOT_FOUND_MSG               "The connection with requested id was not found."
#define CP_AMQP_USE_CUSTOM_EXCHANGE             1
         
/* Read / write queue definations. */

#define RW_BLOCK_SIZE                           1024
#define RW_BLOCK_COUNT                          5*1024
#define RW_Q_NAME_SIZE                          128
#define RW_INVALID_EVENT                        -1

/* zero mq defs*/
#ifdef __linux__
#define tHANDLE    pthread_t
#elif defined _WIN32
#define tHANDLE    HANDLE
#endif

#define BACK_ADD								10
#define FRONT_ADD_LOCAL							20
#define FRONT_ADD_REMOTE						30
#define SER_ERR_INIT_CTX						-1
#define SER_ERR_SK_BACK_C						-2
#define SER_ERR_SK_FRONT_L_C					-3
#define SER_ERR_SK_FRONT_R_C					-4
#define SUCCESS									 1
#define FAILURE									-1
#define HEARTBEAT_FAILURE_STATE					-701
#define SER_ERR_SK_BACK_B						-5
#define SER_ERR_SK_FRONT_L_B					-6
#define SER_ERR_SK_FRONT_R_B					-7
#define SER_ERR_IVALID_REQUEST					-8
#define SER_ERR_INS_NOT_RUNNING					-9
#define SER_ERR_MESSAGE_INIT					-10
#define SER_ERR_WRK_NOT_CREATED					-11
#define SER_ERR_RETRYANOTHERSERVER				-12
#define SER_ERR_NOSERVERAVAILABLE				-13
#define NO_OF_WORKERS							 40
#define CLI_ERR_INIT_CTX						-101
#define	CLI_ERR_SOCK_C							-102
#define	CLI_ERR_SOCK_OP							-103
#define	CLI_ERR_SOCK_B							-104
#define CLI_ERR_NOT_STARTED						-105
#define CLI_ERR_CREATE_EVENT_FAILED				-106
#define CLI_ERR_CREATE_LISTENER_FAILED			-107
#define CLI_ERR_MSG_INIT_FAIL					-108
#define CLI_ERR_REQ_BRK_NOT_RUNNING				-109
#define CLI_ERR_REQ_BRK_BUSY					-110
#define CLI_ERR_MIN_BRK_NOT_STARTED				-112
#define CLI_ERR_FAULTED_STATE					-115
#define CLI_OUT_OF_MEM							-117
#define SYNC_SUCCESS							 3
#define SYNC_FAILURE							-3
#define MAXX_SESSIONS							100
#define DEFAULT_EXPIRY_TIMEOUT					10*60*1000
#define	ZMQ_OUT_OF_MEM							-201
#define SM_ERR_CREATE_EVENT_FAILED				-202
#define SM_ERR_SESSION_EXPIRED					-203
#define SM_ERR_INVALID_SESSION_ID				-204
#define SM_ERR_MAX_SESSION_REACHED				-205
#define IS_64BIT								0
#define MAX_MSG_SIZE							1000
#define INTERNAL_BUFFER_SIZE					15
#define SESSION_NOT_STARTED						-12345	
#define DEF_POLL_TIMEOUT						3*1000
#define HB_POLL_TIMEOUT							500*100
#define WORKER_TIMEOUT							10*1000*50
#define REQUEST_TIMEOUT							-301
#define INVALID_ID								15
#define	MAXX_CONNECTIONS						40	
#define MINN_CONNECTIONS						20
#define REGISTER_DATA_SIZE						100
#define SERVER_INSTANCE_FAULTED					-302
#define USE_POOL_CHECK							0
#define MAX_HBF_COUNT							10
#define ZMQ_LINGER_PERIOD                       1000
#define ZMQ_DEFAULT_BACKEND_ADDR                "inproc://backend"
#define ZMQ_DEFAULT_FRONTEND_ADDR               "tcp://127.0.0.1:5590"
#define ZMQ_DEFAULT_EVENT_ADDR                  "tcp://127.0.0.1:5570"
#define ZMQ_INTERNAL_ERR_MSG                    "internal error"
#define ZMQ_TIMEOUT_MSG                         "timeout in sending msg"

#ifdef _WIN32
#define ZMQ_EXPORT_CLASS __declspec(dllexport)
#elif defined __linux__
#define ZMQ_EXPORT_CLASS __attribute__ ((dllexport))
#endif

#endif