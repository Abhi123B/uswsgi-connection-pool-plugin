#ifndef __ZMQSERVER_H__
#define __ZMQSERVER_H__

#include "uws_pg_config.h"
#include <stdlib.h>     
#include <time.h>      
#include "amqp_conn_pool.h"
#include <zmq.h>
#include "parser_defs.h"

struct workerData
{
	void*									ctx;                // the zmq context
	int										identity;           // worker identity
	void*									rescueData;         // data in case of deadlock
    connPoolFactory*                        factory;            // the pool factory
};

class zmqServer 
{
	void*					bSocket;										// backend socket internal to server to communicate with workers
	void*					fSocket;										// frontend socket to communicate with clients
	void*					eSocket;										// socket to send events to clients
	void*					ctx;											// zmq context
	char*					fAddress;										// address for fSocket to bind to
	char*					eAddress;										// address for cSocket to bind to
	static char*			bAddress;										// address for bSocket to bind to
	static FILE*			ofile;											// file used for logging
	static bool				closeS;											// server closing;
	bool					isRunning;										// check instance running
    connPoolFactory**       factories;                                      // the pool factories use to make connections
    struct workerData*		wData;											// data provided to worker thread
    int                     noWorkers;                                      // no of worker threads
    int                     noConnections;                                  // no of connections

public:
	zmqServer();
	/*pass bAddress,fAddress,no of workers respectively*/
	zmqServer(const char*, const char*, int n = NO_OF_WORKERS);
	~zmqServer();
    
    /* All returnable fuctions returns SUCCESS on sucessfull execution or appropriate error code on failure.*/
	/* start server instance this is a blocking call and will return only when current zmq coxtext is terminated*/
	int startServer();
	/*worker thread*/
	static void *workerThread(void*);
	/*stop running instace of server*/
	int stopServer();
	/*set the file to be used for logging if not set standard error will be used
     * @param 1     the full filepath
     * */
	int setLogFile(const char*);
	/*update addresses during runtime
     * @param 1     the frontend address
     * @param 2     the event address
     * */
	void updateAddress(const char*, const char*);
    /*add connection to pool factories
     * @param 1     the connection string
     * @param 2     a flag that specifies if current operation is for testing
     * */
    int addConnection(const char*, bool);
};

class zmqClient
{
	void*							socket;							// socket to communicate to server
	void*							ctx;							// current zmq context
	char*							address;						// address of socket connect
    int                             poll_timeout;                   // time in milliseconds after which abandon reply
    bool                            isRunning;
    
public:
	zmqClient(const char*);
	~zmqClient();
    
    /* All returnable fuctions returns SUCCESS on sucessfull execution or appropriate error code on failure.*/
	/* start server instance this is a blocking call and will return only when current zmq coxtext is terminated*/
    
	/*start a client instance
     * @param 1     the timeout value after which reply is abandoned
     * */
	int startClient(int);
	/*stop a client instance*/
	int stopClient();
	/* basic funtion for making a request and receiving coresponding response
     * in case reply data is greater than buffer size it ignores the remaining data
     * application needs to ensure that buffer size is enough
     * @param 1     the request byte data.
     * @param 2     the size of param1
     * @param 3     the buffer to receive reply data
     * @param 4     the buffer size
     * @param 5     the amount of data written
     * */
	int requestReply(void*, int, char*, int, int*);
};

#endif