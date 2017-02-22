#include "zmqServer.h"
#include <string.h>

using namespace std;

/* zmq server funtion definations start*/

char* zmqServer::bAddress = NULL;										
FILE* zmqServer::ofile = NULL;
bool zmqServer::closeS = false;

zmqServer::zmqServer()
{
	int len = 0;
	bSocket = NULL;
	eSocket = NULL;
	fSocket = NULL;
	ctx = NULL;
	len = strlen(ZMQ_DEFAULT_BACKEND_ADDR);
	bAddress = new char[len + 1];
	memcpy(bAddress, ZMQ_DEFAULT_BACKEND_ADDR, len + 1);
	len = strlen(ZMQ_DEFAULT_EVENT_ADDR);
	eAddress = new char[len + 1];
	memcpy(eAddress, ZMQ_DEFAULT_EVENT_ADDR, len + 1);
	len = strlen(ZMQ_DEFAULT_FRONTEND_ADDR);
	fAddress = new char[len + 1];
	memcpy(fAddress, ZMQ_DEFAULT_FRONTEND_ADDR, len + 1);
	ofile = stderr;
	closeS = false;
    noWorkers = NO_OF_WORKERS;
	wData = new workerData[NO_OF_WORKERS];
	for (len = 0; len < NO_OF_WORKERS; len++)
	{
		wData[len].ctx = ctx;
		wData[len].identity = len;
		wData[len].rescueData = NULL;
        wData[len].factory = NULL;
	}
	isRunning = false;
    factories = new connPoolFactory*[NO_OF_WORKERS];
    for (len = 0; len < NO_OF_WORKERS; len++)
	{
        factories[len] = new connPoolFactory();
    }
}

zmqServer::zmqServer(const char* str1, const char* str2, int n)
{
	int len = 0;
	bSocket = NULL;
	eSocket = NULL;
	fSocket = NULL;
    bAddress = NULL;
    fAddress = NULL;
    eAddress = NULL;
	ctx = NULL;
	len = strlen(ZMQ_DEFAULT_BACKEND_ADDR);
	bAddress = new char[len + 1];
	memcpy(bAddress, ZMQ_DEFAULT_BACKEND_ADDR, len + 1);
	len = strlen(str1);
	fAddress = new char[len + 1];
	memcpy(fAddress, str1, len + 1);
    if(str2) {
        len = strlen(str2);
        eAddress = new char[len + 1];
        memcpy(eAddress, str2, len + 1);
    }
	ofile = stderr;
	closeS = false;
    noWorkers = n;
	wData = new workerData[n];
	for (len = 0; len < n; len++)
	{
		wData[len].ctx = ctx;
		wData[len].identity = len;
		wData[len].rescueData = NULL;
        wData[len].factory = NULL;
	}
	isRunning = false;
    factories = new connPoolFactory*[NO_OF_WORKERS];
    for (len = 0; len < NO_OF_WORKERS; len++)
	{
        factories[len] = new connPoolFactory();
    }
}

void zmqServer::updateAddress(const char* r_addr, const char* e_addr)
{
	int len = 0;

	if (r_addr != NULL)
	{
		len = strlen(r_addr);
		if (fAddress != NULL)
			delete[] fAddress;
		fAddress = new char[len + 1];
		memcpy(fAddress, r_addr, len + 1);
	}
	if (e_addr != NULL)
	{
		len = strlen(e_addr);
		if (eAddress != NULL)
			delete[] eAddress;
		eAddress = new char[len + 1];
		memcpy(eAddress, e_addr, len + 1);
	}
}

zmqServer::~zmqServer()
{
	if (bSocket != NULL)
		zmq_close(bSocket);
	if (eSocket != NULL)
		zmq_close(eSocket);
	if (fSocket != NULL)
		zmq_close(fSocket);
	if (ctx != NULL)
	{
		zmq_ctx_shutdown(ctx);
	}
	if (bAddress != NULL)
		delete[] bAddress;
	if (eAddress != NULL)
		delete[] eAddress;
	if (fAddress != NULL)
		delete[] fAddress;
	if (ofile != NULL && ofile != stderr)
		fclose(ofile);
	if (wData != NULL)
	{
		delete[] wData;
		wData = NULL;
	}
}

int zmqServer::addConnection(const char* conn_str, bool flag)
{
    if(!conn_str)
    {
        return -1;
    }
    
    if(noConnections == CP_MAXX_CONNECTIONS)
    {
        fprintf(stderr , "Error maximum allowed connections reached.");
        return -1;
    }
 
    char buf1[UWS_SMALL_BUFFER_SIZE], buf2[UWS_SMALL_BUFFER_SIZE];
    int i =0;
    
    if(parse_rmq_con_str(conn_str, strlen(conn_str), buf1, buf2, ',') <= 0)
    {
        fprintf(stderr, "Error in RMQ conn str: %s", conn_str);
        return -1;
    }
    
    for(i=0;i<noWorkers;i++)
    {
        connPoolFactory* _factory = factories[i];
        
        if(!_factory)
        {
            fprintf(stderr, "Factory not created!!!\n");
            return -1;
        }
        
        if(!_factory->addConnection( buf2, buf1, AMQP_CONN, flag))
        {
            fprintf(stderr,"Error adding connection er_msg: %s er_code: %d\n", _factory->getLastErrorMsg(), _factory->getLastErrorCode());
            continue;
        }
    }
    
    return SUCCESS;
}

int zmqServer::startServer()
{
	int res = 0, i = 0, retVal =0,retry=100+noWorkers,j=0;
	tHANDLE *h = new tHANDLE[noWorkers];
	bool workersCreated = false;
    int linger_period = ZMQ_LINGER_PERIOD;

	if (isRunning)
		return SUCCESS;
	isRunning = true;
	closeS = false;
	
	ctx = zmq_init(1);
	if (!ctx) {
		fprintf(ofile, "error in zmq_init: %s\n", zmq_strerror(errno));
		retVal = SER_ERR_INIT_CTX;
		goto on_error;
	}

	/*provide zmq context to workers*/
	for (i = 0; i < noWorkers; i++)
	{
		wData[i].ctx = ctx;
        wData[i].factory = factories[i];
	}

	/*create and bind the backend socket to communicate with workers*/
	bSocket = zmq_socket(ctx, ZMQ_DEALER);
	if (!bSocket) {
		fprintf(ofile, "error in zmq_socket_1: %s\n", zmq_strerror(errno));
		retVal = SER_ERR_SK_BACK_C;
		goto on_error;
	}
	res = zmq_bind(bSocket, bAddress);
	if (res != 0) {
		fprintf(ofile, "error in zmq_bind_backend: %s\n", zmq_strerror(errno));
		retVal = res;
		goto on_error;
	}
    /*set linger time for the socket sockets*/
    zmq_setsockopt(bSocket, ZMQ_LINGER, &linger_period, sizeof(linger_period));

	/*create and bind the event publisher socket to communicate with clients*/
    if(eAddress) {
        eSocket = zmq_socket(ctx, ZMQ_PUB);
        if (!eSocket) {
        fprintf(ofile, "error in zmq_socket_2: %s\n", zmq_strerror(errno));
        retVal = SER_ERR_SK_FRONT_L_C;
        goto on_error;
        }
        res = zmq_bind(eSocket, eAddress);
        if (res != 0) {
        fprintf(ofile, "error in zmq_bind_event: %s\n", zmq_strerror(errno));
        retVal = res;
        goto on_error;
        }
        /*set linger time for the socket sockets*/
        zmq_setsockopt(eSocket, ZMQ_LINGER, &linger_period, sizeof(linger_period));
    }
	/*create and bind the frontend socket to communicate with clients*/
	fSocket = zmq_socket(ctx, ZMQ_ROUTER);
	if (!fSocket) {
		fprintf(ofile, "error in zmq_socket_3: %s\n", zmq_strerror(errno));
		retVal = SER_ERR_SK_FRONT_R_C;
		goto on_error;
	}
	res = zmq_bind(fSocket, fAddress);
	if (res != 0) {
		fprintf(ofile, "error in zmq_bind_frontend: %s\n", zmq_strerror(errno));
		retVal = res;
		goto on_error;
	}
    /*set linger time for the socket sockets*/
    zmq_setsockopt(fSocket, ZMQ_LINGER, &linger_period, sizeof(linger_period));


	/*create workers*/
	i = 0;
	workersCreated = false;
	for (j = 0; j < retry; j++) {
		res = pthread_create(&h[i], NULL, workerThread, &wData[i]);
		if (res != 0) {
			fprintf(stderr, "error in pthread_create: %d\n", res);
		}
		else
		{
			res = pthread_detach(h[i]);
			if (res != 0) {
				fprintf(stderr, "error in pthread_detach: %d\n", res);
			}
			i++;
		} 
		if (i == noWorkers)
		{
			workersCreated = true;
			break;
		}
	}

	fprintf(ofile, "zmq server started 2.\n req address: %s , event address: %s \n", fAddress, eAddress);
	try {
		res = zmq_proxy(fSocket, bSocket, NULL);
		if (res != -1) {
			fprintf(ofile, "error in zmq_proxy: %s\n", zmq_strerror(errno));
			retVal = res;
			goto on_error;
		}
	}
	catch (std::exception &e)
	{
		fprintf(ofile, "Exception: %s", e.what());
	}

	return SUCCESS;

on_error:
	stopServer();
	return retVal;
}

void *zmqServer::workerThread(void* data)
{
	if (data == NULL)
	{
		return NULL;
	}

	void* s = NULL,*ctx_=NULL,*appctx =NULL,*resData=NULL;
	int res = 0, replies = 5, result = 0, sesid = 0, resDataSize = 0;
	workerData* w = NULL;
	std::string msg_str_r;
	std::string msg_str;
    std::string err_str;
	char buffA[MAX_MSG_SIZE], buffB[MAX_MSG_SIZE], *regBuff = NULL; 
	int len = 0, rMore = 0, bytesR = 0, bytesW = 0, regBuffSize =0;
	size_t rSize = 0;
	bool flag = true, retry =false;
	void* hint = NULL;
	int reqResult = SUCCESS;
    connPoolFactory *_factory = NULL;
    char mbuff[RW_BLOCK_SIZE];
    char nbuff[RW_Q_NAME_SIZE];
    char ibuff[RW_Q_NAME_SIZE];
    char bbuff[RW_Q_NAME_SIZE+5];
    int linger_period = ZMQ_LINGER_PERIOD;

	/*gather required data*/
	w = (workerData*)data;
	ctx_ = w->ctx;
    _factory = w->factory;

	/*create the socket*/
	s = zmq_socket(ctx_, ZMQ_DEALER);
	if (!s) {
		fprintf(ofile, "error in zmq_socket_4: %s\n", zmq_strerror(errno));
		exit(1);
	}

	/*connect to server address*/
	res = zmq_connect(s, bAddress);
	if (res != 0) {
		fprintf(ofile, "error in zmq_connect: %s\n", zmq_strerror(errno));
		exit(1);
	}

    /*set linger time for the socket sockets*/
    zmq_setsockopt(s, ZMQ_LINGER, &linger_period, sizeof(linger_period));

	try {
		while (true && _factory != NULL) {
			zmq_msg_t identity;
			zmq_msg_t copied_id;
			bool _success = true;

			/*check for thread termination request*/
			if (w->identity == -1 || closeS == true)
			{
				zmq_close(s);
				break;
			}

			res = zmq_msg_init(&copied_id);
			if (res != 0) {
				fprintf(ofile, "error in zmq_msg_init: %s\n", zmq_strerror(errno));
				continue;
			}
			res = zmq_msg_init(&identity);
			if (res != 0) {
				fprintf(ofile, "error in zmq_msg_init: %s\n", zmq_strerror(errno));
				zmq_msg_close(&copied_id);
				continue;
			}

			res = zmq_recvmsg(s, &identity, 0);
			if (res < 0) {
				fprintf(ofile, "error in zmq_recvmsg: %s\n", zmq_strerror(errno));
				continue;
			}
			res = zmq_msg_copy(&copied_id, &identity);
			if (res != 0) {
				fprintf(ofile, "error in zmq_msg_copy: %s\n", zmq_strerror(errno));
				continue;
			}
            
			res = zmq_recv(s, buffA, MAX_MSG_SIZE, 0);
			if (res < 0) {
				fprintf(ofile, "error in zmq_recvmsg: %s\n", zmq_strerror(errno));
				continue;
			}
			msg_str.append(static_cast<char*>(buffA), res);
			rSize = sizeof(int);
			do
			{
				rMore = 0;
				res = zmq_getsockopt(s, ZMQ_RCVMORE, &rMore , &rSize);
				if (res != 0)
				{
					fprintf(ofile, "error in zmq_getsockopt: %s\n", zmq_strerror(errno));
					break;
				}
				if (rMore == 1)
				{
					res = zmq_recv(s, buffA, MAX_MSG_SIZE, 0);
					if (res < 0) {
						fprintf(ofile, "error in zmq_recvmsg: %s\n", zmq_strerror(errno));
						continue;
					}
					else
					{
						msg_str.append(static_cast<char*>(buffA), res);
					}
				}
			} while (rMore == 1);
            
            parse_body(ibuff,nbuff,mbuff,msg_str.c_str(),msg_str.length());
            
            sprintf(bbuff, "%s%d", ibuff, 0);
            
            if(!_factory->publishMsgAmqp(bbuff, nbuff, mbuff, AMQP_CONN))
            {
                fprintf(stderr, "Primary error in publish er_msg: %s er_code: %d, trying secondary.\n", _factory->getLastErrorMsg(), _factory->getLastErrorCode());
                err_str.append("P err msg: ");
                err_str.append(_factory->getLastErrorMsg());
                sprintf(bbuff, "%s%d", ibuff, 1);
                if(!_factory->publishMsgAmqp(bbuff, nbuff, mbuff, AMQP_CONN))
                {
                    fprintf(stderr, "Secondary error in publish er_msg: %s er_code: %d.\n", _factory->getLastErrorMsg(), _factory->getLastErrorCode());
                    _success = false;
                    err_str.append("P err msg: ");
                    err_str.append(_factory->getLastErrorMsg());
                }
            }
            
            if(_success)
            {
                err_str.append("status: 0 success");
            }
            
            len = err_str.size();
            
            for (int reply = 0; reply < replies; ++reply) {
				retry = false;
				usleep(1000);
				res = zmq_sendmsg(s, &copied_id, ZMQ_SNDMORE);
				if (res < 0) {
					fprintf(ofile, "error in zmq_sendmsg_2: %s(%d)\n", zmq_strerror(errno), res);
					continue;
				}
				if (len <= MAX_MSG_SIZE)
				{
					memcpy(buffB, err_str.c_str(), len);
					res = zmq_send(s, buffB, len, 0);
					if (res < 0) {
						fprintf(ofile, "error in zmq_sendmsg_3: %s\n", zmq_strerror(errno));
						continue;
					}
				}
				else
				{
					bytesW = 0;
					bytesR = len;
					flag = true;
					while (bytesW<=len && flag)
					{
						memcpy(buffB, err_str.c_str() + bytesW, len);
						res = zmq_send(s, buffB, MAX_MSG_SIZE, ZMQ_SNDMORE);
						if (res < 0) {
							fprintf(ofile, "error in zmq_sendmsg_3: %s\n", zmq_strerror(errno));
							break;
						}
						else
						{
							bytesW = bytesW + MAX_MSG_SIZE;
							bytesR = bytesR - MAX_MSG_SIZE;
							if (bytesR <= MAX_MSG_SIZE)
								flag = false;
						}
					}
					if (bytesR > MAX_MSG_SIZE)
						retry = true;
					else
					{
						if (bytesR > 0)
						{
							memcpy(buffB, err_str.c_str() + bytesW, bytesR);
							res = zmq_send(s, buffB, bytesR, 0);
							if (res < 0) {
								fprintf(ofile, "error in zmq_sendmsg_3: %s\n", zmq_strerror(errno));
								continue;
							}
						}
					}
				}
				break;
			}
            
			/*clear buffers*/
			msg_str.clear();
            err_str.clear();
			/*signal free messages*/
			zmq_msg_close(&copied_id);
			zmq_msg_close(&identity);
		}
	}
	catch (std::exception &e)
	{
		fprintf(ofile, "Exception: %s", e.what());
	}
	
	return NULL;
}

int zmqServer::stopServer()
{
	if (!isRunning)
		return SUCCESS;
	closeS = true;
    usleep(UWS_COOLDOWN_TIME);
    int i =0;
	if (bSocket != NULL)
		zmq_close(bSocket);
	if (eSocket != NULL)
		zmq_close(eSocket);
	if (fSocket != NULL)
		zmq_close(fSocket);
	if (ctx != NULL)
	{
		zmq_ctx_shutdown(ctx);
		//zmq_ctx_term(ctx);
	}
	bSocket = NULL;
	eSocket = NULL;
	fSocket = NULL;
	ctx = NULL;
	isRunning = false;
	return SUCCESS;
}

int zmqServer::setLogFile(const char *path)
{
	ofile = fopen(path, "w");
	if (ofile == NULL)
		ofile = stderr;
	return SUCCESS;
}

zmqClient::zmqClient(const char *add)
{
	int len = 0, r1 = 0, r2 = 0, r3 = 0;
	socket = NULL;
	ctx = NULL;
    address = NULL;
	if(!add) {
        len = strlen(ZMQ_DEFAULT_FRONTEND_ADDR);
        address = new char[len + 1];
        memcpy(address, ZMQ_DEFAULT_FRONTEND_ADDR, len + 1);
    }
    else {
        len = strlen(add);
        address = new char[len + 1];
        memcpy(address, add, len + 1);
    }
}

zmqClient::~zmqClient()
{
	if (socket != NULL)
		zmq_close(socket);
	if (ctx != NULL)
	{
		zmq_ctx_shutdown(ctx);
		//zmq_ctx_term(ctx);
	}
	if (address != NULL)
		delete[] address;
}

int zmqClient::startClient(int _timeout)
{
	int res = 0, retVal = 0;

	int linger = ZMQ_LINGER_PERIOD;
    
    poll_timeout = _timeout;

	if (isRunning)
		return SUCCESS;
	isRunning = true;

	ctx = zmq_init(1);
	if (!ctx) {
		fprintf(stderr, "error in zmq_init: %s\n", zmq_strerror(errno));
		retVal = CLI_ERR_INIT_CTX;
		goto on_error;
	}

    socket = zmq_socket(ctx, ZMQ_DEALER);
    if (!socket) {
			fprintf(stderr, "error in zmq_socket: %s\n", zmq_strerror(errno));
            retVal = CLI_ERR_SOCK_B;
			goto on_error;
    }
    else
    {
        res = zmq_connect(socket, address);
        if (res != 0) {
				fprintf(stderr, "error in zmq_connect: %s\n", zmq_strerror(errno));
				retVal = CLI_ERR_SOCK_C;
                goto on_error;
        }
        zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger));
    }
    
    return SUCCESS;

on_error:
	stopClient();
	return retVal;
}


int zmqClient::stopClient()
{
	if (!isRunning)
		return SUCCESS;
	isRunning = false;
    
	usleep(UWS_COOLDOWN_TIME);
    
	if (socket != NULL)
		zmq_close(socket);
	if (ctx != NULL)
	{
		zmq_ctx_shutdown(ctx);
		//zmq_ctx_term(ctx);
	}
	ctx = NULL;
	socket = NULL;
	
	return SUCCESS;
}

int zmqClient::requestReply(void* inbuf, int inlen, char* outbuf, int outlen, int *blen)
{
	if (!isRunning)
		return CLI_ERR_NOT_STARTED;

	std::string msg_str_r;
	std::string msg_str;
	int res = 0, retVal = SUCCESS, len = 0, bytesW = 0, bytesR = 0, rMore = 0;
	bool condition = true, fault = false,flag =true,rAll=false,isalloc=false;
	zmq_pollitem_t pItems;
	size_t rSize = 0;
	char *buff = NULL, *rbuff = NULL;

	rSize = sizeof(rMore);
	msg_str.append(static_cast<char*>(inbuf),inlen);

    if (len <= MAX_MSG_SIZE)
    {
        res = zmq_send_const(socket, (void*)msg_str.c_str(), inlen, 0);
        if (res < 0) {
            fprintf(stderr, "error in zmq_sendmsg: %s\n", zmq_strerror(errno));
            retVal = errno;
        }
    }
    else
    {
        bytesW = 0;
        bytesR = len;
        flag = true;
        while (bytesW <= len && flag)
        {
            res = zmq_send_const(socket, (void*)(msg_str_r.c_str() + bytesW), MAX_MSG_SIZE, ZMQ_SNDMORE);
            if (res < 0) {
                fprintf(stderr, "error in zmq_sendmsg: %s\n", zmq_strerror(errno));
                retVal = errno;
                break;
            }
            else
            {
                bytesW = bytesW + MAX_MSG_SIZE;
                bytesR = bytesR - MAX_MSG_SIZE;
                if (bytesR <= MAX_MSG_SIZE)
                                            flag = false;
            }
        }
        if (bytesR > 0)
        {
            res = zmq_send_const(socket, (void*)(msg_str_r.c_str() + bytesW), bytesR, 0); 
            if (res < 0) {
                fprintf(stderr, "error in zmq_sendmsg: %s\n", zmq_strerror(errno));
                retVal = errno;
            }
        }
    }
	
    if(retVal!=SUCCESS)
    {
        return retVal;
    }
    
    zmq_poll(&pItems, 1, poll_timeout);
    rAll = true;
    if (pItems.revents & ZMQ_POLLIN)
    {
        res = zmq_recv(socket, buff, MAX_MSG_SIZE, 0);
        if (res < 0) {
            fprintf(stderr, "error in zmq_recvmsg: %s\n", zmq_strerror(errno));
            retVal = errno; 
        }
        else
            msg_str_r.append(static_cast<char*>(buff), res);
        do
        {
            rMore = 0;
            res = zmq_getsockopt(socket, ZMQ_RCVMORE, &rMore, &rSize);
            if (res != 0)
            {
                fprintf(stderr, "error in zmq_getsockopt: %s\n", zmq_strerror(errno));
                retVal = errno;
                break;
            }
            if (rMore == 1)
            {
                res = zmq_recv(socket, buff, MAX_MSG_SIZE, 0);
                if (res < 0) {
                    fprintf(stderr, "error in zmq_recvmsg: %s\n", zmq_strerror(errno));
                    retVal = errno; 
                    break;
                }
                else
                    msg_str_r.append(static_cast<char*>(buff), res);
            }
        } while (rMore == 1); 
    }
    else {
        retVal = REQUEST_TIMEOUT;
    }
    
    if(retVal == SUCCESS)
    {
        len = msg_str_r.length();
        len = (len > outlen) ? outlen : len;
        memcpy(outbuf, msg_str_r.c_str(), len);
        *blen = len;
    }
    
	return retVal;
}