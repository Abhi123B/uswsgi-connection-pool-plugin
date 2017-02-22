#include "amqp_conn_pool.h"

using namespace std;

amqpConnection::amqpConnection(const char* connectionStr, const char *_id_)
{
    if(connectionStr != NULL && _id_ != NULL)
    {
        connectionString.append(connectionStr);
        _id.append(_id_);
        pthread_mutex_init(&_lock, NULL);
        state = AMQP_ALIVE;
        noChannels = 0;
        try {
            qp = NULL;
            qp = new AMQP(connectionString);
        }
        catch(AMQPException e)
        {
            cerr<<"Exception creating connection.\n Msg: "<<e.getMessage()<<" Code: "<<e.getReplyCode()<<endl;
            state = AMQP_DEAD;
        }
    }
    else
    {
        state = AMQP_DEAD;
    }
    failCnt = 0;
    useCustomExchange = false;
}
    
amqpConnection::~amqpConnection()
{
    map<string, void*>::const_iterator _pos;
    
    pthread_mutex_lock(&_lock);
    for(_pos = hmap.begin(); _pos!= hmap.end(); _pos++)
    {
        amqpChannel *channel = (amqpChannel*)_pos->second;
        hmap.erase(_pos->first);
        delete channel;
    }
    pthread_mutex_unlock(&_lock);
 
    pthread_mutex_destroy(&_lock); 
  
    if(qp)
        delete qp;
}
    
void amqpConnection::setExchFlag(bool value)
{
    useCustomExchange = value;
}

int amqpConnection::addChannel(const char* queueName)
{
    if(state == AMQP_DEAD || !qp)
    {
        setLastError(CP_ERR_CONN_DEAD);
        setLastMsg(CP_ERR_CONN_DEAD_MSG);
        return UWS_FAILURE;
    }
    
    if(!queueName)
    {
        setLastError(CP_ERR_QUEUE_NAME_NULL);
        setLastMsg(CP_ERR_QUEUE_NAME_NULL_MSG);
        return UWS_FAILURE;
    }
    
    if(noChannels >= CP_MAXX_AMQP_CHANNELS)
    {
        setLastError(CP_ERR_MAXX_AMQP_CHANNELS_REACHED);
        setLastMsg(CP_ERR_MAXX_AMQP_CHANNELS_REACHED_MSG);
        return UWS_FAILURE;
    }
    
    amqpChannel *channel = NULL;
    map<string, void*>::const_iterator _pos;
    int status = UWS_SUCCESS;
    
    pthread_mutex_lock(&_lock);
    _pos = hmap.find(queueName);
    if(_pos != hmap.end()) 
    {
        return UWS_SUCCESS;
    }
    
    channel = new amqpChannel();
    if(channel == NULL)
    {
        setLastError(UWS_ERR_OUT_OF_MEMORY);
        setLastMsg(UWS_ERR_OUT_OF_MEMORY_MSG);
        return UWS_FAILURE;
    }
    
    try {
        channel->queueName = queueName;
        if (useCustomExchange){
            channel->ex = qp->createExchange("ex");
            channel->ex->Declare("ex" , "direct");
            channel->qu = qp->createQueue(queueName);
            channel->qu->Bind("ex", queueName);
        }
        else {
            channel->ex = qp->createExchange();
            channel->qu = qp->createQueue(queueName);
        }
        hmap.insert(pair<string,void*>(queueName,(void*)channel));
        noChannels++;
    }
    catch(AMQPException e)
    {
        status = UWS_FAILURE;
        setLastError(e.getReplyCode());
        setLastMsg(e.getMessage().c_str());
        failCnt++;
        fprintf(stderr, "failure:%d:%s:%d\n", e.getReplyCode(), e.getMessage().c_str(), failCnt);
        if(failCnt >= CP_MAXX_FAILURES)
        {
            failCnt = 0;
            state = AMQP_FAULTED;
        }
    }
    pthread_mutex_unlock(&_lock);
    
    return status;
}

int amqpConnection::removeChannel(const char* queueName)
{
    if(queueName == NULL)
    {
        return UWS_SUCCESS;
    }
    
    amqpChannel *channel = NULL;
    map<string, void*>::const_iterator _pos;
    
    _pos = hmap.find(queueName);
    if(_pos == hmap.end()) 
    {
        return UWS_SUCCESS;
    }
    else
    {
        pthread_mutex_lock(&_lock);
        channel = (amqpChannel*)_pos->second;
        fprintf(stdout, "\nremoving channel %s\n", queueName);
        hmap.erase(_pos->first);
        delete channel;
        pthread_mutex_unlock(&_lock);
    }
    
    return UWS_SUCCESS;
}

int amqpConnection::publishMessage(const char* queueName, const char* msg)
{
    if(state == AMQP_DEAD)
    {
        setLastError(CP_ERR_CONN_DEAD);
        setLastMsg(CP_ERR_CONN_DEAD_MSG);
        return UWS_FAILURE;
    }
    
    if(!queueName)
    {
        setLastError(CP_ERR_QUEUE_NAME_NULL);
        setLastMsg(CP_ERR_QUEUE_NAME_NULL_MSG);
        return UWS_FAILURE;
    }
    
    if(!msg)
    {
        return UWS_SUCCESS;
    }
    
    if(state == AMQP_FAULTED || !qp)
    {
        reConnect();
    }
    
    amqpChannel *channel = NULL;
    map<string, void*>::const_iterator _pos;
    int status = UWS_SUCCESS;
    
    _pos = hmap.find(queueName);
    if(_pos == hmap.end()) 
    {
        fprintf(stdout, "\nadding channel %s\n", queueName);
        if(addChannel(queueName))
        {
            _pos = hmap.find(queueName);
            if(_pos == hmap.end())
            {
                return UWS_FAILURE;
            }
            else{
                channel = (amqpChannel*)_pos->second;
            }
        }
    }
    else
    {
        channel = (amqpChannel*)_pos->second;
    }
    
    if(!channel)
    {
        setLastError(CP_ERR_CHANNEL_UNKNOWN_ERROR);
        setLastMsg(CP_ERR_CHANNEL_UNKNOWN_ERROR_MSG);
        return UWS_FAILURE;
    }
    
    try {
         channel->ex->Publish((char*)msg, strlen(msg), queueName);   
    }
    catch(AMQPException e)
    {
        status = UWS_FAILURE;
        setLastError(e.getReplyCode());
        setLastMsg(e.getMessage().c_str());
        removeChannel(queueName);
    }
    
    return status;
}

bool amqpConnection::isActive()
{
    return state == AMQP_ALIVE;
}

void amqpConnection::reConnect()
{
    fprintf(stdout,"reconnecting......\n");
    
    pthread_mutex_lock(&_lock);
    
    if(state != AMQP_FAULTED)
    {
        pthread_mutex_unlock(&_lock);
        return;
    }
   
    try {
        if(qp){
            qp->closeChannel();
            delete qp;
            qp = NULL;
        }

        qp = new AMQP(connectionString);
        state = AMQP_ALIVE;
    }
    catch(AMQPException e)
    {
        cerr<<"Exception creating connection.\n Msg: "<<e.getMessage()<<" Code: "<<e.getReplyCode()<<endl;
    }
    
    pthread_mutex_unlock(&_lock);
}

connPoolFactory::connPoolFactory()
{
    activeConnections = 0;
    pthread_mutex_init(&_lock, NULL);
}
    
connPoolFactory::~connPoolFactory()
{
    pthread_mutex_destroy(&_lock);
}
    
int connPoolFactory::addConnection(const char* connectionStr, const char* idStr, connType _type, bool flag)
{
    if(!connectionStr)
    {
        setLastError(CP_ERR_CONN_STR_NULL);
        setLastMsg(CP_ERR_CONN_STR_NULL_MSG);
        return UWS_FAILURE;
    }
    
    if(!idStr)
    {
        setLastError(CP_ERR_CONN_ID_NULL);
        setLastMsg(CP_ERR_CONN_ID_NULL_MSG);
        return UWS_FAILURE;
    }
    
    if(activeConnections >= CP_MAXX_CONNECTIONS)
    {
        setLastError(CP_ERR_MAXX_CONN_REACHED);
        setLastMsg(CP_ERR_MAXX_CONN_REACHED_MSG);
        return UWS_FAILURE;
    }
    
    int status = UWS_SUCCESS;
    amqpConnection *conn = NULL;
    string _id(idStr);
    
    switch(_type)
    {
        case AMQP_CONN:
            conn = new amqpConnection(connectionStr , idStr);
            if(conn == NULL)
            {
                setLastError(UWS_ERR_OUT_OF_MEMORY);
                setLastMsg(UWS_ERR_OUT_OF_MEMORY_MSG);
                status = UWS_FAILURE;
            }
            else {
                if(!conn->isActive())
                {
                    setLastError(CP_ERR_CONN_DEAD);
                    setLastMsg(CP_ERR_CONN_DEAD_MSG);
                    status = UWS_FAILURE;
                }
                else {
                    if(flag)
                        conn->setExchFlag(flag);
                    pthread_mutex_lock(&_lock);
                    activeConnections++;
                    poolMap.insert(pair<string, void*>(_id, (void*)conn));
                    pthread_mutex_unlock(&_lock);
                }
            }
            break;
        default:
            setLastError(CP_ERR_CONNECTION_TYPE_UNSUPPORTED);
            setLastMsg(CP_ERR_CONNECTION_TYPE_UNSUPPORTED_MSG);
            status = UWS_FAILURE;
    }
    
    return status;
}
    
int connPoolFactory::removeConnection(const char* idStr, connType _type)
{
    if(!idStr)
    {
        return UWS_SUCCESS;
    }
    
    map<string, void*>::const_iterator _pos;
    amqpConnection *conn = NULL;
    string _id(idStr);
    int status = UWS_SUCCESS;
    
    switch(_type)
    {
        case AMQP_CONN:
            _pos = poolMap.find(_id);
            if(_pos != poolMap.end())
            {
                conn = (amqpConnection*)_pos->second;
                pthread_mutex_lock(&_lock);
                delete conn;
                poolMap.erase(_pos->first);
                activeConnections--;
                pthread_mutex_unlock(&_lock);
            }
        default:
            setLastError(CP_ERR_CONNECTION_TYPE_UNSUPPORTED);
            setLastMsg(CP_ERR_CONNECTION_TYPE_UNSUPPORTED_MSG);
            status = UWS_FAILURE;
    }
    
    return status;
}
    
int connPoolFactory::publishMsgAmqp(const char* _id, const char* queueName ,const char* msg, connType _type)
{
    if(!_id)
    {
        setLastError(CP_ERR_CONN_ID_NULL);
        setLastMsg(CP_ERR_CONN_ID_NULL_MSG);
        return UWS_FAILURE;
    }   
    
    string id_(_id);
    map<string, void*>::const_iterator _pos;
    amqpConnection *conn = NULL;
    int status = UWS_SUCCESS;
    
    _pos = poolMap.find(id_);
    if(_pos == poolMap.end())
    {
        setLastError(CP_ERR_CONN_NOT_FOUND);
        setLastMsg(CP_ERR_CONN_NOT_FOUND_MSG);
        return UWS_FAILURE;
    }
    
    switch(_type)
    {
        case AMQP_CONN:
            conn = (amqpConnection*)_pos->second;
            status = conn->publishMessage(queueName, msg);
            if(status != UWS_SUCCESS)
            {
                setLastError(conn->getLastErrorCode());
                setLastMsg(conn->getLastErrorMsg());
            }
            break;
        default:
            setLastError(CP_ERR_CONNECTION_TYPE_UNSUPPORTED);
            setLastMsg(CP_ERR_CONNECTION_TYPE_UNSUPPORTED_MSG);
            status = UWS_FAILURE;
    }
        
    return status;
}