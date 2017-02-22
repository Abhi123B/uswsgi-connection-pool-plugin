#ifndef __AMQP_CONN_POOL_H__
#define __AMQP_CONN_POOL_H__

#ifdef __linux__
#else
#error "OS not supported."
#endif

#include <stdio.h>
#include <iostream>
#include <string.h>
#include <AMQPcpp.h>
#include <signal.h>
#include <fstream>
#include <pthread.h>
#include <map>
#include <ctime>
#include <unistd.h>
#include "uws_pg_config.h"
#include "error_defs.h"

enum amqpState
{
    AMQP_ALIVE = 0 ,
    AMQP_FAULTED = 1,
    AMQP_DEAD = 2
};

/* A basic stucture defining an amqp channel
 * */

struct amqpChannel
{
    std::string                 queueName;
    AMQPQueue*                  qu;
    AMQPExchange*               ex;
};

/* AMQP connection class , describes a connection to a rabbit mq*/
class amqpConnection : public uwsError
{
    AMQP*                       qp;                 // AMQP connection object
    std::string                 connectionString;   // A string which is used to establish the connection
    std::map<std::string,void*> hmap;               // Map of channels to queuename
    pthread_mutex_t             _lock;              // Simple lock
    int                         noChannels;         // total active channels
    std::string                 _id;                // knowlus id
    amqpState                   state;              // connection state
    int                         failCnt;            // current failure cnt
    void                        reConnect(void);    // re establish connection
    bool                        useCustomExchange;  // use custum exchange of RMQ
    
public:

    amqpConnection(const char* connectionStr, const char *_id);
    ~amqpConnection();
    
    /* add a channel to this connection 
     * @param queueName     the RMQ queue name
     * @retun a value greater than zero on success
     * */
    int addChannel(const char* queueName);
    
    /* remove a channel from this connection 
     * @param queueName     the RMQ queue name
     * @retun a value greater than zero on success
     * */
    int removeChannel(const char* queueName);
    
    /* publish a message to the queue 
     * @param queueName     the RMQ queue name
     * @param msg           the message to be sent to RMQ
     * @retun a value greater than zero on success
     * */
    int publishMessage(const char* queueName, const char* msg);
    
    /* check if connection is healthy 
     * @retun true if healthy
     * */
    bool isActive(void);
    
    /* set this flag for testing purpose only.
     * it creates a custom exchange so that cunsumers are not able to get the msg sent.
     * */
    void setExchFlag(bool value);
};

/* The connection pool class, contains all the active connections. 
 * Implements Factory design pattern to some extent */
 
class connPoolFactory : public uwsError
{
    int                         activeConnections; // Total number of active connections
    std::map<std::string,void*> poolMap; // map of connections to knowlus id
    pthread_mutex_t             _lock; // a simple lock
    char                        lastError[CP_MAXX_ERROR_MSG_SIZE]; // the last error message generated
    
public:

    connPoolFactory();
    ~connPoolFactory();
    
    /* add connection to factory
     * @param   connectionStr the connection to rabbitmq
     * @param   idStr         the knowlus id
     * @param   _type         the type of connection like AMQP, MQTT etc
     * @param   flag          internal use for testing purpose
     * @return  value greater than 0
     * */
    int addConnection(const char* connectionStr, const char* idStr, connType _type, bool flag=false);
    
    /* remove connection from factory 
     * @param   idStr         the knowlus id
     * @param   _type       the type of connection like AMQP, MQTT etc
     * @return  value greater than 0 
     */
    int removeConnection(const char* idStr, connType _type);
    
    /* publish a message to RMQ described by the knowlus id 
     * @param   _id         the knowlus id
     * @param   _type       the type of connection like AMQP, MQTT etc
     * @param   queueName   the queue to which to send the message
     * @param   msg         the msg to be sent
     * @return  value greater than 0 on success
     * */
    int publishMsgAmqp(const char* _id, const char* queueName ,const char* msg, connType _type);
};

#endif