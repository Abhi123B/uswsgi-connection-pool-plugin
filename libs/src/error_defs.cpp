#include "error_defs.h"
#include <string.h>
#include <pthread.h>

void uwsError::setLastError(int errno)
{
    pthread_mutex_lock(&lock);
    lastError = errno;
    pthread_mutex_unlock(&lock);
}

void uwsError::setLastMsg(const char* msg)
{
    if(msg != NULL)
    {
        int len = strlen(msg);
        int size = (len<(CP_MAXX_ERROR_MSG_SIZE-1))?len:CP_MAXX_ERROR_MSG_SIZE;
        pthread_mutex_lock(&lock);
        memcpy(lastMsg, msg, size+1);
        pthread_mutex_unlock(&lock);
    }  
}
    
uwsError::uwsError()
{
    lastError = 0;
    memcpy(lastMsg, "NULL" , 5);
    pthread_mutex_init(&lock, NULL);
}
    
uwsError::~uwsError()
{
    pthread_mutex_destroy(&lock);
}
  
const char* uwsError::getLastErrorMsg(void)
{
    return lastMsg;    
}

int uwsError::getLastErrorCode(void)
{
    return lastError;    
}